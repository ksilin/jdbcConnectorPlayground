package com.example

import com.example.ConnectConfigs.DbzSourceConfig
import com.typesafe.config.{ Config, ConfigFactory }
import io.getquill.{ Insert, MysqlJdbcContext, SnakeCase }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.{ AdminClient, ListTopicsResult, TopicListing }
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecords, KafkaConsumer }
import org.scalatest.concurrent.Eventually
import wvlet.log.LogSupport

import java.time
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class SimpleTestData(id: Int, data: String)

class DbzSourceTest
    extends AnyFreeSpec
    with Matchers
    with ConfigUtil
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport
    with Eventually {

  lazy val ctx: MysqlJdbcContext[SnakeCase.type] = new MysqlJdbcContext(SnakeCase, "ctx")
  val jdbcHelper: JdbcHelper                     = JdbcHelper(ctx)

  val connectRestUtil: ConnectRestUtil = ConnectRestUtil("localhost", 8083)

  private val producerConfigFull: Config = ConfigFactory.load("producer.conf")
  private val prodProps                  = producerConfigFull.getConfig("producer-config").toProperties

  private val adminClientConfigFull: Config = ConfigFactory.load("admin.conf")
  private val adminConfig: Config           = adminClientConfigFull.getConfig("admin-config")
  private val adminProperties: Properties   = adminConfig.toProperties
  val adminClient: AdminClient              = AdminClient.create(adminProperties)

  val baseConnectorConfig: DbzSourceConfig = DbzSourceConfig()

  private def prepareConnectorTopicAndTable(connectorName: String) = {
    connectRestUtil.deleteConnector(connectorName)
    connectRestUtil.connectorExists(connectorName) mustBe false

    TopicUtil.truncateTopic(adminClient, connectorName, 1)
    TopicUtil.doesTopicExist(adminClient, connectorName) mustBe true
    jdbcHelper.dropTable(connectorName)
    jdbcHelper.doesTableExist2(connectorName) mustBe false
  }

  "creating the connector and a table" in {

    val connectorName = "dbzConnector"
    val tableName     = "simple_test_data"
    prepareConnectorTopicAndTable(connectorName)

    val createTableSql =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         | id INT PRIMARY KEY,
         | data VARCHAR(255)
         |)""".stripMargin

    ctx.executeAction(createTableSql)

    // TODO - how does DBZ connector autocreate its target topics?

    TopicUtil.deleteTopic(adminClient, connectorName)
    TopicUtil.doesTopicExist(adminClient, connectorName) mustBe false

    val dbzSourceConfig       = baseConnectorConfig.copy(tableIncludeList = Some(connectorName))
    val connectorConfigString = dbzSourceConfig.jsonString
    info(connectorConfigString)

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      connectorConfigString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true
    // jdbcHelper.doesTableExist2(connectorName) mustBe true
    // TopicUtil.doesTopicExist(adminClient, connectorName) mustBe true

    val data = SimpleTestData(123, "+1510488988")

    import ctx._
    val x: Long = ctx.transaction {
      ctx.run(query[SimpleTestData].insert(lift(data)).onConflictIgnore(_.id))
    }
    info(s"written $x records")

    val res: List[SimpleTestData] = ctx.run(query[SimpleTestData])
    info("found data in DB")
    res foreach println

    // topics for the schema history and db server must have been created
    // TopicUtil.doesTopicExist(adminClient, dbzSourceConfig.databaseHistoryKafkaTopic) mustBe true

    val listTopics: ListTopicsResult                = adminClient.listTopics()
    val listings: mutable.Map[String, TopicListing] = listTopics.namesToListings().get().asScala
    listings.filterNot(_._1.startsWith("_")) foreach println

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "AvroSpecGroup1")
    consumerProps.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    consumerProps.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    )
    consumerProps.put("schema.registry.url", "http://localhost:8081") // we use the default value of
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, GenericRecord](consumerProps)
    consumer.subscribe(List("dbserver1.db.simple_test_data").asJava)

    val duration: time.Duration = java.time.Duration.ofMillis(100)
    var found                   = false
    var attempts                = 0
    val maxAttempts             = 100
    // needs btw 3 and 4 seconds to receive first data
    while (!found && attempts < maxAttempts) {
      val records: ConsumerRecords[String, GenericRecord] = consumer.poll(duration)

      attempts = attempts + 1
      found = !records.isEmpty
      if (found) {
        info(s"fetched ${records.count()} records on attempt $attempts")
        info("printing records")
        records.asScala foreach println
      }
    }
  }

}
