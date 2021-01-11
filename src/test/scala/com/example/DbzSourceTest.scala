package com.example

import com.example.ConnectConfigs.DbzSourceConfig
import com.typesafe.config.{ Config, ConfigFactory }
import io.getquill.{ MysqlJdbcContext, SnakeCase }
import org.apache.avro.generic.GenericData.Record
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
import scala.util.Random

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

  private val adminClientConfigFull: Config = ConfigFactory.load("admin.conf")
  private val adminConfig: Config           = adminClientConfigFull.getConfig("admin-config")
  private val adminProperties: Properties   = adminConfig.toProperties
  val adminClient: AdminClient              = AdminClient.create(adminProperties)

  val baseConnectorConfig: DbzSourceConfig = DbzSourceConfig(includeQuery = true)

  val targetTopic = "dbserver1.db.simple_test_data"

  val tableName = "simple_test_data"
  val createTableSql: String =
    s"""CREATE TABLE IF NOT EXISTS $tableName (
       | id INT PRIMARY KEY,
       | data VARCHAR(255)
       |)""".stripMargin

  val commonConsumerProps = new Properties()
  commonConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  commonConsumerProps.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer"
  )
  commonConsumerProps.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroDeserializer"
  )
  commonConsumerProps.put(
    "schema.registry.url",
    "http://localhost:8081"
  ) // we use the default value of
  commonConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private def deleteConnector(connectorName: String) = {
    connectRestUtil.deleteConnector(connectorName)
    connectRestUtil.connectorExists(connectorName) mustBe false
  }

  "records inserted into table must be copied to topic" in {

    val connectorName = "dbzConnector"
    deleteConnector(connectorName)
    TopicUtil.deleteTopic(adminClient, targetTopic)
    ctx.executeAction(createTableSql)

    // TODO - how does DBZ connector autocreate its target topics?
    val dbzSourceConfig = baseConnectorConfig.copy(
      tableIncludeList = Some(tableName)
    )
    val connectorConfigString = dbzSourceConfig.jsonString
    info(connectorConfigString)

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      connectorConfigString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true
    // jdbcHelper.doesTableExist2(connectorName) mustBe true
    // TopicUtil.doesTopicExist(adminClient, connectorName) mustBe true

    val data = SimpleTestData(111, "eorgjeor")

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

    val consumerProps = commonConsumerProps.clone().asInstanceOf[Properties]
    consumerProps.put(
      ConsumerConfig.GROUP_ID_CONFIG,
      "AvroSpecGroup10" + Random.alphanumeric.take(10).mkString
    )
    val consumer = new KafkaConsumer[String, GenericRecord](consumerProps)
    consumer.subscribe(List(targetTopic).asJava)
    fetchAndLogRecords(consumer)
  }

  "filtering by gtid" in {

    val connectorName = "correctGtidConnector"
    deleteConnector(connectorName)
    TopicUtil.deleteTopic(adminClient, targetTopic)

    val createTableSql =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         | id INT PRIMARY KEY,
         | data VARCHAR(255)
         |)""".stripMargin

    ctx.executeAction(createTableSql)

    val uuid = jdbcHelper.getGtid
    info(s"UUID: $uuid")
    val dbzSourceConfig = baseConnectorConfig.copy(
      tableIncludeList = Some(tableName),
      gtidSourceIncludes = Some(uuid)
    )
    val connectorConfigString = dbzSourceConfig.jsonString
    info(connectorConfigString)
    connectRestUtil.createOrUpdateConnector(
      connectorName,
      connectorConfigString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true

    val data = SimpleTestData(222, "dgfehwkd")

    {
      import ctx._
      val x: Long = ctx.transaction {
        ctx.run(query[SimpleTestData].insert(lift(data)).onConflictIgnore(_.id))
      }
      info(s"written $x records to DB")
      val res: List[SimpleTestData] = ctx.run(query[SimpleTestData])
      info("found data in DB")
      res foreach println
    }

    val consumerProps = commonConsumerProps.clone().asInstanceOf[Properties]
    consumerProps.put(
      ConsumerConfig.GROUP_ID_CONFIG,
      "AvroSpecGroup20" + Random.alphanumeric.take(10).mkString
    )
    val consumer = new KafkaConsumer[String, GenericRecord](consumerProps)
    consumer.subscribe(List(targetTopic).asJava)
    fetchAndLogRecords(consumer)
  }

  "filtering by gtid - wrong UUID" in {

    val connectorName1 = "wrongGtidConnector"
    deleteConnector(connectorName1)
    TopicUtil.deleteTopic(adminClient, targetTopic)

    val createTableSql =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         | id INT PRIMARY KEY,
         | data VARCHAR(255)
         |)""".stripMargin

    ctx.executeAction(createTableSql)

    val uuid1 = jdbcHelper.getGtid
    info(uuid1)

    // This UUID does not exist -> connector shoudl not produce any data
    val dbzSourceConfig1 = baseConnectorConfig.copy(
      tableIncludeList = Some(tableName),
      gtidSourceIncludes = Some("not_a_UUID")
    )
    val connectorConfigString1 = dbzSourceConfig1.jsonString
    info(connectorConfigString1)
    connectRestUtil.createOrUpdateConnector(
      connectorName1,
      connectorConfigString1
    )
    connectRestUtil.connectorExists(connectorName1) mustBe true

    val data = SimpleTestData(333, "dgfehwkd")

    {
      import ctx._
      val x: Long = ctx.transaction {
        ctx.run(query[SimpleTestData].insert(lift(data)).onConflictIgnore(_.id))
      }
      info(s"written $x records to DB1")
      val res: List[SimpleTestData] = ctx.run(query[SimpleTestData])
      info("found data in DB 1")
      res foreach println
    }

    val consumerProps = commonConsumerProps.clone().asInstanceOf[Properties]
    consumerProps.put(
      ConsumerConfig.GROUP_ID_CONFIG,
      "AvroSpecGroup30" + Random.alphanumeric.take(10).mkString
    )
    val consumer = new KafkaConsumer[String, GenericRecord](consumerProps)
    consumer.subscribe(List(targetTopic).asJava)
    fetchAndLogRecords(consumer)
  }

    )
    consumerProps.put("schema.registry.url", "http://localhost:8081") // we use the default value of
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, GenericRecord](consumerProps)
    consumer.subscribe(List("dbserver1.db.simple_test_data").asJava)

  def fetchAndLogRecords(
      consumer: KafkaConsumer[String, GenericRecord],
      duration: time.Duration = java.time.Duration.ofMillis(100),
      maxAttempts: Int = 100
  ): Unit = {
    var found    = false
    var attempts = 0
    // needs btw 3 and 4 seconds to receive first data
    while (!found && attempts < maxAttempts) {
      val records: ConsumerRecords[String, GenericRecord] = consumer.poll(duration)

      attempts = attempts + 1
      found = !records.isEmpty
      if (found) {
        info(s"fetched ${records.count()} records on attempt $attempts")
        info("printing records")
        records.asScala foreach { r =>
          info(r.value())
          val source: Record = r.value().get("source").asInstanceOf[Record]
          info(s"gtid: ${source.get("gtid")}")
        }
      }
    }
    if (attempts >= maxAttempts) {
      warn("retries exhausted, no data has been written to the topic")
    }
  }

}
