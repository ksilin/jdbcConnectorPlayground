package com.example

import com.example.ConnectConfigs.JdbcSourceConfig
import com.example.TestData.Product
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.syntax._
import io.getquill.{MysqlJdbcContext, SnakeCase}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

import java.util.Properties
import scala.jdk.CollectionConverters._

class JdbcSourceTableDetectionTest
    extends AnyFreeSpec
    with Matchers
    with ConfigUtil
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport
    with Eventually {

  lazy val ctx: MysqlJdbcContext[SnakeCase.type] = new MysqlJdbcContext(SnakeCase, "ctx")
  import ctx._
  val jdbcHelper: JdbcHelper = JdbcHelper(ctx)

  val connectRestUtil: ConnectRestUtil = ConnectRestUtil("localhost", 8083)

  private val consumerConfigFull: Config = ConfigFactory.load("consumer.conf")
  private val consumerProps              = consumerConfigFull.getConfig("consumer-config").toProperties
  consumerProps.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    classOf[StringDeserializer].getName
  )
  consumerProps.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    classOf[StringDeserializer].getName
  )
  consumerProps.put(
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
    "earliest"
  )

  private val adminClientConfigFull: Config = ConfigFactory.load("admin.conf")
  private val adminConfig: Config           = adminClientConfigFull.getConfig("admin-config")
  private val adminProperties: Properties   = adminConfig.toProperties
  val adminClient: AdminClient              = AdminClient.create(adminProperties)

  val baseConnectorConfig: JdbcSourceConfig = JdbcSourceConfig(
    connectionUrl = "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    valueConverter = "org.apache.kafka.connect.json.JsonConverter"
  )

  "create connector, add data to table, see results" in {

    val connectorName = "noTableConnector"

    val sourceConnectorConfig: JdbcSourceConfig =
      baseConnectorConfig.copy(tableWhitelist = connectorName)
    dropConnectorDeleteTopicRecreateTable(
      connectorName,
      adminClient,
      Some(sourceConnectorConfig.topicPrefix)
    )

    val product1: Product = Product("id1", "testProduct")
    implicit val qSchema2 = schemaMeta[Product]("noTableConnector")
    ctx.run {
      quote {
        query[Product].insert(lift(product1)).onConflictIgnore(_.id)
      }
    }

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      sourceConnectorConfig.asJson.dropNullValues.noSpaces
    )
    connectRestUtil.connectorExists(connectorName) mustBe true
    jdbcHelper.doesTableExist2(connectorName) mustBe true
    val status: Option[ConnectConfigs.ConnectorStatus] =
      connectRestUtil.connectorStatus(connectorName)

    status.isEmpty mustBe false
    status.get.connector.state mustBe "RUNNING"

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(List(s"${sourceConnectorConfig.topicPrefix}${connectorName}").asJava)
    val records = TopicUtil.fetchAndPrintRecords(consumer)
    records.isEmpty mustBe false
  }

  "table name contains a dot" in {

    val connectorName = "dotTableConnector"
    val tableNameUnescaped     = "dot.tbl"
    val tableName     = s"`${tableNameUnescaped}`"

    val tableDdl =
      s"""$tableName (
         | id varchar(255) NOT NULL,
         | description varchar(255),
         | PRIMARY KEY(id)
         |)""".stripMargin
    jdbcHelper.createTable(tableDdl)

    val sourceConnectorConfig: JdbcSourceConfig =
      baseConnectorConfig.copy(tableWhitelist = tableName)
    dropConnectorDeleteTopicRecreateTable(
      connectorName,
      adminClient,
      Some(sourceConnectorConfig.topicPrefix),
      Some(tableName)
    )

    val product2: Product = Product("id2", "testProduct")
    implicit val qSchema2 = schemaMeta[Product]("`dot.tbl`")
    ctx.run {
      quote {
        query[Product].insert(lift(product2)).onConflictIgnore(_.id)
      }
    }

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      sourceConnectorConfig.asJson.dropNullValues.noSpaces
    )
    connectRestUtil.connectorExists(connectorName) mustBe true
    val status: Option[ConnectConfigs.ConnectorStatus] =
      connectRestUtil.connectorStatus(connectorName)
    status.isEmpty mustBe false
    status.get.connector.state mustBe "RUNNING"

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(List(s"${sourceConnectorConfig.topicPrefix}${tableNameUnescaped}").asJava)
    val records: Iterable[ConsumerRecord[String, String]] = TopicUtil.fetchAndPrintRecords(consumer)
    records.isEmpty mustBe false
  }

  private def dropConnectorDeleteTopicRecreateTable(
      connectorName: String,
      adminClient: AdminClient,
      topicPrefix: Option[String] = None,
      tableName: Option[String] = None
  ) = {
    connectRestUtil.deleteConnector(connectorName)
    connectRestUtil.connectorExists(connectorName) mustBe false

    val table = tableName.getOrElse(connectorName)
    val topic = topicPrefix.map(p => s"$p$table").getOrElse(table)

    TopicUtil.deleteTopic(adminClient, topic)
    TopicUtil.doesTopicExist(adminClient, topic) mustBe false

    jdbcHelper.dropTable(table)
    jdbcHelper.createTable(TestData.productTableDDl(table))
    // jdbcHelper.doesTableExist2(table) mustBe true
  }

}
