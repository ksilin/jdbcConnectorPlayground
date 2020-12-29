package com.example

import com.example.ConnectConfigs._
import io.circe.syntax._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import wvlet.log.LogSupport

class ConnectRestUtilTest
    extends AnyFreeSpec
    with Matchers
    with ConfigUtil
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport {

  val connectRestUtil: ConnectRestUtil = ConnectRestUtil("localhost", 8083)

  val sinkConnectorConfig = JdbcSinkConfig(
    connectionUrl = "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    topics = "orders",
    valueConverter = "org.apache.kafka.connect.json.JsonConverter",
    valueConverterSchemasEnable = Some(true)
  )

  "must retrieve list of connectors" in {
    val connectors: List[String] = connectRestUtil.listConnectors()
    info("found connectors: ")
    info(connectors.mkString)
  }

  "typed connector config visual test" in {

    val sinkConnectorConfig = JdbcSinkConfig(
      connectionUrl = "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
      topics = "orders",
      valueConverter = "org.apache.kafka.connect.json.JsonConverter",
      valueConverterSchemasEnable = Some(true)
    )
    val jsonString = sinkConnectorConfig.asJson.spaces2
    println(jsonString)
  }

  "must create connector" in {

    val connectorName = "mysql-sink2"

    info(s"connector $connectorName exists: ${connectRestUtil.connectorExists(connectorName)}")

    val connectorString = sinkConnectorConfig.asJson.noSpaces
    info("connector string:")
    info(connectorString)

    val createdOrUpdated: Option[String] =
      connectRestUtil.createOrUpdateConnector(connectorName, connectorString)
    info(s"created or updated connector $connectorName")
    info(createdOrUpdated)

    connectRestUtil.connectorExists(connectorName) mustBe true

    val status = connectRestUtil.connectorStatus(connectorName)
    info("connector status:")
    info(status)
  }

  "must delete connector" in {
    val connectorName   = "mysql-sink-todelete"
    val connectorString = sinkConnectorConfig.asJson.noSpaces
    val createdOrUpdated: Option[String] =
      connectRestUtil.createOrUpdateConnector(connectorName, connectorString)
    connectRestUtil.connectorExists(connectorName) mustBe true

    val res = connectRestUtil.deleteConnector(connectorName)
    info("connector deleted: ")
    info(res)
    connectRestUtil.connectorExists(connectorName) mustBe false
  }

}
