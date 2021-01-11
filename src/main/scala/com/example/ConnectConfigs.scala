package com.example

import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.generic.extras.{ Configuration, ConfiguredJsonCodec }

import java.util.regex.Pattern

object ConnectConfigs {

  val basePattern: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")
  val swapPattern: Pattern = Pattern.compile("([a-z\\d])([A-Z])")

  val dotCaseTransformation: String => String = s => {
    val partial = basePattern.matcher(s).replaceAll("$1.$2")
    swapPattern.matcher(partial).replaceAll("$1.$2").toLowerCase
  }

  implicit val dotCase: Configuration =
    Configuration.default.copy(transformMemberNames = dotCaseTransformation)

  // TODO - there are two variants of this config - one intended to be POSTed to /connectors <- must contain a "name" field
  // and the other, intended to be PUT to /connectors/my-connector/config  does not have a name
  @ConfiguredJsonCodec
  case class JdbcSinkConfig(
      connectionUrl: String,
      topics: String, // TODO - does not work with List[String] - fails to deserialize at the REST endpoint
      valueConverter: String,
      keyConverter: String = "org.apache.kafka.connect.storage.StringConverter",
      connectorClass: String = "io.confluent.connect.jdbc.JdbcSinkConnector",
      tasksMax: Int = 1,
      batchSize: Int = 1, // for writing
      autoCreate: Boolean = true,
      autoEvolve: Boolean = false,
      valueConverterSchemasEnable: Option[Boolean] = None,
      errorsLogEnable: Boolean = true,
      errorsLogIncludeMessages: Boolean = true,
      pkMode: String = "none",         // 'none' <- default, 'kafka', 'record_key', 'record_value'
      pkFields: Option[String] = None, // depends on setting for pk.mode
      fieldsWhitelist: Option[String] = None,
      deleteEnabled: Boolean = false,           // true required pk.mode to be 'record_key'
      insertMode: String = "insert",            // valid: 'insert', 'upsert', 'update'
      consumerOverrideFetchMaxWaitMs: Int = 10, // 500 by default
      consumerOverrideMaxPollRecords: Int = 1,
      consumerOverrideEnableAutoCommit: Boolean = false // we dont want to commit any offsets
  ) {
    val json: Json         = this.asJson.dropNullValues
    val jsonString: String = json.noSpaces
  }

  case class ConnectorState(state: String, worker_id: String)
  case class TaskState(id: Int, state: String, worker_id: String, trace: Option[String] = None)
  case class ConnectorStatus(
      name: String,
      `type`: String,
      connector: ConnectorState,
      tasks: List[TaskState]
  )

  // https://debezium.io/documentation/reference/1.4/connectors/mysql.html#mysql-connector-properties

  // https://github.com/vdesabou/kafka-docker-playground-connect contains DBZ 1.3.1
  @ConfiguredJsonCodec
  case class DbzSourceConfig(
      connectorClass: String = "io.debezium.connector.mysql.MySqlConnector",
      tasksMax: Int = 1,
      databaseHostname: String = "mysql",
      databasePort: String = "3306",
      databaseUser: String = "debezium",
      databasePassword: String = "dbz",
      databaseServerId: String = "223344",
      databaseServerName: String = "dbserver1",
      databaseHistoryKafkaBootstrapServers: String = "broker:9092",
      databaseHistoryKafkaTopic: String = "schema-changes.mydb",
      databaseIncludeList: Option[String] = None, // "mydb",
      tableIncludeList: Option[String] = None,    // "mydb",
      includeSchemaChanges: Boolean = true,
      includeQuery: Boolean = false,
      messageKeyColumns: Option[String] = None
      // gtid.source.includes <- A comma-separated list of regular expressions that match source UUIDs in the GTID set used to find the binlog position in the MySQL server
  ){
    val json: Json         = this.asJson.dropNullValues
    val jsonString: String = json.noSpaces
  }

}
