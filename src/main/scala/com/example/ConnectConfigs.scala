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

}
