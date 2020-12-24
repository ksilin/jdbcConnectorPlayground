package com.example

import io.circe.generic.extras.{Configuration, ConfiguredJsonCodec}

import java.util.regex.Pattern

object ConnectConfigs {

  val basePattern: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")
  val swapPattern: Pattern = Pattern.compile("([a-z\\d])([A-Z])")

  val dotCaseTransformation: String => String = s => {
    val partial = basePattern.matcher(s).replaceAll("$1.$2")
    swapPattern.matcher(partial).replaceAll("$1.$2").toLowerCase
  }

  implicit val dotCase: Configuration = Configuration.default.copy(transformMemberNames = dotCaseTransformation)

  // TODO - there are two variants of this config - one intended to be POSTed to /connectors <- must contain a "name" field
  // and the other, intended to be PUT to /connectors/my-connector/config  does not have a name
  @ConfiguredJsonCodec
  case class JdbcSinkConfig(
                             connectionUrl: String,
                             topics: String, // TODO - does not work with List[String] - fails to deserialize at the REST endpoint
                             valueConverter: String,
                             connectorClass: String = "io.confluent.connect.jdbc.JdbcSinkConnector",
                             tasksMax: Int = 1,
                             autoCreate: Boolean = true,
                             autoEvolve: Boolean = false,
                             valueConverterSchemasEnable: Option[Boolean] = None,
                             errorsLogEnable: Boolean = true,
                             errorsLogIncludeMessages: Boolean = true
                           )

  case class ConnectorState(state: String, worker_id: String)
  case class TaskState(id: Int, state: String, worker_id: String, trace: Option[String] = None)
  case class ConnectorStatus(name: String, `type`: String, connector: ConnectorState, tasks: List[TaskState])

}
