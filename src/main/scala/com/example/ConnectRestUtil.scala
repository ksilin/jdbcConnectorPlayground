package com.example

import com.example.ConnectConfigs.ConnectorStatus
import io.circe
import sttp.capabilities
import sttp.client3._
import sttp.client3.okhttp.OkHttpSyncBackend
import sttp.client3.circe._
import circe.generic.auto._
import sttp.model.{Headers, MediaType, Uri}
import wvlet.log.LogSupport

case class ConnectRestUtil(host: String, port: Int) extends LogSupport {

  val backend: SttpBackend[Identity, capabilities.WebSockets] = OkHttpSyncBackend()

  val baseUrl = uri"http://$host:$port"

  val pluginsUrl: Uri    = baseUrl.addPath("plugins")
  val connectorsUrl: Uri = baseUrl.addPath("connectors")

  val config = "config"
  val status = "status"
  val tasks  = "tasks"

  def logFailedRequest(responseOrError: Identity[Response[_]]): Unit = {
    warn("request failed:")
    warn(responseOrError.code)
    warn(responseOrError.statusText)
    warn(responseOrError.headers)
    try warn(responseOrError.body)
  }

  def listConnectors()
      : List[String] = { //Identity[Response[Either[ResponseException[String, circe.Error], List[String]]]] = {
    debug(s"retrieving list of connectors from $connectorsUrl")

    val reqT = basicRequest.get(connectorsUrl).response(asJson[List[String]])

    val responseOrError
        : Identity[Response[Either[ResponseException[String, circe.Error], List[String]]]] =
      reqT.send(backend)

    if (!responseOrError.isSuccess) {
      logFailedRequest(responseOrError)
      Nil
    } else {
      responseOrError.body.fold(
        _ => Nil,
        { l: List[String] =>
          debug("retrieved connectors: ")
          debug(l)
          l
        }
      )
    }
  }

  def createOrUpdateConnector(connectorName: String, connectorJson: String): Option[String] = {
    debug("creating or updating connector: ")
    val reqT = basicRequest
      .put(connectorsUrl.addPath(connectorName, config))
      .body(connectorJson)
      .contentType(MediaType.ApplicationJson)
      .response(asString)
    val res = reqT.send(backend)

    if (!res.isSuccess) {
      logFailedRequest(res)
      None
    } else {
      res.body.fold(_ => None, b => Some(b))
    }
  }

  def deleteConnector(connectorName: String): Option[String] = {
    val reqT                                            = basicRequest.delete(connectorsUrl.addPath(connectorName)).response(asString)
    val res: Identity[Response[Either[String, String]]] = reqT.send(backend)

    if (!res.isSuccess) {
      logFailedRequest(res)
      None
    } else {
      res.body.fold(_ => None, b => Some(b))
    }
  }

  def connectorExists(connectorName: String): Boolean =
    listConnectors().contains(connectorName)

  def connectorStatus(connectorName: String): Option[ConnectorStatus] = {
    val reqT                                            = basicRequest.get(connectorsUrl.addPath(connectorName, status)).response(asJson[ConnectorStatus])
    val res: Identity[Response[Either[ResponseException[String, circe.Error], ConnectorStatus]]] = reqT.send(backend)
    if (!res.isSuccess) {
      logFailedRequest(res)
      None
    } else {
      res.body.fold(_ => None, b => Some(b))
    }
  }

}
