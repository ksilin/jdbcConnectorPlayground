package com.example

import io.circe.Printer

object ConnectJsonSchemaUtil {

  val dropNullValuesJsonPrinter: Printer = Printer.noSpaces.copy(dropNullValues = true)
  val printer = dropNullValuesJsonPrinter

  case class SimpleJsonNodeDef(`type`: String, optional: Boolean, field: String, default: Option[String] = None)
  case class ConnectJsonSchema(version: Option[Int] = None, `type`: String = "struct", fields: List[SimpleJsonNodeDef])

  case class JsonEnvelope[T](schema: ConnectJsonSchema, payload: T)
}
