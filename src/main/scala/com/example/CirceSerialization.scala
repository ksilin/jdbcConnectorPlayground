package com.example

import cats.syntax.either._

import java.nio.charset.StandardCharsets
import io.circe.parser._
import io.circe.syntax._
import io.circe.{ Decoder, Encoder, Error, Json, Printer }
import org.apache.kafka.common.serialization.{
  Deserializer => KafkaDeserializer,
  Serializer => KafkaSerializer
}

object CirceSerialization {

  val prntr: Printer = Printer.noSpaces.copy(dropNullValues = true)

  def circeJsonSerializer[T: Encoder]: KafkaSerializer[T] = (_, data) =>
    prntr.print(data.asJson).getBytes(StandardCharsets.UTF_8)

  def circeJsonDeserializer[T: Decoder]: KafkaDeserializer[T] = (_, data) =>
    (for {
      json <- parse(new String(data, StandardCharsets.UTF_8)): Either[Error, Json]
      t    <- json.as[T]: Either[Error, T]
    } yield t).fold(
      error => throw new RuntimeException(s"Deserialization failure: ${error.getMessage}", error),
      identity _
    )

}
