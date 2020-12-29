package com.example

import com.example.ConnectConfigs.JdbcSinkConfig
import com.example.ConnectJsonSchemaUtil.{ ConnectJsonSchema, JsonEnvelope, SimpleJsonNodeDef }
import com.example.TestData.{ Product, ProductOptional }
import com.typesafe.config.{ Config, ConfigFactory }
import io.getquill.{ MysqlJdbcContext, SnakeCase }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import io.circe.generic.auto._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization.{ Serializer, StringSerializer }
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Seconds, Span }
import wvlet.log.LogSupport

import java.util.Properties

case class SimpleTestData(id: Int, data: String)

class AutoCreateTest
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

  val baseConnectorConfig: JdbcSinkConfig = JdbcSinkConfig(
    connectionUrl = "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    topics = "DEFAULT_CONNECTOR",
    valueConverter = "org.apache.kafka.connect.json.JsonConverter",
    valueConverterSchemasEnable = Some(true),
    autoCreate = true, // is already the default,
    pkMode = "none"    // this is the default
  )

  private def prepareConnectorTopicAndTable(connectorName: String) = {
    connectRestUtil.deleteConnector(connectorName)
    connectRestUtil.connectorExists(connectorName) mustBe false

    TopicUtil.truncateTopic(adminClient, connectorName, 1)
    TopicUtil.doesTopicExist(adminClient, connectorName) mustBe true
    jdbcHelper.dropTable(connectorName)
    jdbcHelper.doesTableExist2(connectorName) mustBe false
  }

  "creating just the connector without a topic does not create a table" in {

    val connectorName = "noTopicConnector"
    prepareConnectorTopicAndTable(connectorName)
    TopicUtil.deleteTopic(adminClient, connectorName)
    TopicUtil.doesTopicExist(adminClient, connectorName) mustBe false

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      baseConnectorConfig.copy(topics = connectorName).jsonString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true
    jdbcHelper.doesTableExist2(connectorName) mustBe false
  }

  "creating just the connector with an empty topic does not create a table" in {

    val connectorName = "emptyTopicConnector"
    prepareConnectorTopicAndTable(connectorName)
    connectRestUtil.createOrUpdateConnector(
      connectorName,
      baseConnectorConfig.copy(topics = connectorName).jsonString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true
    jdbcHelper.doesTableExist2(connectorName) mustBe false
  }

  "producing a json message without a schema and key def will not create a table and task will fail" in {

    val connectorName = "schemalessTopicConnectorNoPK"
    prepareConnectorTopicAndTable(connectorName)

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      baseConnectorConfig.copy(topics = connectorName).jsonString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true

    val product                         = Product(id = connectorName, description = connectorName)
    val serializer: Serializer[Product] = CirceSerialization.circeJsonSerializer[Product]
    val producer: KafkaProducer[String, Product] =
      new KafkaProducer(prodProps, new StringSerializer(), serializer)
    val producedMeta: RecordMetadata =
      producer.send(new ProducerRecord[String, Product](connectorName, product.id, product)).get()

    // give the connector some time to pick up the record
    Thread.sleep(1000)
    jdbcHelper.doesTableExist2(connectorName) mustBe false

    val failedConnectorTask = eventually(timeout(Span(10, Seconds)), interval(Span(250, Millis))) {
      val status: ConnectConfigs.ConnectorStatus =
        connectRestUtil.connectorStatus(connectorName).get
      status.tasks.find(t => t.state == "FAILED").get
    }
    val expectedErrorString =
      """configured with 'delete.enabled=false' and 'pk.mode=none' and therefore requires records with a non-null Struct value and non-null Struct schema"""
    failedConnectorTask.trace.get.contains(expectedErrorString) mustBe true
  }

  // org.apache.kafka.connect.errors.ConnectException:
  // Sink connector 'schemalessTopicConnector' is configured with 'delete.enabled=false' and
  // 'pk.mode=none' and therefore requires records with a non-null Struct value
  // and non-null Struct schema, but found record at
  // (topic='schemalessTopicConnector',partition=0,offset=0,timestamp=1609077869968)
  // with a HashMap value and null value schema.

  "producing a json message without a schema to an empty topic creates a table but then fails on the message" in {

    val connectorName = "schemalessTopicConnectorMsgPK"
    prepareConnectorTopicAndTable(connectorName)

    val sinkConnectorConfig: JdbcSinkConfig = baseConnectorConfig.copy(
      topics = connectorName,
      valueConverterSchemasEnable = Some(false),
      pkMode = "record_key",
      pkFields = Some("REC_PK")
    )
    connectRestUtil.createOrUpdateConnector(
      connectorName,
      sinkConnectorConfig.jsonString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true

    val product                         = Product(id = connectorName, description = connectorName)
    val serializer: Serializer[Product] = CirceSerialization.circeJsonSerializer[Product]
    val producer: KafkaProducer[String, Product] =
      new KafkaProducer(prodProps, new StringSerializer(), serializer)
    producer.send(new ProducerRecord[String, Product](connectorName, product.id, product)).get()

    eventually(timeout(Span(10, Seconds)), interval(Span(250, Millis))) {
      if (!jdbcHelper.doesTableExist2(connectorName))
        throw new IllegalStateException("table not yet there")
    }
    val tableDesc: Seq[DescribeResult] = jdbcHelper.describeTable(connectorName)
    tableDesc.size mustBe 1
    tableDesc.head mustBe DescribeResult("REC_PK", "varchar(256)", "NO", Some("PRI"), null, None)

    val failedConnectorTask = eventually(timeout(Span(10, Seconds)), interval(Span(250, Millis))) {
      val status: ConnectConfigs.ConnectorStatus =
        connectRestUtil.connectorStatus(connectorName).get
      status.tasks.find(t => t.state == "FAILED").get
    }
    val expectedExceptionString =
      "java.lang.ClassCastException: class java.util.HashMap cannot be cast to class org.apache.kafka.connect.data.Struct"
    failedConnectorTask.trace.get.contains(expectedExceptionString) mustBe true

    // java.lang.ClassCastException: class java.util.HashMap cannot be cast to class org.apache.kafka.connect.data.Struct (java.util.HashMap is in module java.base of loader 'bootstrap'; org.apache.kafka.connect.data.Struct is in unnamed module of loader 'app')
    //	at io.confluent.connect.jdbc.sink.PreparedStatementBinder.bindRecord(PreparedStatementBinder.java:61)
    //	at io.confluent.connect.jdbc.sink.BufferedRecords.flush(BufferedRecords.java:182)
    //	at io.confluent.connect.jdbc.sink.JdbcDbWriter.write(JdbcDbWriter.java:72)
    //	at io.confluent.connect.jdbc.sink.JdbcSinkTask.put(JdbcSinkTask.java:74)
    //	at org.apache.kafka.connect.runtime.WorkerSinkTask.deliverMessages(WorkerSinkTask.java:560)
    //	at org.apache.kafka.connect.runtime.WorkerSinkTask.poll(WorkerSinkTask.java:323)
    //	at org.apache.kafka.connect.runtime.WorkerSinkTask.iteration(WorkerSinkTask.java:226)
    //	at org.apache.kafka.connect.runtime.WorkerSinkTask.execute(WorkerSinkTask.java:198)
    //	at org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:185)
    //	at org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:235)
    // ...
  }

  "producing a json message with a schema to an empty topic creates a table" in {

    val connectorName = "schemaTopicConnector"
    prepareConnectorTopicAndTable(connectorName)
    connectRestUtil.createOrUpdateConnector(
      connectorName,
      baseConnectorConfig.copy(topics = connectorName).jsonString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true

    val product = Product(id = connectorName, description = connectorName)
    val productIdFieldSchema: SimpleJsonNodeDef =
      SimpleJsonNodeDef(`type` = "string", field = "id", optional = false)
    val productDescriptionFieldSchema: SimpleJsonNodeDef =
      SimpleJsonNodeDef(`type` = "string", field = "description", optional = false)

    val productSchema: ConnectJsonSchema =
      ConnectJsonSchema(fields = List(productIdFieldSchema, productDescriptionFieldSchema))
    val jsonEnvelopeSerializer: Serializer[JsonEnvelope[Product]] =
      CirceSerialization.circeJsonSerializer[JsonEnvelope[Product]]
    val producer: KafkaProducer[String, JsonEnvelope[Product]] =
      new KafkaProducer(prodProps, new StringSerializer(), jsonEnvelopeSerializer)
    val record = new ProducerRecord[String, JsonEnvelope[Product]](
      connectorName,
      product.id,
      JsonEnvelope[Product](productSchema, product)
    )
    producer.send(record).get()

    eventually(timeout(Span(10, Seconds)), interval(Span(250, Millis))) {
      if (!jdbcHelper.doesTableExist2(connectorName))
        throw new IllegalStateException("table not yet there")
    }
    val describeResult: Seq[DescribeResult] = jdbcHelper.describeTable(connectorName)

    val expectedColumns = List(
      DescribeResult("id", "varchar(256)", "NO", None, null, None),
      DescribeResult("description", "varchar(256)", "NO", None, null, None)
    )
    describeResult must contain allElementsOf expectedColumns

    val status: ConnectConfigs.ConnectorStatus =
      eventually(timeout(Span(10, Seconds)), interval(Span(250, Millis))) {
        val status = connectRestUtil.connectorStatus(connectorName).get
        if (status.tasks.isEmpty) throw new IllegalStateException("no tasks created yet")
        else status
      }
    status.tasks.map(_.state).distinct mustBe List("RUNNING")
    status.tasks.map(_.trace).distinct mustBe List(None)
  }

  "must create optional columns with defaults according to Json schema of first msg" in {

    val connectorName = "optionalColumnsConnector"
    prepareConnectorTopicAndTable(connectorName)

    connectRestUtil.createOrUpdateConnector(
      connectorName,
      baseConnectorConfig.copy(topics = connectorName).jsonString
    )
    connectRestUtil.connectorExists(connectorName) mustBe true

    val product: ProductOptional = ProductOptional(id = None, description = None)
    val productIdFieldSchema: SimpleJsonNodeDef =
      SimpleJsonNodeDef(
        `type` = "string",
        field = "id",
        optional = true,
        default = Some("DEFAULT_ID")
      )
    val productDescriptionFieldSchema: SimpleJsonNodeDef =
      SimpleJsonNodeDef(
        `type` = "string",
        field = "description",
        optional = true,
        default = Some("DEFAULT_DESC")
      )

    val productSchema: ConnectJsonSchema =
      ConnectJsonSchema(fields = List(productIdFieldSchema, productDescriptionFieldSchema))
    val jsonEnvelopeSerializer: Serializer[JsonEnvelope[ProductOptional]] =
      CirceSerialization.circeJsonSerializer[JsonEnvelope[ProductOptional]]
    val producer: KafkaProducer[String, JsonEnvelope[ProductOptional]] =
      new KafkaProducer(prodProps, new StringSerializer(), jsonEnvelopeSerializer)
    val record = new ProducerRecord[String, JsonEnvelope[ProductOptional]](
      connectorName,
      product.id.getOrElse(productIdFieldSchema.default.get),
      JsonEnvelope[ProductOptional](productSchema, product)
    )
    producer.send(record).get()

    eventually(timeout(Span(10, Seconds)), interval(Span(250, Millis))) {
      if (!jdbcHelper.doesTableExist2(connectorName))
        throw new IllegalStateException("table not yet there")
    }

    // check table def
    val describeResult: List[DescribeResult] = jdbcHelper.describeTable(connectorName)
    val expectedColumns = List(
      DescribeResult("id", "varchar(256)", "YES", None, productIdFieldSchema.default.get, None),
      DescribeResult(
        "description",
        "varchar(256)",
        "YES",
        None,
        productDescriptionFieldSchema.default.get,
        None
      )
    )
    describeResult must contain allElementsOf expectedColumns

    // check table contents
    import jdbcHelper.ctx._
    val q                          = quote(querySchema[ProductOptional]("optionalColumnsConnector"))
    val res: List[ProductOptional] = jdbcHelper.ctx.run(q)
    res.size mustBe 1
    res.head mustBe ProductOptional(
      productIdFieldSchema.default,
      productDescriptionFieldSchema.default
    )
  }

}
