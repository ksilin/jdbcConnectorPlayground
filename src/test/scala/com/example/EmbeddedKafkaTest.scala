package com.example

import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.admin.{
  AdminClient,
  AdminClientConfig,
  ListTopicsResult,
  TopicListing
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class EmbeddedKafkaTest extends AnyFreeSpec with Matchers with ConfigUtil with BeforeAndAfterAll {

  private val privateCluster: EmbeddedSingleNodeKafkaCluster =
    EmbeddedSingleNodeKafkaCluster.build()
  var brokerPort = ""

  private val inputTopic  = "inputTopic"
  private val outputTopic = "outputTopic"

  override def beforeAll(): Unit = {
    privateCluster.start()

    brokerPort = privateCluster.bootstrapServers()
    println(brokerPort)
    println("creating topics")
    createTopics()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    privateCluster.stop()
    super.afterAll()
  }

  def createTopics() {
    privateCluster.createTopic(inputTopic, 2, 1)
    privateCluster.waitForTopicsToBePresent(inputTopic)
  }

  "test topics are being created" in {
    val localhostConfig = "localhost" + brokerPort
    println(localhostConfig)
    val adminProperties = new Properties()
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, localhostConfig)
    val adminClient: AdminClient                    = AdminClient.create(adminProperties)
    val listTopics: ListTopicsResult                = adminClient.listTopics()
    val listings: mutable.Map[String, TopicListing] = listTopics.namesToListings().get().asScala
    listings foreach println
  }

}
