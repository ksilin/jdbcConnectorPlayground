package com.example

import org.apache.kafka.clients.admin.{ AdminClient, CreateTopicsResult, NewTopic }
import org.apache.kafka.common.config.TopicConfig
import wvlet.log.LogSupport

import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.javaapi.CollectionConverters.asJava
import scala.util.Try

object TopicUtil extends LogSupport with FutureConverter {

  def createTopic(
      adminClient: AdminClient,
      topicName: String,
      numberOfPartitions: Int = 1,
      replicationFactor: Short = 1
  ): Unit =
    if (!adminClient.listTopics().names().get().contains(topicName)) {
      debug(s"Creating topic ${topicName}")

      val configs: Map[String, String] =
        if (replicationFactor < 3) Map(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> "1")
        else Map.empty

      val newTopic: NewTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor)
      newTopic.configs(configs.asJava)
      try {
        val topicsCreationResult: CreateTopicsResult =
          adminClient.createTopics(Collections.singleton(newTopic))
        topicsCreationResult.all().get()
      } catch {
        case e: Throwable => debug(e)
      }
    } else {
      info(s"topic $topicName already exists, skipping")
    }

  def deleteTopic(adminClient: AdminClient, topicName: String): Any = {
    debug(s"deleting topic $topicName")
    try {
      val topicDeletionResult = adminClient.deleteTopics(List(topicName).asJava)
      topicDeletionResult.all().get()
    } catch {
      case e: Throwable => debug(e)
    }
  }

  // TODO - eliminate waiting by catching the exceptions on retry
  val truncateTopic: (AdminClient, String, Int) => Unit =
    (adminClient: AdminClient, topic: String, partitions: Int) => {

      val javaTopicSet = asJava(Set(topic))
      logger.info(s"deleting topic $topic")
      val deleted: Try[Void] = Try {
        Await.result(adminClient.deleteTopics(javaTopicSet).all().toScalaFuture, 10.seconds)
      }
      waitForTopicToBeDeleted(adminClient, topic)
      Thread.sleep(2000)
      logger.info(s"creating topic $topic")
      val created: Try[Void] = Try {
        val newTopic =
          new NewTopic(
            topic,
            partitions,
            Short.box(1)
          ) // need to box the short here to prevent ctor ambiguity
        val createTopicsResult: CreateTopicsResult = adminClient.createTopics(asJava(Set(newTopic)))
        Await.result(createTopicsResult.all().toScalaFuture, 10.seconds)
      }
      waitForTopicToExist(adminClient, topic)
      Thread.sleep(2000)
    }

  val waitForTopicToExist: (AdminClient, String) => Unit =
    (adminClient: AdminClient, topic: String) => {
      var topicExists = false
      while (!topicExists) {
        Thread.sleep(100)
        topicExists = doesTopicExist(adminClient, topic)
        if (!topicExists) logger.info(s"topic $topic still does not exist")
      }
    }

  val waitForTopicToBeDeleted: (AdminClient, String) => Unit =
    (adminClient: AdminClient, topic: String) => {
      var topicExists = true
      while (topicExists) {
        Thread.sleep(100)
        topicExists = doesTopicExist(adminClient, topic)
        if (topicExists) logger.info(s"topic $topic still exists")
      }
    }

  val doesTopicExist: (AdminClient, String) => Boolean =
    (adminClient: AdminClient, topic: String) => {
      val names = Await.result(adminClient.listTopics().names().toScalaFuture, 10.seconds)
      names.contains(topic)
    }

}
