/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import kafka.admin.AdminUtils
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.{StringDecoder, StringEncoder}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import org.apache.spark.{Logging, SparkConf, SparkFunSuite}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.Utils

/**
 * This is an abstract base class for Kafka testsuites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
 */
abstract class KafkaStreamSuiteBase extends SparkFunSuite with Eventually with Logging {

  private val zkHost = "localhost"
  private var zkPort: Int = 0
  private val zkConnectionTimeout = 6000
  private val zkSessionTimeout = 6000
  private var zookeeper: EmbeddedZookeeper = _
  private val brokerHost = "localhost"
  private var brokerPort = 9092
  private var brokerConf: KafkaConfig = _
  private var server: KafkaServer = _
  private var producer: Producer[String, String] = _
  private var zkReady = false
  private var brokerReady = false

  protected var zkClient: ZkClient = _

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$brokerHost:$brokerPort"
  }

  def setupKafka() {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    zkReady = true
    logInfo("==================== Zookeeper Started ====================")

    zkClient = new ZkClient(zkAddress, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    logInfo("==================== Zookeeper Client Created ====================")

    // Kafka broker startup
    var bindSuccess: Boolean = false
    while(!bindSuccess) {
      try {
        val brokerProps = getBrokerConfig()
        brokerConf = new KafkaConfig(brokerProps)
        server = new KafkaServer(brokerConf)
        server.startup()
        logInfo("==================== Kafka Broker Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
            brokerPort += 1
          }
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }

    Thread.sleep(2000)
    logInfo("==================== Kafka + Zookeeper Ready ====================")
    brokerReady = true
  }

  def tearDownKafka() {
    brokerReady = false
    zkReady = false
    if (producer != null) {
      producer.close()
      producer = null
    }

    if (server != null) {
      server.shutdown()
      server = null
    }

    brokerConf.logDirs.foreach { f => Utils.deleteRecursively(new File(f)) }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }
  }

  def createTopic(topic: String) {
    AdminUtils.createTopic(zkClient, topic, 1, 1)
    // wait until metadata is propagated
    waitUntilMetadataIsPropagated(topic, 0)
    logInfo(s"==================== Topic $topic Created ====================")
  }

  def sendMessages(topic: String, messageToFreq: Map[String, Int]) {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }
  
  def sendMessages(topic: String, messages: Array[String]) {
    producer = new Producer[String, String](new ProducerConfig(getProducerConfig()))
    producer.send(messages.map { new KeyedMessage[String, String](topic, _ ) }: _*)
    producer.close()
    logInfo(s"==================== Sent Messages: ${messages.mkString(", ")} ====================")
  }

  private def getBrokerConfig(): Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", brokerPort.toString)
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  private def getProducerConfig(): Properties = {
    val brokerAddr = brokerConf.hostName + ":" + brokerConf.port
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }

  private def waitUntilMetadataIsPropagated(topic: String, partition: Int) {
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(
        server.apis.metadataCache.containsTopicAndPartition(topic, partition),
        s"Partition [$topic, $partition] metadata not propagated after timeout"
      )
    }
  }

  class EmbeddedZookeeper(val zkConnect: String) {
    val random = new Random()
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      Utils.deleteRecursively(snapshotDir)
      Utils.deleteRecursively(logDir)
    }
  }
}


class KafkaStreamSuite extends KafkaStreamSuiteBase with BeforeAndAfter {
  var ssc: StreamingContext = _

  before {
    setupKafka()
  }

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    tearDownKafka()
  }

  test("Kafka input stream") {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    ssc = new StreamingContext(sparkConf, Milliseconds(500))
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    createTopic(topic)
    sendMessages(topic, sent)

    val kafkaParams = Map("zookeeper.connect" -> zkAddress,
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}",
      "auto.offset.reset" -> "smallest")

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]()
    stream.map(_._2).countByValue().foreachRDD { r =>
      val ret = r.collect()
      ret.toMap.foreach { kv =>
        val count = result.getOrElseUpdate(kv._1, 0) + kv._2
        result.put(kv._1, count)
      }
    }
    ssc.start()
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(sent.size === result.size)
      sent.keys.foreach { k =>
        assert(sent(k) === result(k).toInt)
      }
    }
    ssc.stop()
  }
}

