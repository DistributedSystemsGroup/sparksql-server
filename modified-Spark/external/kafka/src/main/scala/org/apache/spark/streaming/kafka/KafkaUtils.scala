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

import java.lang.{Integer => JInt}
import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import java.util.{Set => JSet}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, Decoder, StringDecoder}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaPairInputDStream, JavaInputDStream, JavaPairReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}

object KafkaUtils {
  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc       StreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def createStream(
      ssc: StreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc         StreamingContext object
   * @param kafkaParams Map of kafka configuration parameters,
   *                    see http://kafka.apache.org/08/configuration.html
   * @param topics      Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                    in its own thread.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[(K, V)] = {
    val walEnabled = ssc.conf.getBoolean("spark.streaming.receiver.writeAheadLog.enable", false)
    new KafkaInputDStream[K, V, U, T](ssc, kafkaParams, topics, walEnabled, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   */
  def createStream(
      jssc: JavaStreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt]
    ): JavaPairReceiverInputDStream[String, String] = {
    createStream(jssc.ssc, zkQuorum, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*))
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..).
   * @param groupId   The group id for this consumer.
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread.
   * @param storageLevel RDD storage level.
   */
  def createStream(
      jssc: JavaStreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[String, String] = {
    createStream(jssc.ssc, zkQuorum, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*),
      storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param keyTypeClass Key type of DStream
   * @param valueTypeClass value type of Dstream
   * @param keyDecoderClass Type of kafka key decoder
   * @param valueDecoderClass Type of kafka value decoder
   * @param kafkaParams Map of kafka configuration parameters,
   *                    see http://kafka.apache.org/08/configuration.html
   * @param topics  Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                in its own thread
   * @param storageLevel RDD storage level.
   */
  def createStream[K, V, U <: Decoder[_], T <: Decoder[_]](
      jssc: JavaStreamingContext,
      keyTypeClass: Class[K],
      valueTypeClass: Class[V],
      keyDecoderClass: Class[U],
      valueDecoderClass: Class[T],
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyTypeClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueTypeClass)

    implicit val keyCmd: ClassTag[U] = ClassTag(keyDecoderClass)
    implicit val valueCmd: ClassTag[T] = ClassTag(valueDecoderClass)

    createStream[K, V, U, T](
      jssc.ssc, kafkaParams.toMap, Map(topics.mapValues(_.intValue()).toSeq: _*), storageLevel)
  }

  /** get leaders for the given offset ranges, or throw an exception */
  private def leadersForRanges(
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange]): Map[TopicAndPartition, (String, Int)] = {
    val kc = new KafkaCluster(kafkaParams)
    val topics = offsetRanges.map(o => TopicAndPartition(o.topic, o.partition)).toSet
    val leaders = kc.findLeaders(topics).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
    leaders
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   */
  @Experimental
  def createRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange]
    ): RDD[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val leaders = leadersForRanges(kafkaParams, offsetRanges)
    new KafkaRDD[K, V, KD, VD, (K, V)](sc, kafkaParams, offsetRanges, leaders, messageHandler)
  }

  /**
   * :: Experimental ::
   * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param leaders Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty map,
   *   in which case leaders will be looked up on the driver.
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  @Experimental
  def createRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange],
      leaders: Map[TopicAndPartition, Broker],
      messageHandler: MessageAndMetadata[K, V] => R
    ): RDD[R] = {
    val leaderMap = if (leaders.isEmpty) {
      leadersForRanges(kafkaParams, offsetRanges)
    } else {
      // This could be avoided by refactoring KafkaRDD.leaders and KafkaCluster to use Broker
      leaders.map {
        case (tp: TopicAndPartition, Broker(host, port)) => (tp, (host, port))
      }.toMap
    }
    new KafkaRDD[K, V, KD, VD, R](sc, kafkaParams, offsetRanges, leaderMap, messageHandler)
  }


  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   */
  @Experimental
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange]
    ): JavaPairRDD[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    new JavaPairRDD(createRDD[K, V, KD, VD](
      jsc.sc, Map(kafkaParams.toSeq: _*), offsetRanges))
  }

  /**
   * :: Experimental ::
   * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param leaders Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty map,
   *   in which case leaders will be looked up on the driver.
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  @Experimental
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaRDD[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val leaderMap = Map(leaders.toSeq: _*)
    createRDD[K, V, KD, VD, R](
      jsc.sc, Map(kafkaParams.toSeq: _*), offsetRanges, leaderMap, messageHandler.call _)
  }

  /**
   * :: Experimental ::
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on 
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  @Experimental
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      messageHandler: MessageAndMetadata[K, V] => R
  ): InputDStream[R] = {
    new DirectKafkaInputDStream[K, V, KD, VD, R](
      ssc, kafkaParams, fromOffsets, messageHandler)
  }

  /**
   * :: Experimental ::
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on 
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   */
  @Experimental
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
  ): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val kc = new KafkaCluster(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)

    (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      val fromOffsets = leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
      }
      new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, fromOffsets, messageHandler)
    }).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
  }

  /**
   * :: Experimental ::
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on 
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class of the value decoder
   * @param recordClass Class of the records in DStream
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  @Experimental
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaInputDStream[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    createDirectStream[K, V, KD, VD, R](
      jssc.ssc,
      Map(kafkaParams.toSeq: _*),
      Map(fromOffsets.mapValues { _.longValue() }.toSeq: _*),
      messageHandler.call _
    )
  }

  /**
   * :: Experimental ::
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on 
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class type of the value decoder
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   */
  @Experimental
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      topics: JSet[String]
    ): JavaPairInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    createDirectStream[K, V, KD, VD](
      jssc.ssc,
      Map(kafkaParams.toSeq: _*),
      Set(topics.toSeq: _*)
    )
  }
}

/**
 * This is a helper class that wraps the KafkaUtils.createStream() into more
 * Python-friendly class and function so that it can be easily
 * instantiated and called from Python's KafkaUtils (see SPARK-6027).
 *
 * The zero-arg constructor helps instantiate this class from the Class object
 * classOf[KafkaUtilsPythonHelper].newInstance(), and the createStream()
 * takes care of known parameters instead of passing them from Python
 */
private class KafkaUtilsPythonHelper {
  def createStream(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel): JavaPairReceiverInputDStream[Array[Byte], Array[Byte]] = {
    KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      jssc,
      classOf[Array[Byte]],
      classOf[Array[Byte]],
      classOf[DefaultDecoder],
      classOf[DefaultDecoder],
      kafkaParams,
      topics,
      storageLevel)
  }
}
