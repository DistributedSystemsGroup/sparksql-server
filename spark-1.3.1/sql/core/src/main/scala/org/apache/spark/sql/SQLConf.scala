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

package org.apache.spark.sql

import scala.collection.immutable
import scala.collection.JavaConversions._

import java.util.Properties

private[spark] object SQLConf {
  val COMPRESS_CACHED = "spark.sql.inMemoryColumnarStorage.compressed"
  val COLUMN_BATCH_SIZE = "spark.sql.inMemoryColumnarStorage.batchSize"
  val IN_MEMORY_PARTITION_PRUNING = "spark.sql.inMemoryColumnarStorage.partitionPruning"
  val AUTO_BROADCASTJOIN_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"
  val DEFAULT_SIZE_IN_BYTES = "spark.sql.defaultSizeInBytes"
  val SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val CODEGEN_ENABLED = "spark.sql.codegen"
  val DIALECT = "spark.sql.dialect"

  val PARQUET_BINARY_AS_STRING = "spark.sql.parquet.binaryAsString"
  val PARQUET_INT96_AS_TIMESTAMP = "spark.sql.parquet.int96AsTimestamp"
  val PARQUET_CACHE_METADATA = "spark.sql.parquet.cacheMetadata"
  val PARQUET_COMPRESSION = "spark.sql.parquet.compression.codec"
  val PARQUET_FILTER_PUSHDOWN_ENABLED = "spark.sql.parquet.filterPushdown"
  val PARQUET_USE_DATA_SOURCE_API = "spark.sql.parquet.useDataSourceApi"

  val COLUMN_NAME_OF_CORRUPT_RECORD = "spark.sql.columnNameOfCorruptRecord"
  val BROADCAST_TIMEOUT = "spark.sql.broadcastTimeout"

  // Options that control which operators can be chosen by the query planner.  These should be
  // considered hints and may be ignored by future versions of Spark SQL.
  val EXTERNAL_SORT = "spark.sql.planner.externalSort"

  // This is only used for the thriftserver
  val THRIFTSERVER_POOL = "spark.sql.thriftserver.scheduler.pool"

  // This is used to set the default data source
  val DEFAULT_DATA_SOURCE_NAME = "spark.sql.sources.default"
  // This is used to control the when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters). We will split the JSON string of a schema
  // to its length exceeds the threshold.
  val SCHEMA_STRING_LENGTH_THRESHOLD = "spark.sql.sources.schemaStringLengthThreshold"

  // Whether to perform eager analysis when constructing a dataframe.
  // Set to false when debugging requires the ability to look at invalid query plans.
  val DATAFRAME_EAGER_ANALYSIS = "spark.sql.eagerAnalysis"

  object Deprecated {
    val MAPRED_REDUCE_TASKS = "mapred.reduce.tasks"
  }
}

/**
 * A class that enables the setting and getting of mutable config parameters/hints.
 *
 * In the presence of a SQLContext, these can be set and queried by passing SET commands
 * into Spark SQL's query functions (i.e. sql()). Otherwise, users of this class can
 * modify the hints by programmatically calling the setters and getters of this class.
 *
 * SQLConf is thread-safe (internally synchronized, so safe to be used in multiple threads).
 */
private[sql] class SQLConf extends Serializable {
  import SQLConf._

  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  /** ************************ Spark SQL Params/Hints ******************* */
  // TODO: refactor so that these hints accessors don't pollute the name space of SQLContext?

  /**
   * The SQL dialect that is used when parsing queries.  This defaults to 'sql' which uses
   * a simple SQL parser provided by Spark SQL.  This is currently the only option for users of
   * SQLContext.
   *
   * When using a HiveContext, this value defaults to 'hiveql', which uses the Hive 0.12.0 HiveQL
   * parser.  Users can change this to 'sql' if they want to run queries that aren't supported by
   * HiveQL (e.g., SELECT 1).
   *
   * Note that the choice of dialect does not affect things like what tables are available or
   * how query execution is performed.
   */
  private[spark] def dialect: String = getConf(DIALECT, "sql")

  /** When true tables cached using the in-memory columnar caching will be compressed. */
  private[spark] def useCompression: Boolean = getConf(COMPRESS_CACHED, "true").toBoolean

  /** The compression codec for writing to a Parquetfile */
  private[spark] def parquetCompressionCodec: String = getConf(PARQUET_COMPRESSION, "gzip")

  /** The number of rows that will be  */
  private[spark] def columnBatchSize: Int = getConf(COLUMN_BATCH_SIZE, "10000").toInt

  /** Number of partitions to use for shuffle operators. */
  private[spark] def numShufflePartitions: Int = getConf(SHUFFLE_PARTITIONS, "200").toInt

  /** When true predicates will be passed to the parquet record reader when possible. */
  private[spark] def parquetFilterPushDown =
    getConf(PARQUET_FILTER_PUSHDOWN_ENABLED, "false").toBoolean

  /** When true uses Parquet implementation based on data source API */
  private[spark] def parquetUseDataSourceApi =
    getConf(PARQUET_USE_DATA_SOURCE_API, "true").toBoolean

  /** When true the planner will use the external sort, which may spill to disk. */
  private[spark] def externalSortEnabled: Boolean = getConf(EXTERNAL_SORT, "false").toBoolean

  /**
   * When set to true, Spark SQL will use the Scala compiler at runtime to generate custom bytecode
   * that evaluates expressions found in queries.  In general this custom code runs much faster
   * than interpreted evaluation, but there are significant start-up costs due to compilation.
   * As a result codegen is only beneficial when queries run for a long time, or when the same
   * expressions are used multiple times.
   *
   * Defaults to false as this feature is currently experimental.
   */
  private[spark] def codegenEnabled: Boolean = getConf(CODEGEN_ENABLED, "false").toBoolean

  /**
   * Upper bound on the sizes (in bytes) of the tables qualified for the auto conversion to
   * a broadcast value during the physical executions of join operations.  Setting this to -1
   * effectively disables auto conversion.
   *
   * Hive setting: hive.auto.convert.join.noconditionaltask.size, whose default value is 10000.
   */
  private[spark] def autoBroadcastJoinThreshold: Int =
    getConf(AUTO_BROADCASTJOIN_THRESHOLD, (10 * 1024 * 1024).toString).toInt

  /**
   * The default size in bytes to assign to a logical operator's estimation statistics.  By default,
   * it is set to a larger value than `autoBroadcastJoinThreshold`, hence any logical operator
   * without a properly implemented estimation of this statistic will not be incorrectly broadcasted
   * in joins.
   */
  private[spark] def defaultSizeInBytes: Long =
    getConf(DEFAULT_SIZE_IN_BYTES, (autoBroadcastJoinThreshold + 1).toString).toLong

  /**
   * When set to true, we always treat byte arrays in Parquet files as strings.
   */
  private[spark] def isParquetBinaryAsString: Boolean =
    getConf(PARQUET_BINARY_AS_STRING, "false").toBoolean

  /**
   * When set to true, we always treat INT96Values in Parquet files as timestamp.
   */
  private[spark] def isParquetINT96AsTimestamp: Boolean =
    getConf(PARQUET_INT96_AS_TIMESTAMP, "true").toBoolean

  /**
   * When set to true, partition pruning for in-memory columnar tables is enabled.
   */
  private[spark] def inMemoryPartitionPruning: Boolean =
    getConf(IN_MEMORY_PARTITION_PRUNING, "false").toBoolean

  private[spark] def columnNameOfCorruptRecord: String =
    getConf(COLUMN_NAME_OF_CORRUPT_RECORD, "_corrupt_record")

  /**
   * Timeout in seconds for the broadcast wait time in hash join
   */
  private[spark] def broadcastTimeout: Int =
    getConf(BROADCAST_TIMEOUT, (5 * 60).toString).toInt

  private[spark] def defaultDataSourceName: String =
    getConf(DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.parquet")

  // Do not use a value larger than 4000 as the default value of this property.
  // See the comments of SCHEMA_STRING_LENGTH_THRESHOLD above for more information.
  private[spark] def schemaStringLengthThreshold: Int =
    getConf(SCHEMA_STRING_LENGTH_THRESHOLD, "4000").toInt

  private[spark] def dataFrameEagerAnalysis: Boolean =
    getConf(DATAFRAME_EAGER_ANALYSIS, "true").toBoolean

  /** ********************** SQLConf functionality methods ************ */

  /** Set Spark SQL configuration properties. */
  def setConf(props: Properties): Unit = settings.synchronized {
    props.foreach { case (k, v) => settings.put(k, v) }
  }

  /** Set the given Spark SQL configuration property. */
  def setConf(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    settings.put(key, value)
  }

  /** Return the value of Spark SQL configuration property for the given key. */
  def getConf(key: String): String = {
    Option(settings.get(key)).getOrElse(throw new NoSuchElementException(key))
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`.
   */
  def getConf(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   */
  def getAllConfs: immutable.Map[String, String] = settings.synchronized { settings.toMap }

  private[spark] def unsetConf(key: String) {
    settings -= key
  }

  private[spark] def clear() {
    settings.clear()
  }
}

