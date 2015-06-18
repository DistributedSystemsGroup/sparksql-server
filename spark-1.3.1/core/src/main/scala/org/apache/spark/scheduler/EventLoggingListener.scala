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

package org.apache.spark.scheduler

import java.io._
import java.net.URI

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SPARK_VERSION}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * A SparkListener that logs events to persistent storage.
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 */
private[spark] class EventLoggingListener(
    appId: String,
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, logBaseDir: URI, sparkConf: SparkConf) =
    this(appId, logBaseDir, sparkConf, SparkHadoopUtil.get.newConfiguration(sparkConf))

  private val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = sparkConf.getBoolean("spark.eventLog.overwrite", false)
  private val testing = sparkConf.getBoolean("spark.eventLog.testing", false)
  private val outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf))
    } else {
      None
    }
  private val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  // The Hadoop APIs have changed over time, so we use reflection to figure out
  // the correct method to use to flush a hadoop data stream. See SPARK-1518
  // for details.
  private val hadoopFlushMethod = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }

  private var writer: Option[PrintWriter] = None

  // For testing. Keep track of all JSON serialized events that have been logged.
  private[scheduler] val loggedEvents = new ArrayBuffer[JValue]

  // Visible for tests only.
  private[scheduler] val logPath = getLogPath(logBaseDir, appId, compressionCodecName)

  /**
   * Creates the log file in the configured log directory.
   */
  def start() {
    if (!fileSystem.isDirectory(new Path(logBaseDir))) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir does not exist.")
    }

    val workingPath = logPath + IN_PROGRESS
    val uri = new URI(workingPath)
    val path = new Path(workingPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    if (shouldOverwrite && fileSystem.exists(path)) {
      logWarning(s"Event log $path already exists. Overwriting...")
      fileSystem.delete(path, true)
    }

    /* The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
     * Therefore, for local files, use FileOutputStream instead. */
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(fileSystem.create(path))
        hadoopDataStream.get
      }

    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)

      EventLoggingListener.initEventLog(bstream)
      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      writer = Some(new PrintWriter(bstream))
      logInfo("Logging events to %s".format(logPath))
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    writer.foreach(_.println(compact(render(eventJson))))
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(hadoopFlushMethod.invoke(_))
    }
    if (testing) {
      loggedEvents += eventJson
    }
  }

  // Events that do not trigger a flush
  override def onStageSubmitted(event: SparkListenerStageSubmitted) =
    logEvent(event)
  override def onTaskStart(event: SparkListenerTaskStart) =
    logEvent(event)
  override def onTaskGettingResult(event: SparkListenerTaskGettingResult) =
    logEvent(event)
  override def onTaskEnd(event: SparkListenerTaskEnd) =
    logEvent(event)
  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate) =
    logEvent(event)

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted) =
    logEvent(event, flushLogger = true)
  override def onJobStart(event: SparkListenerJobStart) =
    logEvent(event, flushLogger = true)
  override def onJobEnd(event: SparkListenerJobEnd) =
    logEvent(event, flushLogger = true)
  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded) =
    logEvent(event, flushLogger = true)
  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved) =
    logEvent(event, flushLogger = true)
  override def onUnpersistRDD(event: SparkListenerUnpersistRDD) =
    logEvent(event, flushLogger = true)
  override def onApplicationStart(event: SparkListenerApplicationStart) =
    logEvent(event, flushLogger = true)
  override def onApplicationEnd(event: SparkListenerApplicationEnd) =
    logEvent(event, flushLogger = true)
  override def onExecutorAdded(event: SparkListenerExecutorAdded) =
    logEvent(event, flushLogger = true)
  override def onExecutorRemoved(event: SparkListenerExecutorRemoved) =
    logEvent(event, flushLogger = true)

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate) { }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
   */
  def stop() = {
    writer.foreach(_.close())

    val target = new Path(logPath)
    if (fileSystem.exists(target)) {
      if (shouldOverwrite) {
        logWarning(s"Event log $target already exists. Overwriting...")
        fileSystem.delete(target, true)
      } else {
        throw new IOException("Target log file already exists (%s)".format(logPath))
      }
    }
    fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
  }

}

private[spark] object EventLoggingListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"
  val SPARK_VERSION_KEY = "SPARK_VERSION"
  val COMPRESSION_CODEC_KEY = "COMPRESSION_CODEC"

  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  /**
   * Write metadata about an event log to the given stream.
   * The metadata is encoded in the first line of the event log as JSON.
   *
   * @param logStream Raw output stream to the event log file.
   */
  def initEventLog(logStream: OutputStream): Unit = {
    val metadata = SparkListenerLogStart(SPARK_VERSION)
    val metadataJson = compact(JsonProtocol.logStartToJson(metadata)) + "\n"
    logStream.write(metadataJson.getBytes(Charsets.UTF_8))
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      compressionCodecName: Option[String] = None): String = {
    val sanitizedAppId = appId.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase
    // e.g. app_123, app_123.lzf
    val logName = sanitizedAppId + compressionCodecName.map { "." + _ }.getOrElse("")
    logBaseDir.toString.stripSuffix("/") + "/" + logName
  }

  /**
   * Opens an event log file and returns an input stream that contains the event data.
   *
   * @return input stream that holds one JSON record per line.
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    if (!fs.exists(log)) {
      throw new FileNotFoundException(s"File $log does not exist.")
    }

    val in = new BufferedInputStream(fs.open(log))

    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    val codec = codecName.map { c =>
      codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
    }

    try {
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Exception =>
        in.close()
        throw e
    }
  }

}
