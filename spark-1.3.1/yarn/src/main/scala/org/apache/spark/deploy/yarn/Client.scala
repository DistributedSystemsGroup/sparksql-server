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

package org.apache.spark.deploy.yarn

import java.net.{InetAddress, UnknownHostException, URI, URISyntaxException}
import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer, Map}
import scala.util.{Try, Success, Failure}

import com.google.common.base.Objects

import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.Utils

private[spark] class Client(
    val args: ClientArguments,
    val hadoopConf: Configuration,
    val sparkConf: SparkConf)
  extends Logging {

  import Client._

  def this(clientArgs: ClientArguments, spConf: SparkConf) =
    this(clientArgs, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

  def this(clientArgs: ClientArguments) = this(clientArgs, new SparkConf())

  private val yarnClient = YarnClient.createYarnClient
  private val yarnConf = new YarnConfiguration(hadoopConf)
  private val credentials = UserGroupInformation.getCurrentUser.getCredentials
  private val amMemoryOverhead = args.amMemoryOverhead // MB
  private val executorMemoryOverhead = args.executorMemoryOverhead // MB
  private val distCacheMgr = new ClientDistributedCacheManager()
  private val isClusterMode = args.isClusterMode


  def stop(): Unit = yarnClient.stop()

  /* ------------------------------------------------------------------------------------- *
   | The following methods have much in common in the stable and alpha versions of Client, |
   | but cannot be implemented in the parent trait due to subtle API differences across    |
   | hadoop versions.                                                                      |
   * ------------------------------------------------------------------------------------- */

  /**
   * Submit an application running our ApplicationMaster to the ResourceManager.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
   * creating applications and setting up the application submission context. This was not
   * available in the alpha API.
   */
  def submitApplication(): ApplicationId = {
    yarnClient.init(yarnConf)
    yarnClient.start()

    logInfo("Requesting a new application from cluster with %d NodeManagers"
      .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

    // Get a new application from our RM
    val newApp = yarnClient.createApplication()
    val newAppResponse = newApp.getNewApplicationResponse()
    val appId = newAppResponse.getApplicationId()

    // Verify whether the cluster has enough resources for our AM
    verifyClusterResources(newAppResponse)

    // Set up the appropriate contexts to launch our AM
    val containerContext = createContainerLaunchContext(newAppResponse)
    val appContext = createApplicationSubmissionContext(newApp, containerContext)

    // Finally, submit and monitor the application
    logInfo(s"Submitting application ${appId.getId} to ResourceManager")
    yarnClient.submitApplication(appContext)
    appId
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
   * This uses the YarnClientApplication not available in the Yarn alpha API.
   */
  def createApplicationSubmissionContext(
      newApp: YarnClientApplication,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = newApp.getApplicationSubmissionContext
    appContext.setApplicationName(args.appName)
    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(containerContext)
    appContext.setApplicationType("SPARK")
    sparkConf.getOption("spark.yarn.maxAppAttempts").map(_.toInt) match {
      case Some(v) => appContext.setMaxAppAttempts(v)
      case None => logDebug("spark.yarn.maxAppAttempts is not set. " +
          "Cluster's default value will be used.")
    }
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(args.amMemory + amMemoryOverhead)
    capability.setVirtualCores(args.amCores)
    appContext.setResource(capability)
    appContext
  }

  /** Set up security tokens for launching our ApplicationMaster container. */
  private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))
  }

  /** Get the application report from the ResourceManager for an application we have submitted. */
  def getApplicationReport(appId: ApplicationId): ApplicationReport =
    yarnClient.getApplicationReport(appId)

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
   * If no security is enabled, the token returned by the report is null.
   */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  /**
   * Fail fast if we have requested more resources per container than is available in the cluster.
   */
  private def verifyClusterResources(newAppResponse: GetNewApplicationResponse): Unit = {
    val maxMem = newAppResponse.getMaximumResourceCapability().getMemory()
    logInfo("Verifying our application has not requested more than the maximum " +
      s"memory capability of the cluster ($maxMem MB per container)")
    val executorMem = args.executorMemory + executorMemoryOverhead
    if (executorMem > maxMem) {
      throw new IllegalArgumentException(s"Required executor memory (${args.executorMemory}" +
        s"+$executorMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster!")
    }
    val amMem = args.amMemory + amMemoryOverhead
    if (amMem > maxMem) {
      throw new IllegalArgumentException(s"Required AM memory (${args.amMemory}" +
        s"+$amMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster!")
    }
    logInfo("Will allocate AM container, with %d MB memory including %d MB overhead".format(
      amMem,
      amMemoryOverhead))

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves.
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
   * The file is only copied if the source and destination file systems are different. This is used
   * for preparing resources for launching the ApplicationMaster container. Exposed for testing.
   */
  private[yarn] def copyFileToRemote(
      destDir: Path,
      srcPath: Path,
      replication: Short): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    var destPath = srcPath
    if (!compareFs(srcFs, destFs)) {
      destPath = new Path(destDir, srcPath.getName())
      logInfo(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val fc = FileContext.getFileContext(qualifiedDestPath.toUri(), hadoopConf)
    fc.resolvePath(qualifiedDestPath)
  }

  /**
   * Upload any resources to the distributed cache if needed. If a resource is intended to be
   * consumed locally, set up the appropriate config for downstream code to handle it properly.
   * This is used for setting up a container launch context for our ApplicationMaster.
   * Exposed for testing.
   */
  def prepareLocalResources(appStagingDir: String): HashMap[String, LocalResource] = {
    logInfo("Preparing resources for our AM container")
    // Upload Spark and the application JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    val fs = FileSystem.get(hadoopConf)
    val dst = new Path(fs.getHomeDirectory(), appStagingDir)
    val nns = getNameNodesToAccess(sparkConf) + dst
    obtainTokensForNamenodes(nns, hadoopConf, credentials)

    val replication = sparkConf.getInt("spark.yarn.submit.file.replication",
      fs.getDefaultReplication(dst)).toShort
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    val oldLog4jConf = Option(System.getenv("SPARK_LOG4J_CONF"))
    if (oldLog4jConf.isDefined) {
      logWarning(
        "SPARK_LOG4J_CONF detected in the system environment. This variable has been " +
          "deprecated. Please refer to the \"Launching Spark on YARN\" documentation " +
          "for alternatives.")
    }

    /**
     * Copy the given main resource to the distributed cache if the scheme is not "local".
     * Otherwise, set the corresponding key in our SparkConf to handle it downstream.
     * Each resource is represented by a 3-tuple of:
     *   (1) destination resource name,
     *   (2) local path to the resource,
     *   (3) Spark property key to set if the scheme is not local
     */
    List(
      (SPARK_JAR, sparkJar(sparkConf), CONF_SPARK_JAR),
      (APP_JAR, args.userJar, CONF_SPARK_USER_JAR),
      ("log4j.properties", oldLog4jConf.orNull, null)
    ).foreach { case (destName, _localPath, confKey) =>
      val localPath: String = if (_localPath != null) _localPath.trim() else ""
      if (!localPath.isEmpty()) {
        val localURI = new URI(localPath)
        if (localURI.getScheme != LOCAL_SCHEME) {
          val src = getQualifiedLocalPath(localURI, hadoopConf)
          val destPath = copyFileToRemote(dst, src, replication)
          val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
          distCacheMgr.addResource(destFs, hadoopConf, destPath,
            localResources, LocalResourceType.FILE, destName, statCache)
        } else if (confKey != null) {
          // If the resource is intended for local use only, handle this downstream
          // by setting the appropriate property
          sparkConf.set(confKey, localPath)
        }
      }
    }

    /**
     * Do the same for any additional resources passed in through ClientArguments.
     * Each resource category is represented by a 3-tuple of:
     *   (1) comma separated list of resources in this category,
     *   (2) resource type, and
     *   (3) whether to add these resources to the classpath
     */
    val cachedSecondaryJarLinks = ListBuffer.empty[String]
    List(
      (args.addJars, LocalResourceType.FILE, true),
      (args.files, LocalResourceType.FILE, false),
      (args.archives, LocalResourceType.ARCHIVE, false)
    ).foreach { case (flist, resType, addToClasspath) =>
      if (flist != null && !flist.isEmpty()) {
        flist.split(',').foreach { file =>
          val localURI = new URI(file.trim())
          if (localURI.getScheme != LOCAL_SCHEME) {
            val localPath = new Path(localURI)
            val linkname = Option(localURI.getFragment()).getOrElse(localPath.getName())
            val destPath = copyFileToRemote(dst, localPath, replication)
            distCacheMgr.addResource(
              fs, hadoopConf, destPath, localResources, resType, linkname, statCache)
            if (addToClasspath) {
              cachedSecondaryJarLinks += linkname
            }
          } else if (addToClasspath) {
            // Resource is intended for local use only and should be added to the class path
            cachedSecondaryJarLinks += file.trim()
          }
        }
      }
    }
    if (cachedSecondaryJarLinks.nonEmpty) {
      sparkConf.set(CONF_SPARK_YARN_SECONDARY_JARS, cachedSecondaryJarLinks.mkString(","))
    }

    localResources
  }

  /**
   * Set up the environment for launching our ApplicationMaster container.
   */
  private def setupLaunchEnv(stagingDir: String): HashMap[String, String] = {
    logInfo("Setting up the launch environment for our AM container")
    val env = new HashMap[String, String]()
    val extraCp = sparkConf.getOption("spark.driver.extraClassPath")
    populateClasspath(args, yarnConf, sparkConf, env, extraCp)
    env("SPARK_YARN_MODE") = "true"
    env("SPARK_YARN_STAGING_DIR") = stagingDir
    env("SPARK_USER") = UserGroupInformation.getCurrentUser().getShortUserName()

    // Set the environment variables to be passed on to the executors.
    distCacheMgr.setDistFilesEnv(env)
    distCacheMgr.setDistArchivesEnv(env)

    // Pick up any environment variables for the AM provided through spark.yarn.appMasterEnv.*
    val amEnvPrefix = "spark.yarn.appMasterEnv."
    sparkConf.getAll
      .filter { case (k, v) => k.startsWith(amEnvPrefix) }
      .map { case (k, v) => (k.substring(amEnvPrefix.length), v) }
      .foreach { case (k, v) => YarnSparkHadoopUtil.addPathToEnvironment(env, k, v) }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
    // Allow users to specify some environment variables.
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs)
      // Pass SPARK_YARN_USER_ENV itself to the AM so it can use it to set up executor environments.
      env("SPARK_YARN_USER_ENV") = userEnvs
    }

    // In cluster mode, if the deprecated SPARK_JAVA_OPTS is set, we need to propagate it to
    // executors. But we can't just set spark.executor.extraJavaOptions, because the driver's
    // SparkContext will not let that set spark* system properties, which is expected behavior for
    // Yarn clients. So propagate it through the environment.
    //
    // Note that to warn the user about the deprecation in cluster mode, some code from
    // SparkConf#validateSettings() is duplicated here (to avoid triggering the condition
    // described above).
    if (isClusterMode) {
      sys.env.get("SPARK_JAVA_OPTS").foreach { value =>
        val warning =
          s"""
            |SPARK_JAVA_OPTS was detected (set to '$value').
            |This is deprecated in Spark 1.0+.
            |
            |Please instead use:
            | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
            | - ./spark-submit with --driver-java-options to set -X options for a driver
            | - spark.executor.extraJavaOptions to set -X options for executors
          """.stripMargin
        logWarning(warning)
        for (proc <- Seq("driver", "executor")) {
          val key = s"spark.$proc.extraJavaOptions"
          if (sparkConf.contains(key)) {
            throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
          }
        }
        env("SPARK_JAVA_OPTS") = value
      }
    }

    sys.env.get(ENV_DIST_CLASSPATH).foreach { dcp =>
      env(ENV_DIST_CLASSPATH) = dcp
    }

    env
  }

  /**
   * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
   * This sets up the launch environment, java options, and the command for launching the AM.
   */
  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse)
    : ContainerLaunchContext = {
    logInfo("Setting up container launch context for our AM")

    val appId = newAppResponse.getApplicationId
    val appStagingDir = getAppStagingDir(appId)
    val localResources = prepareLocalResources(appStagingDir)
    val launchEnv = setupLaunchEnv(appStagingDir)
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(launchEnv)

    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + args.amMemory + "m"

    val tmpDir = new Path(
      YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
      YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
    )
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // TODO: Remove once cpuset version is pushed out.
    // The context is, default gc for server class machines ends up using all cores to do gc -
    // hence if there are multiple containers in same node, Spark GC affects all other containers'
    // performance (which can be that of other Spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
    // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = launchEnv.get("SPARK_USE_CONC_INCR_GC").exists(_.toBoolean)
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:MaxTenuringThreshold=31"
      javaOpts += "-XX:SurvivorRatio=8"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
    }

    // Forward the Spark configuration to the application master / executors.
    // TODO: it might be nicer to pass these as an internal environment variable rather than
    // as Java options, due to complications with string parsing of nested quotes.
    for ((k, v) <- sparkConf.getAll) {
      javaOpts += YarnSparkHadoopUtil.escapeForShell(s"-D$k=$v")
    }

    // Include driver-specific java options if we are launching a driver
    if (isClusterMode) {
      val driverOpts = sparkConf.getOption("spark.driver.extraJavaOptions")
        .orElse(sys.env.get("SPARK_JAVA_OPTS"))
      driverOpts.foreach { opts =>
        javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
      }
      val libraryPaths = Seq(sys.props.get("spark.driver.extraLibraryPath"),
        sys.props.get("spark.driver.libraryPath")).flatten
      if (libraryPaths.nonEmpty) {
        prefixEnv = Some(Utils.libraryPathEnvPrefix(libraryPaths))
      }
      if (sparkConf.getOption("spark.yarn.am.extraJavaOptions").isDefined) {
        logWarning("spark.yarn.am.extraJavaOptions will not take effect in cluster mode")
      }
    } else {
      // Validate and include yarn am specific java options in yarn-client mode.
      val amOptsKey = "spark.yarn.am.extraJavaOptions"
      val amOpts = sparkConf.getOption(amOptsKey)
      amOpts.foreach { opts =>
        if (opts.contains("-Dspark")) {
          val msg = s"$amOptsKey is not allowed to set Spark options (was '$opts'). "
          throw new SparkException(msg)
        }
        if (opts.contains("-Xmx") || opts.contains("-Xms")) {
          val msg = s"$amOptsKey is not allowed to alter memory settings (was '$opts')."
          throw new SparkException(msg)
        }
        javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
      }
    }

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClass =
      if (isClusterMode) {
        Seq("--class", YarnSparkHadoopUtil.escapeForShell(args.userClass))
      } else {
        Nil
      }
    val userJar =
      if (args.userJar != null) {
        Seq("--jar", args.userJar)
      } else {
        Nil
      }
    val primaryPyFile =
      if (args.primaryPyFile != null) {
        Seq("--primary-py-file", args.primaryPyFile)
      } else {
        Nil
      }
    val pyFiles =
      if (args.pyFiles != null) {
        Seq("--py-files", args.pyFiles)
      } else {
        Nil
      }
    val amClass =
      if (isClusterMode) {
        Class.forName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
      } else {
        Class.forName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
      }
    if (args.primaryPyFile != null && args.primaryPyFile.endsWith(".py")) {
      args.userArgs = ArrayBuffer(args.primaryPyFile, args.pyFiles) ++ args.userArgs
    }
    val userArgs = args.userArgs.flatMap { arg =>
      Seq("--arg", YarnSparkHadoopUtil.escapeForShell(arg))
    }
    val amArgs =
      Seq(amClass) ++ userClass ++ userJar ++ primaryPyFile ++ pyFiles ++ userArgs ++
        Seq(
          "--executor-memory", args.executorMemory.toString + "m",
          "--executor-cores", args.executorCores.toString,
          "--num-executors ", args.numExecutors.toString)

    // Command for the ApplicationMaster
    val commands = prefixEnv ++ Seq(
        YarnSparkHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java", "-server"
      ) ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands)

    logDebug("===============================================================================")
    logDebug("Yarn AM launch context:")
    logDebug(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
    logDebug("    env:")
    launchEnv.foreach { case (k, v) => logDebug(s"        $k -> $v") }
    logDebug("    resources:")
    localResources.foreach { case (k, v) => logDebug(s"        $k -> $v")}
    logDebug("    command:")
    logDebug(s"        ${printableCommands.mkString(" ")}")
    logDebug("===============================================================================")

    // send the acl settings into YARN to control who has access via YARN interfaces
    val securityManager = new SecurityManager(sparkConf)
    amContainer.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager))
    setupSecurityToken(amContainer)
    UserGroupInformation.getCurrentUser().addCredentials(credentials)

    amContainer
  }

  /**
   * Report the state of an application until it has exited, either successfully or
   * due to some failure, then return a pair of the yarn application state (FINISHED, FAILED,
   * KILLED, or RUNNING) and the final application state (UNDEFINED, SUCCEEDED, FAILED,
   * or KILLED).
   *
   * @param appId ID of the application to monitor.
   * @param returnOnRunning Whether to also return the application state when it is RUNNING.
   * @param logApplicationReport Whether to log details of the application report every iteration.
   * @return A pair of the yarn application state and the final application state.
   */
  def monitorApplication(
      appId: ApplicationId,
      returnOnRunning: Boolean = false,
      logApplicationReport: Boolean = true): (YarnApplicationState, FinalApplicationStatus) = {
    val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)
    var lastState: YarnApplicationState = null
    while (true) {
      Thread.sleep(interval)
      val report = getApplicationReport(appId)
      val state = report.getYarnApplicationState

      if (logApplicationReport) {
        logInfo(s"Application report for $appId (state: $state)")
        val details = Seq[(String, String)](
          ("client token", getClientToken(report)),
          ("diagnostics", report.getDiagnostics),
          ("ApplicationMaster host", report.getHost),
          ("ApplicationMaster RPC port", report.getRpcPort.toString),
          ("queue", report.getQueue),
          ("start time", report.getStartTime.toString),
          ("final status", report.getFinalApplicationStatus.toString),
          ("tracking URL", report.getTrackingUrl),
          ("user", report.getUser)
        )

        // Use more loggable format if value is null or empty
        val formattedDetails = details
          .map { case (k, v) =>
          val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
          s"\n\t $k: $newValue" }
          .mkString("")

        // If DEBUG is enabled, log report details every iteration
        // Otherwise, log them every time the application changes state
        if (log.isDebugEnabled) {
          logDebug(formattedDetails)
        } else if (lastState != state) {
          logInfo(formattedDetails)
        }
      }

      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        return (state, report.getFinalApplicationStatus)
      }

      if (returnOnRunning && state == YarnApplicationState.RUNNING) {
        return (state, report.getFinalApplicationStatus)
      }

      lastState = state
    }

    // Never reached, but keeps compiler happy
    throw new SparkException("While loop is depleted! This should never happen...")
  }

  /**
   * Submit an application to the ResourceManager and monitor its state.
   * This continues until the application has exited for any reason.
   * If the application finishes with a failed, killed, or undefined status,
   * throw an appropriate SparkException.
   */
  def run(): Unit = {
    val (yarnApplicationState, finalApplicationStatus) = monitorApplication(submitApplication())
    if (yarnApplicationState == YarnApplicationState.FAILED ||
      finalApplicationStatus == FinalApplicationStatus.FAILED) {
      throw new SparkException("Application finished with failed status")
    }
    if (yarnApplicationState == YarnApplicationState.KILLED ||
      finalApplicationStatus == FinalApplicationStatus.KILLED) {
      throw new SparkException("Application is killed")
    }
    if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
      throw new SparkException("The final status of application is undefined")
    }
  }
}

object Client extends Logging {
  def main(argStrings: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a " +
        "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf

    val args = new ClientArguments(argStrings, sparkConf)
    new Client(args, sparkConf).run()
  }

  // Alias for the Spark assembly jar and the user jar
  val SPARK_JAR: String = "__spark__.jar"
  val APP_JAR: String = "__app__.jar"

  // URI scheme that identifies local resources
  val LOCAL_SCHEME = "local"

  // Staging directory for any temporary jars or files
  val SPARK_STAGING: String = ".sparkStaging"

  // Location of any user-defined Spark jars
  val CONF_SPARK_JAR = "spark.yarn.jar"
  val ENV_SPARK_JAR = "SPARK_JAR"

  // Internal config to propagate the location of the user's jar to the driver/executors
  val CONF_SPARK_USER_JAR = "spark.yarn.user.jar"

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  val CONF_SPARK_YARN_SECONDARY_JARS = "spark.yarn.secondary.jars"

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Distribution-defined classpath to add to processes
  val ENV_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH"

  /**
   * Find the user-defined Spark jar if configured, or return the jar containing this
   * class if not.
   *
   * This method first looks in the SparkConf object for the CONF_SPARK_JAR key, and in the
   * user environment if that is not found (for backwards compatibility).
   */
  private def sparkJar(conf: SparkConf): String = {
    if (conf.contains(CONF_SPARK_JAR)) {
      conf.get(CONF_SPARK_JAR)
    } else if (System.getenv(ENV_SPARK_JAR) != null) {
      logWarning(
        s"$ENV_SPARK_JAR detected in the system environment. This variable has been deprecated " +
          s"in favor of the $CONF_SPARK_JAR configuration variable.")
      System.getenv(ENV_SPARK_JAR)
    } else {
      SparkContext.jarOfClass(this.getClass).head
    }
  }

  /**
   * Return the path to the given application's staging directory.
   */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(SPARK_STAGING, appId.toString())
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String])
    : Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    for (c <- classPathElementsToAdd.flatten) {
      YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[YarnConfiguration].getField("DEFAULT_YARN_APPLICATION_CLASSPATH")
      val value = field.get(null).asInstanceOf[Array[String]]
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default YARN Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default YARN application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * In Hadoop 0.23, the MR application classpath comes with the YARN application
   * classpath. In Hadoop 2.0, it's an array of Strings, and in 2.2+ it's a String.
   * So we need to use reflection to retrieve it.
   */
  private[yarn] def getDefaultMRApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[MRJobConfig].getField("DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH")
      val value = if (field.getType == classOf[String]) {
        StringUtils.getStrings(field.get(null).asInstanceOf[String]).toArray
      } else {
        field.get(null).asInstanceOf[Array[String]]
      }
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default MR Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default MR application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * Populate the classpath entry in the given environment map.
   *
   * User jars are generally not added to the JVM's system classpath; those are handled by the AM
   * and executor backend. When the deprecated `spark.yarn.user.classpath.first` is used, user jars
   * are included in the system classpath, though. The extra class path and other uploaded files are
   * always made available through the system class path.
   *
   * @param args Client arguments (when starting the AM) or null (when starting executors).
   */
  private[yarn] def populateClasspath(
      args: ClientArguments,
      conf: Configuration,
      sparkConf: SparkConf,
      env: HashMap[String, String],
      extraClassPath: Option[String] = None): Unit = {
    extraClassPath.foreach(addClasspathEntry(_, env))
    addClasspathEntry(
      YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), env
    )
    if (sparkConf.getBoolean("spark.yarn.user.classpath.first", false)) {
      val userClassPath =
        if (args != null) {
          getUserClasspath(Option(args.userJar), Option(args.addJars))
        } else {
          getUserClasspath(sparkConf)
        }
      userClassPath.foreach { x =>
        addFileToClasspath(x, null, env)
      }
    }
    addFileToClasspath(new URI(sparkJar(sparkConf)), SPARK_JAR, env)
    populateHadoopClasspath(conf, env)
    sys.env.get(ENV_DIST_CLASSPATH).foreach(addClasspathEntry(_, env))
  }

  /**
   * Returns a list of URIs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Array[URI] = {
    getUserClasspath(conf.getOption(CONF_SPARK_USER_JAR),
      conf.getOption(CONF_SPARK_YARN_SECONDARY_JARS))
  }

  private def getUserClasspath(
      mainJar: Option[String],
      secondaryJars: Option[String]): Array[URI] = {
    val mainUri = mainJar.orElse(Some(APP_JAR)).map(new URI(_))
    val secondaryUris = secondaryJars.map(_.split(",")).toSeq.flatten.map(new URI(_))
    (mainUri ++ secondaryUris).toArray
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   *
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   *
   * If not a "local:" file and no alternate name, the environment is not modified.
   *
   * @param uri       URI to add to classpath (optional).
   * @param fileName  Alternate name for the file (optional).
   * @param env       Map holding the environment variables.
   */
  private def addFileToClasspath(
      uri: URI,
      fileName: String,
      env: HashMap[String, String]): Unit = {
    if (uri != null && uri.getScheme == LOCAL_SCHEME) {
      addClasspathEntry(uri.getPath, env)
    } else if (fileName != null) {
      addClasspathEntry(buildPath(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), fileName), env)
    }
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  private def addClasspathEntry(path: String, env: HashMap[String, String]): Unit =
    YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  /**
   * Get the list of namenodes the user may access.
   */
  private[yarn] def getNameNodesToAccess(sparkConf: SparkConf): Set[Path] = {
    sparkConf.get("spark.yarn.access.namenodes", "")
      .split(",")
      .map(_.trim())
      .filter(!_.isEmpty)
      .map(new Path(_))
      .toSet
  }

  private[yarn] def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }
    delegTokenRenewer
  }

  /**
   * Obtains tokens for the namenodes passed in and adds them to the credentials.
   */
  private def obtainTokensForNamenodes(
      paths: Set[Path],
      conf: Configuration,
      creds: Credentials): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      val delegTokenRenewer = getTokenRenewer(conf)
      paths.foreach { dst =>
        val dstFs = dst.getFileSystem(conf)
        logDebug("getting token for namenode: " + dst)
        dstFs.addDelegationTokens(delegTokenRenewer, creds)
      }
    }
  }

  /**
   * Return whether the two file systems are the same.
   */
  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Whether to consider jars provided by the user to have precedence over the Spark jars when
   * loading user classes.
   */
  def isUserClassPathFirst(conf: SparkConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.getBoolean("spark.driver.userClassPathFirst", false)
    } else {
      conf.getBoolean("spark.executor.userClassPathFirst",
        conf.getBoolean("spark.files.userClassPathFirst", false))
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

}
