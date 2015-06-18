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

package org.apache.spark.deploy

import java.net.URI
import java.util.jar.JarFile

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.deploy.SparkSubmitAction._
import org.apache.spark.util.Utils

/**
 * Parses and encapsulates arguments from the spark-submit script.
 * The env argument is used for testing.
 */
private[spark] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env) {
  var master: String = null
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var packages: String = null
  var repositories: String = null
  var ivyRepoPath: String = null
  var verbose: Boolean = false
  var isPython: Boolean = false
  var pyFiles: String = null
  var action: SparkSubmitAction = null
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null

  // Standalone cluster mode only
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = true // used internally

  /** Default properties present in the currently defined defaults file. */
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    if (verbose) SparkSubmit.printStream.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      Utils.getPropertiesFromFile(filename).foreach { case (k, v) =>
        if (k.startsWith("spark.")) {
          defaultProperties(k) = v
          if (verbose) SparkSubmit.printStream.println(s"Adding default property: $k=$v")
        } else {
          SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
        }
      }
    }
    defaultProperties
  }

  // Set parameters from command line arguments
  parseOpts(args.toList)
  // Populate `sparkProperties` map from properties file
  mergeDefaultSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters
  loadEnvironmentArguments()

  validateArguments()

  /**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
   */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /**
   * Load arguments from environment variables, Spark properties etc.
   */
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get("spark.driver.cores"))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get("spark.executor.memory"))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get("spark.executor.cores"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get("spark.cores.max"))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get("spark.jars")).orNull
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull
    deployMode = Option(deployMode).orElse(env.get("DEPLOY_MODE")).orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get("spark.executor.instances").orNull)

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            val jar = new JarFile(uri.getPath)
            // Note that this might still return null if no main-class is set; we catch that later
            mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
          } catch {
            case e: Exception =>
              SparkSubmit.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          SparkSubmit.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
            "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local[*]")

    // In YARN mode, app name can be set via SPARK_YARN_APP_NAME (see SPARK-5222)
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }

    // Action should be SUBMIT unless otherwise specified
    action = Option(action).getOrElse(SUBMIT)
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
    }
  }

  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python file)")
    }
    if (mainClass == null && !isPython) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (pyFiles != null && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        throw new Exception(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }
  }

  private def validateKillArguments(): Unit = {
    if (!master.startsWith("spark://")) {
      SparkSubmit.printErrorAndExit("Killing submissions is only supported in standalone mode!")
    }
    if (submissionToKill == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to kill.")
    }
  }

  private def validateStatusRequestArguments(): Unit = {
    if (!master.startsWith("spark://")) {
      SparkSubmit.printErrorAndExit(
        "Requesting submission statuses is only supported in standalone mode!")
    }
    if (submissionToRequestStatusFor == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to request status for.")
    }
  }

  def isStandaloneCluster: Boolean = {
    master.startsWith("spark://") && deployMode == "cluster"
  }

  override def toString = {
    s"""Parsed arguments:
    |  master                  $master
    |  deployMode              $deployMode
    |  executorMemory          $executorMemory
    |  executorCores           $executorCores
    |  totalExecutorCores      $totalExecutorCores
    |  propertiesFile          $propertiesFile
    |  driverMemory            $driverMemory
    |  driverCores             $driverCores
    |  driverExtraClassPath    $driverExtraClassPath
    |  driverExtraLibraryPath  $driverExtraLibraryPath
    |  driverExtraJavaOptions  $driverExtraJavaOptions
    |  supervise               $supervise
    |  queue                   $queue
    |  numExecutors            $numExecutors
    |  files                   $files
    |  pyFiles                 $pyFiles
    |  archives                $archives
    |  mainClass               $mainClass
    |  primaryResource         $primaryResource
    |  name                    $name
    |  childArgs               [${childArgs.mkString(" ")}]
    |  jars                    $jars
    |  packages                $packages
    |  repositories            $repositories
    |  verbose                 $verbose
    |
    |Spark properties used, including those specified through
    | --conf and those from the properties file $propertiesFile:
    |${sparkProperties.mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /**
   * Fill in values by parsing user options.
   * NOTE: Any changes here must be reflected in YarnClientSchedulerBackend.
   */
  private def parseOpts(opts: Seq[String]): Unit = {
    val EQ_SEPARATED_OPT="""(--[^=]+)=(.+)""".r

    // Delineates parsing of Spark options from parsing of user options.
    parse(opts)

    /**
     * NOTE: If you add or remove spark-submit options,
     * modify NOT ONLY this file but also utils.sh
     */
    def parse(opts: Seq[String]): Unit = opts match {
      case ("--name") :: value :: tail =>
        name = value
        parse(tail)

      case ("--master") :: value :: tail =>
        master = value
        parse(tail)

      case ("--class") :: value :: tail =>
        mainClass = value
        parse(tail)

      case ("--deploy-mode") :: value :: tail =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value
        parse(tail)

      case ("--num-executors") :: value :: tail =>
        numExecutors = value
        parse(tail)

      case ("--total-executor-cores") :: value :: tail =>
        totalExecutorCores = value
        parse(tail)

      case ("--executor-cores") :: value :: tail =>
        executorCores = value
        parse(tail)

      case ("--executor-memory") :: value :: tail =>
        executorMemory = value
        parse(tail)

      case ("--driver-memory") :: value :: tail =>
        driverMemory = value
        parse(tail)

      case ("--driver-cores") :: value :: tail =>
        driverCores = value
        parse(tail)

      case ("--driver-class-path") :: value :: tail =>
        driverExtraClassPath = value
        parse(tail)

      case ("--driver-java-options") :: value :: tail =>
        driverExtraJavaOptions = value
        parse(tail)

      case ("--driver-library-path") :: value :: tail =>
        driverExtraLibraryPath = value
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      case ("--kill") :: value :: tail =>
        submissionToKill = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $KILL.")
        }
        action = KILL
        parse(tail)

      case ("--status") :: value :: tail =>
        submissionToRequestStatusFor = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $REQUEST_STATUS.")
        }
        action = REQUEST_STATUS
        parse(tail)

      case ("--supervise") :: tail =>
        supervise = true
        parse(tail)

      case ("--queue") :: value :: tail =>
        queue = value
        parse(tail)

      case ("--files") :: value :: tail =>
        files = Utils.resolveURIs(value)
        parse(tail)

      case ("--py-files") :: value :: tail =>
        pyFiles = Utils.resolveURIs(value)
        parse(tail)

      case ("--archives") :: value :: tail =>
        archives = Utils.resolveURIs(value)
        parse(tail)

      case ("--jars") :: value :: tail =>
        jars = Utils.resolveURIs(value)
        parse(tail)

      case ("--packages") :: value :: tail =>
        packages = value
        parse(tail)

      case ("--repositories") :: value :: tail =>
        repositories = value
        parse(tail)

      case ("--conf" | "-c") :: value :: tail =>
        value.split("=", 2).toSeq match {
          case Seq(k, v) => sparkProperties(k) = v
          case _ => SparkSubmit.printErrorAndExit(s"Spark config without '=': $value")
        }
        parse(tail)

      case ("--proxy-user") :: value :: tail =>
        proxyUser = value
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case ("--verbose" | "-v") :: tail =>
        verbose = true
        parse(tail)

      case ("--version") :: tail =>
        SparkSubmit.printVersionAndExit()

      case EQ_SEPARATED_OPT(opt, value) :: tail =>
        parse(opt :: value :: tail)

      case value :: tail if value.startsWith("-") =>
        SparkSubmit.printErrorAndExit(s"Unrecognized option '$value'.")

      case value :: tail =>
        primaryResource =
          if (!SparkSubmit.isShell(value) && !SparkSubmit.isInternal(value)) {
            Utils.resolveURI(value).toString
          } else {
            value
          }
        isPython = SparkSubmit.isPython(value)
        childArgs ++= tail

      case Nil =>
    }
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    val outStream = SparkSubmit.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    outStream.println(
      """Usage: spark-submit [options] <app jar | python file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]
        |
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
        |                              on one of the worker machines inside the cluster ("cluster")
        |                              (Default: client).
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --packages                  Comma-separated list of maven coordinates of jars to include
        |                              on the driver and executor classpaths. Will search the local
        |                              maven repo, then maven central and any additional remote
        |                              repositories given by --repositories. The format for the
        |                              coordinates should be groupId:artifactId:version.
        |  --repositories              Comma-separated list of additional remote repositories to
        |                              search for the maven coordinates given with --packages.
        |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
        |                              on the PYTHONPATH for Python apps.
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor.
        |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 512M).
        |  --driver-java-options       Extra Java options to pass to the driver.
        |  --driver-library-path       Extra library path entries to pass to the driver.
        |  --driver-class-path         Extra class path entries to pass to the driver. Note that
        |                              jars added with --jars are automatically included in the
        |                              classpath.
        |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        |
        |  --proxy-user NAME           User to impersonate when submitting the application.
        |
        |  --help, -h                  Show this help message and exit
        |  --verbose, -v               Print additional debug output
        |  --version,                  Print the version of current Spark
        |
        | Spark standalone with cluster deploy mode only:
        |  --driver-cores NUM          Cores for driver (Default: 1).
        |  --supervise                 If given, restarts the driver on failure.
        |  --kill SUBMISSION_ID        If given, kills the driver specified.
        |  --status SUBMISSION_ID      If given, requests the status of the driver specified.
        |
        | Spark standalone and Mesos only:
        |  --total-executor-cores NUM  Total cores for all executors.
        |
        | YARN-only:
        |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
        |                              (Default: 1).
        |  --executor-cores NUM        Number of cores per executor (Default: 1).
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
        |  --num-executors NUM         Number of executors to launch (Default: 2).
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working directory of each executor.
      """.stripMargin
    )
    SparkSubmit.exitFn()
  }
}
