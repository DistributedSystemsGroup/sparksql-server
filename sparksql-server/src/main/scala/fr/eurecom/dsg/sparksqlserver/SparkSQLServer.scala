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

package fr.eurecom.dsg.sparksqlserver

import java.net._
import fr.eurecom.dsg.sparksqlserver.groupingsets._
import fr.eurecom.dsg.sparksqlserver.listener.{DAGListener, JarListenerThread, DAGQueue}
import fr.eurecom.dsg.sparksqlserver.util.ServerConstants
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.language.existentials

/** Main entry point for SparkSQL Server */

object SparkSQLServer extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkSQLServer")

    //global queue, use to queue the incoming DAGs from clients
    var queue: DAGQueue = new DAGQueue()

    //path to this jar file, use to add new classLoader from clients
    val jarPath: URL = this.getClass().getProtectionDomain().getCodeSource().getLocation.toURI.toURL

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc: SparkContext = new SparkContext(conf)

    //set this SparkApplication as SparkSQLServer
    sc.setAsSparkSQLServer

    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)
    sc.createBroadcast("README.md",0)

    val sqlC = new org.apache.spark.sql.SQLContext(sc)

    //jarlistener Thread for jar receiving
    //we need to receive the jar first, because it contains many user-defined classes,
    //that would be very important when we receive the DAG
    val JarListener: JarListenerThread = new JarListenerThread()
    logInfo("JarListener is running on port " + ServerConstants.JAR_LISTENER_PORT)
    JarListener.start()

    //daglistener Thread for rdds, dependencies receiving
    //after receive successfully DAG from client, add it to the global queue
    val server: DAGListener = new DAGListener(sc, sqlC, jarPath, queue)
    logInfo("DAGListener is running on port " + ServerConstants.DAG_LISTENER_PORT)
    server.start()

    //check queue, if the window is reached, do the optimized!
    val wsExe: WorksharingExecutor = new WorksharingExecutor(sc, queue)
    wsExe.start()
    logInfo("WorksharingExecutor is running...")
}

