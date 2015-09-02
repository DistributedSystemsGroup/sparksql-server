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

package org.apache.spark.sqlserver.submitter

import java.io.{BufferedOutputStream, IOException, ObjectOutputStream}
import java.net.{InetAddress, Socket}
import java.security.CodeSource

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Logging, ShuffleDependency, Dependency, SparkContext}

class DAGSubmitter(x: RDD[_], info: String, y: SparkContext, id: Integer, ip: String, port: Int)
  extends Thread with Logging {

  private var df: String = "null"

  //send dependencies of each rdd to server
  def sendDependency(dep: Dependency[_], out: ObjectOutputStream): Unit = {
    var length: Int = dep.rdd.getDP.length

    if (dep.rdd.toString().contains("ToDataFrameHolder")) {
      df = dep.rdd.toString()
    }

    if (dep.rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      val prevRDD: RDD[_] = dep.rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev()
      //println(dep.rdd)
      var deps: Seq[Dependency[_]] = Nil
      deps = deps :+ dep
      out.writeObject(deps)
      out.writeObject(dep.rdd.dependencies)
      sendDAGPartially(prevRDD, prevRDD.getDP, out)
    }

    if (length == 0 && !dep.rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      var last: Seq[Dependency[_]] = Nil
      last = last :+ dep
      out.writeObject(last)
      //out.writeUTF(dep.rdd.name)
    }

    while (length != 0) {
      //println(dep.rdd.toString())
      var deps: Seq[Dependency[_]] = Nil
      deps = deps :+ dep
      out.writeObject(deps)
      if(dep.isInstanceOf[ShuffleDependency[_,_,_]]) {
        out.writeObject(dep.rdd)
      }
      length = length - 1
      for (i <- 0 to dep.rdd.getDP.length - 1)
        sendDependency(dep.rdd.getDP {
          i
        }, out)
    }
  }

  //Send DAGPartially by send rdd and its dependencies to server
  def sendDAGPartially(rdd: RDD[_], deps: Seq[Dependency[_]], out: ObjectOutputStream): Unit = {
    var dep: Dependency[_] = null
    if (rdd.toString().contains("ToDataFrameHolder")) {
      df = rdd.toString()
    }
    out.writeObject(rdd)
    for (dep <- deps)
      sendDependency(dep, out)
    //    out.writeObject(rdd)
  }

  override def run() {
    try {
      log.info("Connecting to SparkServer...")
      val ia = InetAddress.getByName("localhost")
      val socket = new Socket(ip, port)
      log.info("Successfully connected to SparkServer...")

      val out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()))

      //send integer number which indicates the jar's filename at server side
      out.writeInt(id)

      //send DAG partially to server
      sendDAGPartially(x, x.dependencies, out)

      //send dataframe creation information and the sql query
      out.writeObject(df + "__" + info)
      log.info("Successfully sent to SparkServer...")
      log.info("Closing connection...")
      out.flush()
      out.close()
      socket.close()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
