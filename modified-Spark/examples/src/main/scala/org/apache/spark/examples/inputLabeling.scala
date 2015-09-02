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
package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sqlserver.SimultaneousPipeline
import org.apache.spark.util.TimeStampedHashMap

object inputLabeling {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("inputLabeling")

  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setAsSparkSQLServer

    //read file
    //val textFile = sc.textFile(args {0})
    val textFile = sc.textFile("/Users/suwax/WORKSPACE-INTELLIJ/people.txt")
    if(!sc.checkBroadCastInfoExisted(1))
      sc.broadcastBySQLServer(new SerializableWritable(sc.hadoopConfiguration), 1)
    val countsTest1 = textFile.flatMap(_.split(" ")).filter2(word => (word.length < 3)).map((_, 1))
      .reduceByKey(_ + _)
    val countsTest2 = textFile.flatMap(_.split(" ")).filter2(word => (word.length < 5)).map((_, 1))
      .reduceByKey(_ + _)
    val countsTest3 = textFile.flatMap(_.split(" ")).filter2(word => (word.length < 7)).map((_, 1))
      .reduceByKey(_ + _)
    val countsTest4 = textFile.flatMap(_.split(" ")).filter2(word => (word.length < 9)).map((_, 1))
      .reduceByKey(_ + _)
    val countsTest5 = textFile.flatMap(_.split(" ")).filter2(word => (word.length < 11)).map((_, 1))
      .reduceByKey(_ + _)

    //countsTest1.saveAsTextFile(args {1})
    //countsTest2.saveAsTextFile(args {2})
    /*countsTest3.saveAsTextFile(args {3})
    countsTest4.saveAsTextFile(args {4})
    countsTest5.saveAsTextFile(args {5})*/

    //add dags to an array of dag
    var rdds: Array[RDD[_]] = Array[RDD[_]]()
    rdds = rdds :+ countsTest1 :+ countsTest2
    val multiPipeline = new SimultaneousPipeline(sc, rdds, "/Users/suwax/test2")
    val rdd: RDD[_] = multiPipeline.execute()
    rdd.saveAsTextFile("/Users/suwax/test2")

    while (true) {
      Thread.sleep(1000)
    }
  }
}
