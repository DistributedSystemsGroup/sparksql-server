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

package fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.strategies

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.{PostSchedulingStrategy, PostScheduler}
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by hoang on 6/24/15.
 * FIFOstrategy before submitting rewritten jobs to the cluster
 */
class FIFOStrategy extends PostSchedulingStrategy{

  /**
   * get RewriteRuleName, due to each kind of rule, we do the associated scheduling
   * @param sc
   * @param rewritten
   */
  override def execute(sc : SparkContext, rewritten : Array[RewrittenBag]) : Unit = {

    for(i <-0 to rewritten.length - 1) {
      val lstDag : Array[DAGContainer] = rewritten{i}.getListDag()
      if(rewritten{i}.getRewriteRuleName.contains("SCAN")) {
        if(rewritten{i}.getRewriteRuleName.contains("CACHING")) {
          scanSchedulingForCaching(lstDag, rewritten{i}.getRewriteRuleName)
        } else if(rewritten{i}.getRewriteRuleName.contains("MRSHARE")) {
          scanSchedulingForMRShare(lstDag, rewritten{i}.getRewriteRuleName)
        }
      }
      else
        if(rewritten{i}.getRewriteRuleName.contains("JOIN")) {
        }

  }
  /**
   * Scheduling for scan, which is using CACHE
   * Execute the first job in the list, then execute the rest concurrently
   * @param lstDag
   * @param rlName
   */
  def scanSchedulingForCaching(lstDag : Array[DAGContainer], rlName : String): Unit = {
    if(!sc.checkBroadCastInfoExisted(1))
      sc.broadcastBySQLServer(new SerializableWritable(sc.hadoopConfiguration), 1)
    var Opath : String = ""
    Opath = lstDag{0}.getMetadata().getDescriptor.get("OUTPUT").get.asInstanceOf[String]
    lstDag{0}.getDAG().saveAsTextFile(Opath)
    if(lstDag.length > 1)
      for (i <-1 to lstDag.length - 1) {
        Opath = lstDag{i}.getMetadata().getDescriptor.get("OUTPUT").get.asInstanceOf[String]
        val job : JobConcurrent = new JobConcurrent(lstDag{i}.getDAG(), Opath)
        job.start()
      }
  }

  /**
   * Scheduling for scan, which is using MRSHARE
   * Execute the jobs in FIFO order.
   * @param lstDag
   * @param rlName
   */
  def scanSchedulingForMRShare(lstDag: Array[DAGContainer], rlName: String): Unit = {
    if(!sc.checkBroadCastInfoExisted(1))
      sc.broadcastBySQLServer(new SerializableWritable(sc.hadoopConfiguration), 1)
    val Opath: String = System.currentTimeMillis().toString
    for(i<-0 to lstDag.size - 1)
      lstDag{i}.getDAG().saveAsTextFile(Opath)
  }
}

  class JobConcurrent(rdd: RDD[_], output: String) extends Thread {
    override def run(): Unit = {
      rdd.saveAsTextFile(output)
    }
  }

}
