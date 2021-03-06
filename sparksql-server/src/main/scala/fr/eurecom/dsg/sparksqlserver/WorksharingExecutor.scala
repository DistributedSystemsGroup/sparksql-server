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

import fr.eurecom.dsg.sparksqlserver.container.{RewrittenBag, OptimizedBag, DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.MRShareCM
import fr.eurecom.dsg.sparksqlserver.detector.Detector
import fr.eurecom.dsg.sparksqlserver.listener.DAGQueue
import fr.eurecom.dsg.sparksqlserver.optimizer.{OptimizationExecutor, Optimizer}
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.MRShareOptimizer
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteExecutor
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.PostScheduler
import fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.PreScheduler
import fr.eurecom.dsg.sparksqlserver.util.ServerConstants
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
/**
 * All the executions of SparkSQL Server will be happened here
 * @param sc
 * @param queue
 */
class WorksharingExecutor(sc : SparkContext, queue : DAGQueue) extends Thread {

  //DAGs which will be optimized
  private var processingDAG : ArrayBuffer[DAGContainer] = new ArrayBuffer[DAGContainer](ServerConstants.DAG_QUEUE_WINDOW_SIZE)

  //Pre Scheduler for some scheduling strategies (deadline, priority...)
  private val preSched : PreScheduler = new PreScheduler(ServerConstants.PRE_SCHEDULING_STRATEGY)

  //Sharing detector
  private val detector : Detector = new Detector

  //Optimizer the sharing potential DAGs
  private val optimizer : OptimizationExecutor = new OptimizationExecutor

  //Rewrite the optimized DAGs due to the rule the bag brings
  private val rewriter : RewriteExecutor = new RewriteExecutor

  //Submit to cluster
  private val postSched : PostScheduler = new PostScheduler(ServerConstants.POST_SCHEDULING_STRATEGY)

  override def run(): Unit = {

    while(true) {
      Thread.sleep(ServerConstants.DAG_QUEUE_SLEEP_PERIOD)

      if(queue.queue.size >= ServerConstants.DAG_QUEUE_WINDOW_SIZE) {

        val a : Map[Int, RDD[_]] =  sc.getPersistentRDDs

        for(i <-0 to a.size - 1)
          a.get(i+1).get.unpersist()

        for(i <-0 to ServerConstants.DAG_QUEUE_WINDOW_SIZE - 1)
          processingDAG = processingDAG :+ queue.queue.dequeue
        //processingDAG = {DAG1, DAG2, DAG3, DAG4, DAG5}

        val selected : ArrayBuffer[DAGContainer] = preSched.schedule(processingDAG)
        //selected = {DAG1, DAG2, DAG3, DAG4, DAG5} //pre-scheduling information will be added in the future

        val analysed : Array[AnalysedBag] = detector.detect(selected)
        //analysed = {[SCAN,{{DAG1, DAG2}, {DAG3, DAG4}, {DAG5}]}
        //analysed{0} is scan-sharing type, we could have analysed{1} is join-sharing type

        val optimized : Array[OptimizedBag] = optimizer.optimize(analysed)
        //optimized = {{SCAN, {DAG1, DAG2}, true, caching}, {SCAN, {DAG3, DAG4}, true, inputtagging}, {SCAN, {DAG5}, false, null}}

        val rewritten : Array[RewrittenBag] = rewriter.rewrite(sc, optimized)
        //rewritten = {{cache DAG1, DAG2}, {DAG34}, {DAG5}}

        postSched.schedule(sc, rewritten)

        processingDAG.clear()

      }

    }
  }

}