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

package fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sqlserver.SimultaneousPipeline

/**
 * Created by hoang on 6/18/15.
 * Group a batch of jobs into one meta job
 * This batch of jobs is created after going through
 * the MRShareCostModel
 */
class MultiplePipelines extends RewriteRule {

  private val rewriteRuleName : String = "MRSHARE_SCAN"

  override def execute(sc: SparkContext, opt : OptimizedBag): RewrittenBag = {
    var rdds : Array[RDD[_]] = Array[RDD[_]]()
    val lstDAG : Array[DAGContainer] = opt.getListDag()
    for(i<-0 to lstDAG.length - 1) {
      println(lstDAG{i}.getDAG.toDebugString)
      rdds = rdds :+ lstDAG{i}.getDAG()
    }

    //create a SimultaneousPipeLine Executor and execute it
    val sm : SimultaneousPipeline = new SimultaneousPipeline(sc, rdds, "")
    val resDAG : RDD[_] = sm.execute()

    println(resDAG.toDebugString)

    //create a new RewrittenBag to return from the returned DAG
    val resDAGCtn : DAGContainer = new DAGContainer()
    resDAGCtn.setDAG(resDAG)
    var resArr : Array[DAGContainer] = Array[DAGContainer]()
    resArr = resArr :+ resDAGCtn

    new RewrittenBag(resArr, rewriteRuleName)
  }

}
