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

package fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler

import fr.eurecom.dsg.sparksqlserver.container.{RewrittenBag, DAGContainer}
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.strategies.FIFOStrategy
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 * Scheduler right before submitting the rewritten DAGs to the cluster
 * FIFO at the moment
 */
class PostScheduler(strat : String) {

  def schedule(sc : SparkContext, rewritten : Array[RewrittenBag]) : Unit = {

    var strategy : PostSchedulingStrategy = null

    if(strat == "FIFO")
      strategy = new FIFOStrategy
    else {

    }
    strategy.execute(sc, rewritten)
  }

}
