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

package fr.eurecom.dsg.sparksqlserver.listener

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.util.ServerConstants
import org.apache.spark.Logging

import scala.collection.mutable.Queue

/**
 * Created by hoang on 6/1/15.
 * Global queue to receive the incoming DAGs from clients
 */
class DAGQueue() extends Logging {

  var queue : Queue[DAGContainer] = Queue()

  def DAGQueue {
    //logInfo("New DAGQueue")
  }

  /**
   * Just for debugging
   */
  def printQueue: Unit ={
    if (queue.length == 0)
      logInfo("No clientRDD...")
    else
      logInfo("Queue has: " + queue.length + " RDD(s).")

    if (queue.length == ServerConstants.DAG_QUEUE_WINDOW_SIZE) {
      //Pass them to the WS detector (invoke the WS detector)
      for (i <- 0 to queue.length - 1) {
        logInfo(queue(i).getDAG().toDebugString)
      }
    }
  }
}