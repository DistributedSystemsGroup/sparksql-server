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

package fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.strategies.DummyStrategy

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
/**
 * Receive DAG Queue and pass to WS detector
 * FIFO strategy for the moment
 */
class PreScheduler(strat : String) {

  /**
   * schedule the DAG list (reordering for an example, fifo at the moment)
   * @param dagLst
   * @return
   */
  def schedule(dagLst : ArrayBuffer[DAGContainer]) : ArrayBuffer[DAGContainer] = {
    var strategy : PreSchedulingStrategy = null

    if(strat == "DUMMY")
      strategy = new DummyStrategy

    strategy.execute(dagLst)
  }

}
