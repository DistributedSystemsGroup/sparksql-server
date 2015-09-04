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

package fr.eurecom.dsg.sparksqlserver.optimizer.optimizers

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, DAGContainer}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.optimizer.Optimizer
import fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan.{MultiplePipelines, Caching}

/**
 * Created by hoang on 6/22/15.
 * Do nothing with this optimizer
 * Still in experimental phase
 */
class ScanCachingOptimizer(ana : Array[DAGContainer], cm: CostModel) extends Optimizer(ana, cm) {

  private val sharingType : String = "SCAN"

  override def execute() : Array[OptimizedBag] = {
    var res : Array[OptimizedBag] = Array[OptimizedBag]()
    res = res :+ new OptimizedBag(ana, sharingType, true, new Caching)
    res
  }
}
