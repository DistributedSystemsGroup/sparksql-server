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

package fr.eurecom.dsg.sparksqlserver.optimizer

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.{ScanCachingCM, MRShareCM}
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.{ScanCachingOptimizer, MRShareOptimizer}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/17/15.
 * Optimize Executor, do the optimization
 */
class OptimizationExecutor() {
  /**
   * Get the optimizer associate to the sharingType and do the optimizations
   * @param analysed
   * @return Array of OptmizedBag
   */
  def optimize(analysed : Array[AnalysedBag]): Array[OptimizedBag] = {

    //Array of optimizedBag, each optimizedBag is belong to one kind of sharing
    //it can be shared or can not.
    var res : Array[OptimizedBag] = Array.empty[OptimizedBag]

    for (i <-0 to analysed.length - 1) {
      var optimizer : Optimizer = null
      val lstDag : ArrayBuffer[Array[DAGContainer]] = analysed{i}.getListDag()
      for (j <-0 to lstDag.length - 1)
        if (lstDag{j}.length > 1) {
          optimizer = getOptimizer(analysed{i}, lstDag{j})
          optimizer.initiate()
          val optimizedRes : Array[OptimizedBag] = optimizer.execute()
          for(k<-0 to optimizedRes.length - 1)
            res = res :+ optimizedRes{k}
        } else {
          res = res :+ new OptimizedBag(lstDag{j}, analysed{i}.getSharingType(), false, null)
        }
    }
    res
  }

  /**
   * return the optimizer associates to the sharingType
   * Caching for scan at the moment
   * @param analysed
   * @param arrDag
   * @return an Optimizer
   */
  def getOptimizer(analysed : AnalysedBag, arrDag : Array[DAGContainer]) :Optimizer = {
    var optimizer : Optimizer = null
    if (analysed.getSharingType() == "SCAN")
      //optimizer = new MRShareOptimizer(arrDag, new MRShareCM)
      optimizer = new ScanCachingOptimizer(arrDag, new ScanCachingCM)
    else
    if (analysed.getSharingType() == "DUMMY")
      optimizer = new ScanCachingOptimizer(arrDag, new ScanCachingCM)

    optimizer
  }

}
