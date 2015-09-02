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

package fr.eurecom.dsg.sparksqlserver.costmodel.udcm

import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.MRShareJob

import scala.math._

/**
 * Created by hoang on 6/17/15.
 */
class MRShareCM extends CostModel {

  val f = 1

  val g = 2.3

  def checkFirstIteration(job: Array[MRShareJob], t: Int, u: Int) : Boolean = {
    var res = true
    for(i<- t+1 to u)
      if(job{i}.id - job{i-1}.id != 1)
        res = false
    res
  }

  def GAIN(job : Array[MRShareJob], t: Int, u: Int): Float = {
    var res : Float = 0
    var aggregatedD : Float = job{u}.d
    var currentD : Int = job{u}.p
    for(i<-t to u) {
      if(checkFirstIteration(job, t, u))
        if(t == 0 && t!=u)
          res = res + gain(job{i}, job{u}, 1)
        else
          res = res + gain(job{i}, job{u}, 0)
      else
        res = res + gain(job{i}, job{u}, 0)
    }
    res
  }

  def gain(i : MRShareJob, j : MRShareJob, deltaJ: Int) : Float = {
    f - 2 * i.d * (j.p - i.p + deltaJ)
  }

  def GS(job : Array[MRShareJob], t: Int, u: Int) : Float = {
    if(t==u)
      0
    else
      GAIN(job, t, u) - f
  }


  override def compute() : Unit = {

  }

}
