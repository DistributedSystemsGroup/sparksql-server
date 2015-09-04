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

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.MRShareCM
import fr.eurecom.dsg.sparksqlserver.optimizer.Optimizer
import fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan.MultiplePipelines

import scala.math._

/**
 * Created by hoang on 6/17/15.
 * MRShare Optimizer which uses the algorithms in MRShare Paper
 */
class MRShareOptimizer(ana : Array[DAGContainer], cm: MRShareCM) extends Optimizer(ana, cm) {

  private val sharingType : String = "SCAN"

  val fileSize = 9800

  val B = (737000/4096)

  val m = 79

  var mrsJobs : Array[MRShareJob] = new Array[MRShareJob](ana.length)

  def computeSortingPasses(di: Float): Int = {
    (ceil(log10(di*fileSize)/log10(B) - log10(m)/log10(B)) + ceil(log10(m)/log10(B))).toInt
  }

  def SplitMRShareJobs(jobs : Array[MRShareJob]): Array[Array[MRShareJob]] = {

    val N = jobs.size
    val c : Array[Float] = new Array(N)
    val source : Array[Int] = new Array(N)

    for(i<-0 to N - 1) {
      c{i} = 0
      source{i} = 0
    }

    for(l<-0 to N - 1) {
      var cTemp : Array[Float] = Array()
      for (i <- 0 to l) {
        val computedGS = cm.GS(jobs, i, l)
        if(i == 0)
          cTemp = cTemp :+ computedGS
        else
          cTemp = cTemp :+ (c{i-1} + computedGS)
      }

      if(l!=0) {
        c{l} = cTemp.max
        source{l} = cTemp.indexOf(cTemp.max)
      }
    }

    var grouped : Array[Array[MRShareJob]] = Array[Array[MRShareJob]]()

    var mark = source.size - 1

    for(i <- source.size - 1 to 0 by -1) {

      if(source{i} == i) {
        var smallGroup : Array[MRShareJob] = Array[MRShareJob]()
        for(j<-i to mark)
          smallGroup = smallGroup :+ jobs{j}
        grouped = grouped :+ smallGroup
        mark = i - 1
      }
    }
    grouped
  }

  def MultiSplitMRShareJobs(jobs : Array[MRShareJob]): Array[Array[MRShareJob]] = {

    var G : Array[Array[MRShareJob]] = Array[Array[MRShareJob]]()

    var J : Array[MRShareJob] = jobs

    while(J.size != 0) {
      val ALL : Array[Array[MRShareJob]] = SplitMRShareJobs(J)
      var SINGLES : Array[MRShareJob] = Array[MRShareJob]()
      for(i<-0 to ALL.size - 1)
        if(ALL{i}.size > 1)
          G = G :+ ALL{i}
        else
          SINGLES = SINGLES :+ ALL{i}{0}

      if(SINGLES.size < J.size)
        J = SINGLES
      else {
        val Jx = SINGLES{SINGLES.size - 1}
        G = G :+ Array(Jx)
        J = J.take(J.size - 1)
      }
    }
    G
  }

  def convertDAGContainertoMRShareJob() : Unit = {
    //initial job
    for(i <-0 to ana.length - 1) {
      val di = ana{i}.getMetadata().getDescriptor.get("mapOutputRatio").asInstanceOf[Float]
      val pi = computeSortingPasses(di)
      mrsJobs{i} = new MRShareJob(ana{i}, di, pi, 0)
    }

    //sort job
    scala.util.Sorting.stableSort(mrsJobs, (j1: MRShareJob, j2: MRShareJob) => j1.p < j2.p)

    //re-index
    for(i<-0 to mrsJobs.length - 1) {
      mrsJobs{i}.setIdx(i)
    }

  }

  override def initiate(): Unit = {
    convertDAGContainertoMRShareJob()
  }

  override def execute(): Array[OptimizedBag] = {
    var ret : Array[OptimizedBag] = Array[OptimizedBag]()
    ret = ret :+ new OptimizedBag(ana, sharingType, true, new MultiplePipelines)
    ret
  }

}

class MRShareJob(dc : DAGContainer, dx : Float, px: Int, idx: Int) {

  var d: Float = dx
  var p: Int = px

  var id: Int = idx

  val dagCtn : DAGContainer = dc

  def getDagContainer(): DAGContainer = dagCtn

  def setDi(dx : Float): Unit = {
    d = dx
  }

  def setPi(px : Int) : Unit = {
    p = px
  }

  def setIdx(idx : Int) : Unit = {
    id = idx
  }

}