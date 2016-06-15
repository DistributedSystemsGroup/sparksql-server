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

package fr.eurecom.dsg.sparksqlserver.detector.rules

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.detector.DetectionRule
import org.apache.spark.Dependency
import org.apache.spark.rdd.ShuffledRDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
class ScanSharing(listDAG : ArrayBuffer[DAGContainer]) extends DetectionRule() {

  private val indicator : String = "SCAN" //"SCAN_MRSHARE"

  private val preBag : ArrayBuffer[Array[DAGContainer]] = ArrayBuffer()

  private var postBag : ArrayBuffer[Array[DAGContainer]] = ArrayBuffer()

  //update piggyback information
  override def initiate() : Unit = {
    //get sourcelist and add new piggyback information
    for (i <- 0 to listDAG.size - 1) {
      var sources : Array[String] = Array.empty[String]

      if(listDAG{i}.getDAG().isInstanceOf[ShuffledRDD[_,_,_]])
        sources = getInput(listDAG{i}.getDAG().asInstanceOf[ShuffledRDD[_,_,_]].getPrev().getDP, sources)
      else
        sources = getInput(listDAG{i}.getDAG().getDP, sources)

      listDAG{i}.updateMetadata(indicator, sources)
    }

    for (i <- 0 to listDAG.length - 1) {
      preBag += Array(listDAG{i})
      postBag += Array(listDAG{i})
    }
  }

  def getInput(dep : Seq[Dependency[_]], arr : Array[String]) : Array[String] = {

    var arrTmp : Array[String] = Array.empty[String]
    if(dep{0}.rdd.getDP.size !=0){
      arrTmp = arrTmp ++ getInput(dep{0}.rdd.getDP, arrTmp)
    }
    else{
      if(dep{0}.rdd.dependencies.size!=0) {
        val tmpDep = dep{0}.rdd.dependencies
        for(i <-0 to tmpDep.size - 1)
          arrTmp = arrTmp ++ getInput(tmpDep{i}.rdd.getDP, arrTmp)
      } else {
        arrTmp = arrTmp :+ dep{0}.rdd.toString().split(" "){0}
      }
    }
    arrTmp
  }

  def getSourceList(a: Array[DAGContainer]) : Array[String] = {
    var res : Array[String] = Array.empty[String]
    for (i <-0 to a.length - 1)
      res = res ++ a{i}.getMetadata().getDescriptor.get(indicator).asInstanceOf[Some[_]].x.asInstanceOf[Array[String]]
    res
  }

  //check if two arrays have the common part or not
  def check(a : Array[DAGContainer], b : Array[DAGContainer]) : Boolean = {
    var res = false
    val aSource = getSourceList(a)
    val bSource = getSourceList(b)

    if (aSource.intersect(bSource).size!=0)
      res = true
    res
  }

  //check and return new bags of dagcontainer
  override def analyse() : ArrayBuffer[Array[DAGContainer]] = {

    var preLength = 0
    var postLength = 0
    var i = 0
    var turn = 0

    while (turn < postBag.length){
      do {
        preLength = postBag.length
        i = 0
        while (i < preBag.length - 1) {
          if(turn!=(i+1) && check(preBag{turn}, preBag{i+1})) {
            postBag{turn} = postBag{turn} :+ preBag{i+1}{0}
            postBag -= postBag{i+1}
            preBag -= preBag(i+1)
          }
          i = i + 1
        }
        postLength = postBag.length
      } while(preLength!=postLength)
      turn = turn + 1
    }
    postBag
  }
}
