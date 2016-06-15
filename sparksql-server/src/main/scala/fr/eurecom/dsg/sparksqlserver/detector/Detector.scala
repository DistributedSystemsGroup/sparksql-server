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

package fr.eurecom.dsg.sparksqlserver.detector

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.detector.rules.{Noop, ScanSharing}
import fr.eurecom.dsg.sparksqlserver.listener.DAGQueue

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/17/15.
 */
class Detector() {

  private var ruleSet : Array[DetectionRule] = Array.empty[DetectionRule]

  def addToRuleSet(r : DetectionRule) : Unit = {
    ruleSet = ruleSet :+ r
  }

  def detect(listDAG : ArrayBuffer[DAGContainer]) : Array[AnalysedBag] = {

    var res : Array[AnalysedBag] = Array.empty[AnalysedBag]

    //val scan : DetectionRule = new ScanSharing(listDAG)
    //scan.initiate()
    //addToRuleSet(scan)

    val noop : DetectionRule = new Noop(listDAG)
    noop.initiate()
    addToRuleSet(noop)

    //val join : DetectionRule = new JoinSharing(queue)
    //join.initiate()
    //addToRuleSet(join)

    for (i <-0 to ruleSet.length - 1) {
      val tmp : ArrayBuffer[Array[DAGContainer]] = ruleSet{i}.analyse()
      if (ruleSet{i}.isInstanceOf[ScanSharing])
        res = res :+ new AnalysedBag("SCAN", tmp) //"SCAN_MRSHARE"
      else if (ruleSet{i}.isInstanceOf[Noop])
        res = res :+ new AnalysedBag("NOOP", tmp)
      //if (ruleSet{i}.isInstanceOf[JoinSharing])
      // res = res :+ new AnalysedBag("JOIN", tmp)
    }
    ruleSet = Array.empty[DetectionRule]
    res
  }

}
