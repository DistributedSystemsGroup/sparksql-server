package fr.eurecom.dsg.sparksqlserver.detector

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.detector.rules.ScanSharing
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

    val scan : DetectionRule = new ScanSharing(listDAG)
    addToRuleSet(scan)

    //val join : DetectionRule = new JoinSharing(queue)
    //addToRuleSet(join)

    for (i <-0 to ruleSet.length) {
      val tmp : ArrayBuffer[Array[DAGContainer]] = ruleSet{i}.analyse()
      if (ruleSet{i}.isInstanceOf[ScanSharing])
        res = res :+ new AnalysedBag("SCAN", tmp)
      //if (ruleSet{i}.isInstanceOf[JoinSharing])
      // res = res :+ new AnalysedBag("JOIN", tmp)
    }

    res
  }

}
