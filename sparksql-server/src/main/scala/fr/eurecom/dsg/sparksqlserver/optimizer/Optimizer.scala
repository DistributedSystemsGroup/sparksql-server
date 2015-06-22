package fr.eurecom.dsg.sparksqlserver.optimizer

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel

/**
 * Created by hoang on 6/17/15.
 */
class Optimizer(anabag : Array[DAGContainer], cm: CostModel) {

  def initiate(): Unit = {

  }

  def execute(): OptimizedBag = {
    null
  }

}
