package fr.eurecom.dsg.sparksqlserver.optimizer

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.MRShareCM

/**
 * Created by hoang on 6/17/15.
 */
class Optimizer(anabag : AnalysedBag, cm: CostModel) {

  def initiate(): Unit = {

  }

  def optimize(): OptimizedBag = {
    null
  }

}
