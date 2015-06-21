package fr.eurecom.dsg.sparksqlserver.optimizer.optimizers

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.optimizer.Optimizer
/**
 * Created by hoang on 6/17/15.
 */
class MRShareOptimizer(ana : AnalysedBag, cm: CostModel) extends Optimizer(ana, cm) {

  override def initiate(): Unit = {

  }

  override def optimize(): OptimizedBag = {
    null
  }

}
