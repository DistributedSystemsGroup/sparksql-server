package fr.eurecom.dsg.sparksqlserver.optimizer.optimizers

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, DAGContainer}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.optimizer.Optimizer
import fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan.{InputTagging, Caching}

/**
 * Created by hoang on 6/22/15.
 */
class DummyOptimizer(ana : Array[DAGContainer], cm: CostModel) extends Optimizer(ana, cm) {

  private var sharingType : String = "DUMMY"

  override def execute() : OptimizedBag = {
    if (cm.compute().asInstanceOf[Some[_]].x.asInstanceOf[Boolean])
      new OptimizedBag(ana, sharingType, true, new Caching)
    else
      new OptimizedBag(ana, sharingType, true, new InputTagging)
  }
}
