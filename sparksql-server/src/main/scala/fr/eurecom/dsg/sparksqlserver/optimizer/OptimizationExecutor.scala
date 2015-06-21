package fr.eurecom.dsg.sparksqlserver.optimizer

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.MRShareCM
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.MRShareOptimizer

/**
 * Created by hoang on 6/17/15.
 */
class OptimizationExecutor() {

  def optimize(analysed : Array[AnalysedBag]): Array[OptimizedBag] = {

    var res : Array[OptimizedBag] = Array.empty[OptimizedBag]

    for (i <-0 to analysed.length - 1) {
      if (analysed{i}.getListDag().length > 1) {
        if(analysed{i}.getSharingType() == "SCAN") {
          val optimizer : Optimizer = new MRShareOptimizer(analysed{i}, new MRShareCM)
          res = res :+ optimizer.optimize()
        }
      } else
        res = res :+ analysed{i}.toOptimizedBag()
    }

    res
  }

}
