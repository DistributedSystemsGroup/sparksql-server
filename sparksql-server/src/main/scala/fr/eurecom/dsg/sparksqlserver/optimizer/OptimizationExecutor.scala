package fr.eurecom.dsg.sparksqlserver.optimizer

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.MRShareCM
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.MRShareOptimizer

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/17/15.
 */
class OptimizationExecutor() {

  def optimize(analysed : Array[AnalysedBag]): Array[OptimizedBag] = {

    //Array of optimizedBag, each optimizedBag is belong to one kind of sharing
    //it can be shared or can not.
    var res : Array[OptimizedBag] = Array.empty[OptimizedBag]
    var optimizer : Optimizer = null

    for (i <-0 to analysed.length - 1) {
      if(analysed{i}.getSharingType() == "SCAN") {
        val lstDag : ArrayBuffer[Array[DAGContainer]] = analysed{i}.getListDag()
        for (j <-0 to lstDag.length - 1)
          if (lstDag{j}.length > 1) {
            optimizer = new MRShareOptimizer(lstDag{j}, new MRShareCM)
            res = res :+ optimizer.execute()
          }
          else {
            res = res :+ new OptimizedBag(lstDag{j}, "SCAN", false, null)
          }
      }
    }

    res
  }

}
