package fr.eurecom.dsg.sparksqlserver.optimizer

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.{DummyCM, MRShareCM}
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.{DummyOptimizer, MRShareOptimizer}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/17/15.
 */
class OptimizationExecutor() {

  def optimize(analysed : Array[AnalysedBag]): Array[OptimizedBag] = {

    //Array of optimizedBag, each optimizedBag is belong to one kind of sharing
    //it can be shared or can not.
    var res : Array[OptimizedBag] = Array.empty[OptimizedBag]

    for (i <-0 to analysed.length - 1) {
      var optimizer : Optimizer = null
      val lstDag : ArrayBuffer[Array[DAGContainer]] = analysed{i}.getListDag()
      for (j <-0 to lstDag.length - 1)
        if (lstDag{j}.length > 1) {
          optimizer = getOptimizer(analysed{i}, lstDag{j})
          res = res :+ optimizer.execute()
        } else {
          res = res :+ new OptimizedBag(lstDag{j}, analysed{i}.getSharingType(), false, null)
        }
    }
    res
  }

  def getOptimizer(analysed : AnalysedBag, arrDag : Array[DAGContainer]) :Optimizer = {
    var optimizer : Optimizer = null
    if (analysed.getSharingType() == "SCAN")
      optimizer = new MRShareOptimizer(arrDag, new MRShareCM)
    else
    if (analysed.getSharingType() == "DUMMY")
      optimizer = new DummyOptimizer(arrDag, new DummyCM)

    optimizer
  }

}
