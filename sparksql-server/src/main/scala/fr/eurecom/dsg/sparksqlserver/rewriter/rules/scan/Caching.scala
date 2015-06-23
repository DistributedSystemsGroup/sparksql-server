package fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule
import org.apache.spark.Dependency
import org.apache.spark.rdd.{ShuffledRDD, RDD}

/**
 * Created by hoang on 6/18/15.
 */
class Caching extends RewriteRule{

  def getScanRDD(dep : Seq[Dependency[_]], arr : Array[RDD[_]]) : Array[RDD[_]] = {
    var arrTmp : Array[RDD[_]] = Array.empty[RDD[_]]
    if(dep{0}.rdd.getDP.size !=0){
      arrTmp = arrTmp ++ getScanRDD(dep{0}.rdd.getDP, arrTmp)
    }
    else{
      if(dep{0}.rdd.dependencies.size!=0) {
        val tmpDep = dep{0}.rdd.dependencies
        for(i <-0 to tmpDep.size - 1)
          arrTmp = arrTmp ++ getScanRDD(tmpDep{i}.rdd.getDP, arrTmp)
      } else {
        arrTmp = arrTmp :+ dep{0}.rdd
      }
    }
    arrTmp
  }

  override def execute(opt : OptimizedBag): RewrittenBag = {
    val lstDag : Array[DAGContainer] = opt.getListDag()
    val firstDag : RDD[_] = lstDag{0}.getDAG()
    var scan : Array[RDD[_]] = Array.empty[RDD[_]]
    if(firstDag.isInstanceOf[ShuffledRDD[_,_,_]])
      scan = getScanRDD(firstDag.asInstanceOf[ShuffledRDD[_,_,_]].getPrev().getDP, scan)
    else
      scan = getScanRDD(firstDag.getDP, scan)

    //cache it!
    for (i <- 0 to scan.length - 1)
      scan{i}.cache()

    //replace this cached scanRDD of this firstDAG to the rest DAGs in the Bag

    //turn it into a rewrittenBag and return it

    null
  }

}
