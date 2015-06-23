package fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule
import org.apache.spark.Dependency
import org.apache.spark.rdd.{ShuffledRDD, RDD}

/**
 * Created by hoang on 6/18/15.
 */
class Caching extends RewriteRule{

  def getPostScanRDD(dep : Seq[Dependency[_]], scan : RDD[_]): RDD[_] = {
    if(dep{0}.rdd.getDP{0}.rdd == scan)
      dep{0}.rdd
    else
      getPostScanRDD(dep{0}.rdd.getDP, scan)
  }

  def getScanRDD(dep : Seq[Dependency[_]]): RDD[_] = {
    if(dep{0}.rdd.toString.contains("Scan"))
      dep{0}.rdd
    else
      getScanRDD(dep{0}.rdd.getDP)
  }

  def replaceScan(dag : RDD[_], pst : RDD[_]): Unit = {

    var scan : RDD[_] = null
    var post : RDD[_] = null

    if(dag.isInstanceOf[ShuffledRDD[_,_,_]]) {
      scan = getScanRDD(dag.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP)
      post = getPostScanRDD(dag.asInstanceOf[ShuffledRDD[_,_,_]].getPrev().getDP, scan)
    }
    else {
      scan = getScanRDD(dag.getDP)
      post = getPostScanRDD(dag.getDP, scan)
    }

    post.setDeps(pst.getDP)

  }

  override def execute(opt : OptimizedBag): RewrittenBag = {
    val lstDag : Array[DAGContainer] = opt.getListDag()
    val firstDag : RDD[_] = lstDag{0}.getDAG()
    var scan : RDD[_] = null
    var post : RDD[_] = null
    if(firstDag.isInstanceOf[ShuffledRDD[_,_,_]]) {
      scan = getScanRDD(firstDag.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP)
      post = getPostScanRDD(firstDag.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP, scan)
    }
    else {
      scan = getScanRDD(firstDag.getDP)
      post = getPostScanRDD(firstDag.getDP, scan)
    }

    //cache it!
    scan.cache()

    //replace this cached scanRDD of this firstDAG to the rest DAGs in the Bag
    for (i <- 1 to lstDag.length - 1) {
      replaceScan(lstDag{i}.getDAG(), post)
    }
    //turn it into a rewrittenBag and return it
    new RewrittenBag(lstDag)
  }

}
