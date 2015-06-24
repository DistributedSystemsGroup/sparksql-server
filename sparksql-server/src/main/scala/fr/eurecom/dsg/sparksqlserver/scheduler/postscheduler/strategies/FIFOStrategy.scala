package fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.strategies

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.{PostSchedulingStrategy, PostScheduler}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by hoang on 6/24/15.
 */
class FIFOStrategy extends PostSchedulingStrategy{

  override def execute(sc : SparkContext, rewritten : Array[RewrittenBag]) : Unit = {

    for(i <-0 to rewritten.length - 1) {
      val lstDag : Array[DAGContainer] = rewritten{i}.getListDag()
      if(rewritten{i}.getRewriteRuleName.contains("SCAN"))
        scanScheduling(lstDag, rewritten{i}.getRewriteRuleName)
      else
        if(rewritten{i}.getRewriteRuleName.contains("JOIN")) {
        }

  }

  def scanScheduling(lstDag : Array[DAGContainer], rlName : String): Unit = {
    lstDag{0}.getDAG().saveAsTextFile("")
    if(lstDag.length > 1)
      for (i <-1 to lstDag.length - 1) {
        val job : JobConcurrent = new JobConcurrent(lstDag{i}.getDAG(), "")
        job.start()
      }
    }
  }

  class JobConcurrent(rdd: RDD[_], output: String) extends Thread {
    override def run(): Unit = {
      rdd.saveAsTextFile(output)
    }
  }

}
