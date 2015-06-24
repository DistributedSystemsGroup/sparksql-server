package fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler

import fr.eurecom.dsg.sparksqlserver.container.{RewrittenBag, DAGContainer}
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.strategies.FIFOStrategy
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
class PostScheduler(strat : String) {

  def schedule(sc : SparkContext, rewritten : Array[RewrittenBag]) : Unit = {

    var strategy : PostSchedulingStrategy = null

    if(strat == "FIFO")
      strategy = new FIFOStrategy
    else {

    }
    strategy.execute(sc, rewritten)
  }

}
