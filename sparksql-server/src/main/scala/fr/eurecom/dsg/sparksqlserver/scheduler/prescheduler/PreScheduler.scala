package fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.strategies.DummyStrategy

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
/**
 * Receive DAG Queue and pass to WS detector
 * FIFO strategy for the moment
 */
class PreScheduler(strat : String) {

  def schedule(dagLst : ArrayBuffer[DAGContainer]) : ArrayBuffer[DAGContainer] = {
    var strategy : PreSchedulingStrategy = null

    if(strat == "DUMMY")
      strategy = new DummyStrategy

    strategy.execute(dagLst)
  }

}
