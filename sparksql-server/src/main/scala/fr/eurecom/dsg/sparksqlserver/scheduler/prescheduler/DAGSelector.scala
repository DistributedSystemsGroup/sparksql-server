package fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
/**
 * Receive DAG Queue and pass to WS detector
 * FIFO strategy for the moment
 */
class DAGSelector() {

  def select(dagLst : ArrayBuffer[DAGContainer]) : ArrayBuffer[DAGContainer] = dagLst

}
