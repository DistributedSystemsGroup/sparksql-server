package fr.eurecom.dsg.sparksqlserver.listener

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.util.ServerConstants
import org.apache.spark.Logging

import scala.collection.mutable.Queue

/**
 * Created by hoang on 6/1/15.
 */
class DAGQueue() extends Logging {

  var queue : Queue[DAGContainer] = Queue()

  def DAGQueue {
    logInfo("New DAGQueue")
  }

  def printQueue: Unit ={
    if (queue.length == 0)
      logInfo("No clientRDD...")
    else
      logInfo("Queue has: " + queue.length + " RDD(s).")

    if (queue.length == ServerConstants.DAG_QUEUE_WINDOW_SIZE) {
      //Pass them to the WS detector (invoke the WS detector)
      for (i <- 0 to queue.length - 1) {
        logInfo(queue(i).getDAG().toDebugString)
      }
    }
  }
}