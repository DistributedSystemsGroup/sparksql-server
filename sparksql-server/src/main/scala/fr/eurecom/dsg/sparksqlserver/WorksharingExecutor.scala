package fr.eurecom.dsg.sparksqlserver

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.detector.Detector
import fr.eurecom.dsg.sparksqlserver.listener.DAGQueue
import fr.eurecom.dsg.sparksqlserver.optimizer.Optimizer
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.FinalScheduler
import fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.DAGSelector
import fr.eurecom.dsg.sparksqlserver.util.ServerConstants

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
/**
 * All the executions of SparkSQL Server will be happened here
 * @param queue
 */
class WorksharingExecutor(queue : DAGQueue) extends Thread {

  private var processingDAG : ArrayBuffer[DAGContainer] = new ArrayBuffer[DAGContainer](ServerConstants.DAG_QUEUE_WINDOW_SIZE)

  private val preSched : DAGSelector = new DAGSelector

  private val detector : Detector = new Detector

  private val optimizer : Optimizer = new Optimizer

  private val postSched : FinalScheduler = new FinalScheduler

  override def run(): Unit = {
    while(true) {
      Thread.sleep(ServerConstants.DAG_QUEUE_SLEEP_PERIOD)

      if(queue.queue.size == ServerConstants.DAG_QUEUE_WINDOW_SIZE) {

        for(i <-0 to ServerConstants.DAG_QUEUE_WINDOW_SIZE - 1)
          processingDAG = processingDAG :+ queue.queue.dequeue

        //preSched.schedule
        val analysed : Array[AnalysedBag] = detector.detect(processingDAG)
        //bagDAG* = optimizer.optimize(bagDAG)
        //postSched.schedule(bagDAG*)
      }

    }
  }
}
