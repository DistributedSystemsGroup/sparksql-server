package fr.eurecom.dsg.sparksqlserver

import fr.eurecom.dsg.sparksqlserver.container.{RewrittenBag, OptimizedBag, DAGContainer, AnalysedBag}
import fr.eurecom.dsg.sparksqlserver.costmodel.CostModel
import fr.eurecom.dsg.sparksqlserver.costmodel.udcm.MRShareCM
import fr.eurecom.dsg.sparksqlserver.detector.Detector
import fr.eurecom.dsg.sparksqlserver.listener.DAGQueue
import fr.eurecom.dsg.sparksqlserver.optimizer.{OptimizationExecutor, Optimizer}
import fr.eurecom.dsg.sparksqlserver.optimizer.optimizers.MRShareOptimizer
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteExecutor
import fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler.FinalScheduler
import fr.eurecom.dsg.sparksqlserver.scheduler.prescheduler.DAGSelector
import fr.eurecom.dsg.sparksqlserver.util.ServerConstants
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
/**
 * All the executions of SparkSQL Server will be happened here
 * @param sc
 * @param queue
 */
class WorksharingExecutor(sc : SparkContext, queue : DAGQueue) extends Thread {

  private var processingDAG : ArrayBuffer[DAGContainer] = new ArrayBuffer[DAGContainer](ServerConstants.DAG_QUEUE_WINDOW_SIZE)

  private val preSched : DAGSelector = new DAGSelector

  private val detector : Detector = new Detector

  private val optimizer : OptimizationExecutor = new OptimizationExecutor

  private val rewriter : RewriteExecutor = new RewriteExecutor

  private val postSched : FinalScheduler = new FinalScheduler

  override def run(): Unit = {

    while(true) {
      Thread.sleep(ServerConstants.DAG_QUEUE_SLEEP_PERIOD)

      if(queue.queue.size == ServerConstants.DAG_QUEUE_WINDOW_SIZE) {

        for(i <-0 to ServerConstants.DAG_QUEUE_WINDOW_SIZE - 1)
          processingDAG = processingDAG :+ queue.queue.dequeue

        val selected : ArrayBuffer[DAGContainer] = preSched.select(processingDAG)
        val analysed : Array[AnalysedBag] = detector.detect(selected)
        val optimized : Array[OptimizedBag] = optimizer.optimize(analysed)
        val rewritten : Array[RewrittenBag] = rewriter.rewrite(optimized)
        postSched.schedule(sc, rewritten)

        processingDAG.clear()

      }

    }
  }

}