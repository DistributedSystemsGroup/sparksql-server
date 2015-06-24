package fr.eurecom.dsg.sparksqlserver.util

/**
 * Created by hoang on 6/2/15.
 */
object ServerConstants {
  val DAG_QUEUE_SLEEP_PERIOD = 5000
  val DAG_QUEUE_WINDOW_SIZE = 2
  val PRE_SCHEDULING_STRATEGY = "DUMMY"
  val POST_SCHEDULING_STRATEGY = "FIFO"
}
