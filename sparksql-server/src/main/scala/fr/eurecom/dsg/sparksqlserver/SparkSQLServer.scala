package fr.eurecom.dsg.sparksqlserver

import java.io._
import java.net._

import fr.eurecom.dsg.sparksqlserver.WorksharingExecutor
import fr.eurecom.dsg.sparksqlserver.listener.{DAGListener, JarListenerThread, DAGQueue}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.language.existentials

case class Person(name: String, age: Int)

object SparkSQLServer extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Test Spark")

    var queue: DAGQueue = new DAGQueue()

    val jarPath: URL = this.getClass().getProtectionDomain().getCodeSource().getLocation.toURI.toURL

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc: SparkContext = new SparkContext(conf)

    val sqlC = new org.apache.spark.sql.SQLContext(sc)

    import sqlC.implicits._

    //jarlistener for jar receiving
    val JarListener: JarListenerThread = new JarListenerThread()
    JarListener.start()

    //daglistener for rdds, dependencies receiving
    val server: DAGListener = new DAGListener(sc, sqlC, jarPath, queue)
    logInfo("DAGListener is running...")
    server.start()

    //check queue (will be passed to dag selector)
    val wsExe: WorksharingExecutor = new WorksharingExecutor(queue)
    wsExe.start()
    logInfo("ShareScanner is running...")
  }
}

