package fr.eurecom.dsg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object WordCount {

  def main(args: Array[String]) {
    
    // TODO: println usage
    
    val conf : SparkConf = new SparkConf()

    //------Begin set application's name------
    
    //Number of WC jobs
    val noJob = args(0).toInt

    //Sequential mode vs Concurrent mode: SEQ or CON
    val runningMode = args(1).toUpperCase

    if (runningMode == "SEQ")
      conf.set("spark.scheduler.mode", "FIFO")
    else
      conf.set("spark.scheduler.mode", "FAIR")

    //Caching or not: 0 or 1
    var caching = args(2).toInt

    var appName = args(0) + " WCs - " + runningMode + " - "

    if (caching == 1)
      appName = appName + "Caching"
    else
      appName = appName + "No Caching"

    //Force runJob or not: 0 or 1
    var force = args(3).toInt
    
    if(runningMode == "CON" && caching == 1)
        if(force == 1)
            appName = appName + " - Force runJob"
        else
            appName = appName + " - Dummy Action"

    conf.setAppName(appName)

    //------End set application's name------

    val sc = new SparkContext(conf)

    val input = sc.textFile(args(4))

    //caching + concurrent --> cache + dummy action
    //caching + sequential --> only cache
    if (caching == 1) {
      input.cache()
      if(runningMode == "CON") {
          val tStart = System.currentTimeMillis()
          if(force == 0)
            input.count()
          else {
            sc.runJob(input, (iter: Iterator[_]) => {})
          }
          println("Caching: " + (System.currentTimeMillis() - tStart))
      }
    }

    //for loop to repeat jobs
    //if runnning mode is sequential --> execute it sequentially
    //else --> put into a thread and execute it
    for ( i <- 0 to noJob - 1) {
      val oPath = args(5) + i
      val mapped = input.flatMap(_.split(" ")).map((_, 1))
      val wordCounts = mapped.reduceByKey(_ + _)
      if (runningMode == "SEQ") {
        val tStart = System.currentTimeMillis()
        wordCounts.saveAsTextFile(oPath)
        println("job" + i + ": " + (System.currentTimeMillis() - tStart))
      } else {
        val job : JobConcurrent = new JobConcurrent(wordCounts, i, oPath)
        job.start()
      }
    }
  }
}

//Threading class for running job concurrently
class JobConcurrent(rdd: RDD[_], id: Integer, output: String) extends Thread {
  override def run(): Unit = {
    println("running job" + id)
    val tStart = System.currentTimeMillis()
    rdd.saveAsTextFile(output)
    println("job" + id + ": " + (System.currentTimeMillis() - tStart))
  }
}
