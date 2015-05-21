package fr.eurecom.dsg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    
    override def generateActualKey(key: Any, value: Any): Any = NullWritable.get()
    
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = key.asInstanceOf[String]
}

object WordCount {

  def main(args: Array[String]) {
    
    // TODO: println usage
    
    val conf : SparkConf = new SparkConf()

    //------Begin set application's name------
    
    //Number of WC jobs
    val noJob = args(0).toInt

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val appName = "WordCount Input Tagging - " + noJob + " jobs"

    conf.setAppName(appName)

    //------End set application's name------

    val sc = new SparkContext(conf)

    val input = sc.textFile(args(1))
    
    def replicate(x: Any) = for(i <-0 to noJob-1) yield ("q" + i, x)

    val wc = input.flatMap(_.split(" "))
                  .flatMap(x => replicate(x))
                  .map(x => (x,1))
                  .reduceByKey(_ + _).map(x => (x._1._1, (x._1._2,x._2)))

    wc.saveAsHadoopFile(args(2), classOf[String], classOf[String],classOf[RDDMultipleTextOutputFormat])
  }
}
