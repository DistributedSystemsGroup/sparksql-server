package fr.eurecom.dsg

import org.apache.spark._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

//import org.apache.hadoop.mapred.{JobConf, TextOutputFormat}

import org.apache.hadoop.mapred.{RecordWriter, JobConf, TextOutputFormat}
import org.apache.hadoop.util.Progressable

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

import org.apache.spark.Partitioner

import org.apache.hadoop.fs.FileSystem;


import java.util

import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.Progressable
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.DataFrame

import org.apache.spark._
import org.apache.spark.SparkContext._

//import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.sql.catalyst.plans.logical.{Union, LogicalPlan}
//import org.apache.spark.sql.catalyst.trees.TreeNode
import sun.reflect.generics.tree.BaseType

import org.apache.spark.storage.StorageLevel

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.fs.FileSystem;

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] with Logging{

  override def getRecordWriter(fs: FileSystem, job: JobConf,
    name: String, arg3: Progressable): RecordWriter[Any, Any] =  {
    val myFS : FileSystem = fs;
    val myName: String = generateLeafFileName(name);
    val  myJob: JobConf = job;
    val myProgressable: Progressable = arg3;

    new RecordWriter[Any, Any]() {

      // a cache storing the record writers for different output files.
      var recordWriters : util.TreeMap[String, RecordWriter[Any, Any]] = new util.TreeMap[String, RecordWriter[Any, Any]]();

    def write(key: Any, value: Any) {

        val keyArr = key.asInstanceOf[Tuple2[_,_]]

        val actualValue = generateActualValue(key, value);

        // get the file name based on the key
        val keyBasedPath:String = generateFileNameForKeyValue(key, value, myName);

        // get the file name based on the input file name
        var finalPath:String = getInputFileBasedOutputFileName(myJob, keyBasedPath);

        finalPath = finalPath + "-" + keyArr._1.toString

        var rw: RecordWriter[Any, Any] = this.recordWriters.get(finalPath);
        if (rw == null) {
          // if we don't have the record writer yet for the final path, create
          // one
          // and add it to the cache
          rw = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
          this.recordWriters.put(finalPath, rw);
        }
        rw.write(keyArr._2, actualValue);
      };

      def close(reporter: Reporter) {
        val keys = this.recordWriters.keySet().iterator()
        while (keys.hasNext)
        {
          val rw : RecordWriter[Any, Any] = this.recordWriters.get(keys.next);
          rw.close(reporter);
        }
        this.recordWriters.clear();
      };
    };
  }
}

object WordCount {
                                                   
    def main(args: Array[String]) {

        // TODO: println usage
    
        val conf : SparkConf = new SparkConf()

        //------Begin set application's name------
    
        //Number of WC jobs
        val noJob = args(0).toInt

        //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        val appName = "WordCount Input Tagging - " + noJob + " jobs"

        conf.setAppName(appName)

        val sc = new SparkContext(conf)

        val input = sc.textFile(args(1))
    
        def gx(x: Any) = for(i <-0 to noJob-1) yield (i, x)

//        val partitioner = new IdentityIntPartitioner(noJob)

        val wc = input.flatMap(_.split(" "))
                      .flatMap(x => gx(x))
                      .map(x => (x,1))
                      .reduceByKey(_ + _)
                      //.map(x => (x._1._1, (x._1._2,x._2)))
                      //.coalesce(1)
                      //.partitionBy(partitioner)
                      //.saveAsTextFile(args(2))
                      //.collect.foreach(println)
                      .saveAsHadoopFile(args(2), classOf[Tuple2[_,_]], classOf[Integer], classOf[RDDMultipleTextOutputFormat])

    }
}

