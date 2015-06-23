import java.security.CodeSource
import java.util.jar.{JarEntry, JarOutputStream}
import java.util.zip.{ZipOutputStream, ZipEntry, ZipInputStream}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.{KryoSerializer, Serializer}
//import org.apache.spark.sql.DataFrame

//import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.sql.catalyst.plans.logical.{Union, LogicalPlan}
//import org.apache.spark.sql.catalyst.trees.TreeNode
//import sun.reflect.generics.tree.BaseType

import _root_.org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import java.io._
import java.net.URL
import java.net.{InetAddress,ServerSocket,Socket,SocketException}
import java.util.{Arrays, Properties, UUID, Random}
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID.randomUUID

import scala.sys.process._

case class Person(name: String, age: Int)

object SimpleAppClient {

  def main(args: Array[String]): Unit = {

    var src : CodeSource = this.getClass().getProtectionDomain().getCodeSource()

    println(src.getLocation.getFile)

    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Test Spark")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.toDF().registerTempTable("people")
    //send table name to server in rddToDataFrameHolder / SQLContext
    //send sql query
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    val table_query = "people-SELECT name FROM people WHERE age >= 13 AND age <= 19-examples/src/main/resources/people.txt"
    val tmp = teenagers.map(t => "Name: " + t(0))

    val jarSender : JarSubmitter = new JarSubmitter(src)
    val id : Integer = jarSender.send()

    val client: SparkSubmitter = new SparkSubmitter(tmp, table_query, sc, src, id)
    client.start()
  }
}

class JarSubmitter (src : CodeSource) {

  def sendAnonfun(out : OutputStream): Unit = {

    //get jar location
    val path : String = src.getLocation.getFile
    //val path : String = "/home/hoang/DATA/WORKSPACE-INTELLIJ/untitled-2/out/artifacts/untitled_2_jar/untitled-2.jar"

    val dis : DataInputStream = new DataInputStream(new FileInputStream(new File(path)))

    //begin sending jar file
    var read: Integer = 0;
    var bytes: Array[Byte] = new Array[Byte](1024)

    while({read = dis.read(bytes); read != -1}){
      out.write(bytes, 0, read)
    }
    out.flush
    dis.close()
  }

  def send(): Integer = {
    var id : Integer = 0
    println("Connecting to JarServer...")
    val ia = InetAddress.getByName("localhost")
    val socket = new Socket(ia, 9992)
    println("Successfully connected to JarServer...")

    val out = socket.getOutputStream
    println("Sending Jar file...")
    sendAnonfun(out)

    val in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()))

    //server will generate an unique integer for each jar file to avoid duplicating jar file
    //after well receiving, server sends back to client an integer, client uses this integer
    //to serve sending dag
    println("Receiving ID...")
    id = in.readInt()

    println("Closing connection...")
    socket.close()

    id
  }
}

class SparkSubmitter (x: RDD[_], query: String, y: SparkContext, src: CodeSource, id: Integer) extends Thread{

  private var df : String = ""

  //send dependencies of each rdd to server
  def sendDependency(dep : Dependency[_], out: ObjectOutputStream): Unit = {
    var length : Int  = dep.rdd.getDP.length

    if(dep.rdd.toString().contains("ToDataFrameHolder"))
      df = df + dep.rdd.toString()

    if (dep.rdd.isInstanceOf[ShuffledRDD[_,_,_]]) {
      val prevRDD : RDD[_] = dep.rdd.asInstanceOf[ShuffledRDD[_,_,_]].getPrev()
      println(dep.rdd)
      var deps: Seq[Dependency[_]] = Nil
      deps = deps :+ dep
      out.writeObject(deps)
      out.writeObject(dep.rdd.dependencies)
      sendDAGPartially(prevRDD, prevRDD.getDP, out)
    }

    if(length == 0 && !dep.rdd.isInstanceOf[ShuffledRDD[_,_,_]]) {
      var last: Seq[Dependency[_]] = Nil
      last = last :+ dep
      out.writeObject(last)
      //out.writeUTF(dep.rdd.name)
    }

    while(length != 0) {
      println(dep.rdd.toString())
      var deps: Seq[Dependency[_]] = Nil
      deps = deps :+ dep
      out.writeObject(deps)
      length = length - 1
      for (i <- 0 to dep.rdd.getDP.length - 1)
        sendDependency(dep.rdd.getDP{i}, out)
    }
  }

  //Send DAGPartially by send rdd and its dependencies to server
  def sendDAGPartially(rdd: RDD[_], deps: Seq[Dependency[_]], out: ObjectOutputStream): Unit = {
    var dep : Dependency[_] = null
    if(rdd.toString().contains("ToDataFrameHolder"))
      df = df + rdd.toString()
    out.writeObject(rdd)
    for(dep <- deps)
      sendDependency(dep, out)
//    out.writeObject(rdd)
  }

  override def run() {
    try {
      println("Connecting to SparkServer...")
      val ia = InetAddress.getByName("localhost")
      val socket = new Socket(ia, 9991)
      println("Successfully connected to SparkServer...")

      val out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream()))

      //send integer number which indicates the jar's filename at server side
      out.writeInt(id)

      //send DAG partially to server
      sendDAGPartially(x, x.dependencies, out)

      //send dataframe creation information and the sql query
      out.writeObject(df+"-"+query)
      println("Successfully sent to SparkServer...")
      println("Closing connection...")
      out.flush()
      out.close()
      socket.close()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
