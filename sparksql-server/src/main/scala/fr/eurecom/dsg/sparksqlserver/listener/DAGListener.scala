package fr.eurecom.dsg.sparksqlserver.listener

import java.io._
import java.net.{ServerSocket, Socket, URL}
import java.util.concurrent.atomic.AtomicInteger
import fr.eurecom.dsg.sparksqlserver.container.{DAGPiggyback, DAGContainer}
import fr.eurecom.dsg.sparksqlserver.util.ClassLoaderOIS
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{ShuffledRDD, MapPartitionsRDD, RDD}
import org.apache.spark._
import org.apache.spark.sql._


import scala.tools.nsc.util.ScalaClassLoader.URLClassLoader

/**
 * Created by hoang on 6/1/15.
 */
class DAGListener(sc: SparkContext, sqlC: SQLContext, jarPath: URL, dagQueue: DAGQueue) extends Thread {

  private val listener = new ServerSocket(9991)

  private var counter : AtomicInteger = new AtomicInteger//need a lock here

  def DAGListener() {
    println("Server is created and listening on port 9999")
  }

  override def run(): Unit = {
    try {
      while (true){
        new DAGListenerThread(listener.accept(), sc, sqlC, jarPath, counter.getAndIncrement, dagQueue).start();
      }
      //listener.close()
    } catch {
      case e: IOException =>
        System.err.println("Could not listen on port: 9999.");
        ()
    }
  }
}

case class DAGListenerThread(socket: Socket, sc: SparkContext, sqlC: SQLContext, jarPath: URL, counter: Integer, dagQueue: DAGQueue) extends Thread("DAGListenerThread") {

  def receiveAnonfun(ois : ObjectInputStream): String = {
    val path : String = "/tmp/" + counter.toString + ".jar"
    println(path)
    val outputFile : OutputStream = new FileOutputStream(new File(path))

    var stop : Boolean = true
    while (stop) {
      var l = ois.readObject()
      var length = 0
      if (l.isInstanceOf[Integer])
        length = l.asInstanceOf[Integer]
      else if (l.isInstanceOf[Boolean])
        stop = !stop
      var bytes = ois.readObject()
      if (bytes.isInstanceOf[Array[Byte]]) {
        val bytess: Array[Byte] = bytes.asInstanceOf[Array[Byte]]
        outputFile.write(bytess, 0, length)
      }
    }
    outputFile.close()
    println(path)
    path
  }

  /**
   * Receive each RDD, its dependency and reassemble them into a DAG
   * @param in
   * @return
   */
  def reassembleDAG(in : ClassLoaderOIS, sqlC : SQLContext, classLoader : URLClassLoader): RDD[_] = {
    var lastDep : Boolean = true
    var rdd : RDD[_] = null
    var queue : Seq[Object] = Nil
    val nullDep : Seq[Dependency[_]] = Nil
    var splitArr : Seq[String] = Nil
    var dfIndex = 0
    while(lastDep) {
      val deps = in.readObject()
      if (deps.isInstanceOf[String]) {
        splitArr = deps.asInstanceOf[String].split("_")
        lastDep = false
        queue{queue.length - 1}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setSparkContext(sc)
        queue{queue.length - 1}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setDeps(nullDep)
        queue{queue.length - 1}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setCreationSite()
        queue{queue.length - 1}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setName(splitArr{2})
        for (i <- (0 to queue.length - 2).reverse) {
          if(queue{i}.isInstanceOf[Seq[Dependency[_]]]) {
            if(queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.isInstanceOf[OneToOneDependency[_]]){
              queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setSparkContext(sc)
              if(splitArr{0}.contains(queue {i}.asInstanceOf[Seq[Dependency[_]]] {0}.rdd.toString))
                dfIndex = i
              queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setName("initial")
              queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setCreationSite()
              if(!queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.isInstanceOf[ShuffledRDD[_,_,_]]) {
                queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setDeps(queue{i+1}.asInstanceOf[Seq[Dependency[_]]])
              } else {
                queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.asInstanceOf[ShuffledRDD[_,_,_]].setPrev(queue {i + 2}.asInstanceOf[RDD[_]])
                queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.asInstanceOf[ShuffledRDD[_,_,_]].setDeps(queue{i+1}.asInstanceOf[Seq[Dependency[_]]])
                queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.asInstanceOf[ShuffledRDD[_,_,_]].setName("Shuffle")
                queue{i}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.asInstanceOf[ShuffledRDD[_,_,_]].setCreationSite()
              }
            }
          } else {
            if (queue{i}.isInstanceOf[RDD[_]]) {
              queue{i}.asInstanceOf[RDD[_]].setSparkContext(sc)
              if(splitArr{0}.contains(queue {i}.asInstanceOf[RDD[_]].toString))
                dfIndex = i
              queue{i}.asInstanceOf[RDD[_]].setDeps(queue{i+1}.asInstanceOf[Seq[Dependency[_]]])
              queue{i}.asInstanceOf[RDD[_]].setName("final")
              queue{i}.asInstanceOf[RDD[_]].setCreationSite()
            }
          }
        }
        queue{queue.length - 2}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.setName("Scan")
        rdd = queue{0}.asInstanceOf[RDD[_]]
      } else {
        queue = queue :+ deps
      }
    }

    sqlC.createDataFrame(queue{dfIndex}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd, queue{dfIndex+1}.asInstanceOf[Seq[Dependency[_]]]{0}.rdd.elementClassTag.runtimeClass).toDF().registerTempTable(splitArr{1})

    println(rdd.toDebugString)

    rdd
  }

  override def run(): Unit = {
    println("Client connected successfully!")
    try {
      val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(sc.getConf)
      val confBroadcast = sc.broadcast(new SerializableWritable(hadoopConfiguration))

      val classLoader : URLClassLoader = new URLClassLoader(Array(jarPath), ClassLoader.getSystemClassLoader)
      println("Client connected successfully!")
      val in = new ClassLoaderOIS(classLoader, new BufferedInputStream(socket.getInputStream))

      val id : Integer = in.readInt()
      println("clientID: " + id)
      val path : String = "/tmp/" + id.toString + ".jar"
      println(path)

      classLoader.addURL(new File(path).toURI.toURL)
      println("ClassLoader Addeded")

      println("Received RDD from client")

      sc.addJar(path)

      val clientRDD = reassembleDAG(in, sqlC, classLoader)//.saveAsTextFile("/home/hoang/" + id.toString)

      //receive descriptor for DAGPiggyback (costmodel information, scheduling information ... which are passed through submit command)

      val dagCtn : DAGContainer = new DAGContainer()

      dagCtn.setDAG(clientRDD)

      dagCtn.setPiggyback(new DAGPiggyback)

      this.synchronized {
        dagQueue.queue.enqueue(dagCtn)
      }
      println("Received well!")
      in.close();
      socket.close()
    }
    catch {
      case e: Exception => e.printStackTrace()
        println(e)
    }
  }

}