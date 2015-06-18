package fr.eurecom.dsg.sparksqlserver.listener

import java.io._
import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Logging

/**
 * Created by hoang on 6/1/15.
 */
class JarListener() extends Thread with Logging {
  private val listener = new ServerSocket(9992)
  private var counter : AtomicInteger = new AtomicInteger

  override def run(): Unit = {
    while (true) {
      new JarReceiver(listener.accept(), counter.getAndDecrement).start()
    }
  }
}

class JarListenerThread extends Thread with Logging{
  private val JarListener : JarListener = new JarListener()
  logInfo("JarListener is running...")
  override def run(): Unit = {
    JarListener.run()
  }
}

case class JarReceiver(socket: Socket, id: Integer) extends Thread("JarListener") {

  def receiveAnonfun(is : InputStream) {
    val path : String = "/tmp/" + id.toString + ".jar"
    println(path)
    val outputFile : OutputStream = new FileOutputStream(new File(path))

    var read: Integer = 0;
    var bytes: Array[Byte] = new Array[Byte](1024)

    while({read = is.read(bytes); read != -1}){
      outputFile.write(bytes, 0, read)
    }
    outputFile.close()
  }

  override def run: Unit = {

    val out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream))
    out.writeInt(id)

    out.flush()

    receiveAnonfun(socket.getInputStream)
  }
}