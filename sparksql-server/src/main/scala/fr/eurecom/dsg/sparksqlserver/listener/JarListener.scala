/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.eurecom.dsg.sparksqlserver.listener

import java.io._
import java.net.{ServerSocket, Socket}
import java.util.concurrent.atomic.AtomicInteger

import fr.eurecom.dsg.sparksqlserver.util.ServerConstants
import org.apache.spark.Logging

/**
 * Created by hoang on 6/1/15.
 * Listener thread to receive Jar file from client
 */
class JarListener() extends Thread with Logging {
  private val listener = new ServerSocket(ServerConstants.JAR_LISTENER_PORT)
  private var counter : AtomicInteger = new AtomicInteger

  override def run(): Unit = {
    while (true) {
      new JarReceiver(listener.accept(), counter.getAndDecrement).start()
    }
  }
}

/**
 * Thread for listening
 */
class JarListenerThread extends Thread with Logging{
  private val JarListener : JarListener = new JarListener()
  override def run(): Unit = {
    JarListener.run()
  }
}

/**
 * receive the jar file
 * save it to tmp folder with an name as format: id.jar
 * id is an atomic incremental integer
 * @param socket
 * @param id
 */
case class JarReceiver(socket: Socket, id: Integer) extends Thread("JarListener") with Logging {

  def receiveAnonfun(is : InputStream) {
    val path : String = "/tmp/" + id.toString + ".jar"
    //println(path)
    val outputFile : OutputStream = new FileOutputStream(new File(path))

    var read: Integer = 0;
    var bytes: Array[Byte] = new Array[Byte](1024)

    while({read = is.read(bytes); read != -1}){
      outputFile.write(bytes, 0, read)
    }
    outputFile.close()
  }

  override def run: Unit = {

    log.info("Receiving jar file from client...")

    val out = new ObjectOutputStream(new BufferedOutputStream(socket.getOutputStream))
    out.writeInt(id)

    out.flush()

    receiveAnonfun(socket.getInputStream)

    log.info("Saved jar file from client at /tmp/" + id + ".jar")
  }
}