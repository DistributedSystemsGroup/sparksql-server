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

package org.apache.spark.sqlserver.submitter

import java.io._
import java.net.{InetAddress, Socket}
import java.security.CodeSource

class JarSubmitter(path: String, ip: String, port : Int) {
  def sendAnonfun(out: OutputStream): Unit = {

    //get jar location
    //val path: String = src.getLocation.getFile

    val dis: DataInputStream = new DataInputStream(new FileInputStream(new File(path)))

    //begin sending jar file
    var read: Integer = 0;
    var bytes: Array[Byte] = new Array[Byte](1024)

    while ( {
      read = dis.read(bytes); read != -1
    }) {
      out.write(bytes, 0, read)
    }
    out.flush
    dis.close()
  }

  def send(): Integer = {
    var id: Integer = 0
    println("Connecting to JarServer...")
    val ia = InetAddress.getByName(ip)
    val socket = new Socket(ia, port)
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
