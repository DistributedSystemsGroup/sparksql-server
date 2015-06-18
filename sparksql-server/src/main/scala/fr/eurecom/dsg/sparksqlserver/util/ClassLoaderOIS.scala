package fr.eurecom.dsg.sparksqlserver.util

import java.io.{ObjectInputStream, BufferedInputStream}

/**
 * Created by hoang on 6/1/15.
 */
class ClassLoaderOIS(cl : ClassLoader, in: BufferedInputStream) extends ObjectInputStream(in) {
  override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
    try { Class.forName(desc.getName, false, cl) }
    catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
  }
}
