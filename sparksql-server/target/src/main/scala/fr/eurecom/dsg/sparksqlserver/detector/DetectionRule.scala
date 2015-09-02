package fr.eurecom.dsg.sparksqlserver.detector

import fr.eurecom.dsg.sparksqlserver.container.DAGContainer
import fr.eurecom.dsg.sparksqlserver.listener.DAGQueue

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/1/15.
 */
abstract class DetectionRule (){

  private var name : String = ""

  private var dag : DAGQueue = null

  def Rule(nm : String, dg : DAGQueue): Unit = {
    name = nm
    dag = dg
  }

  def initiate() : Unit = {

  }

  def analyse() : ArrayBuffer[Array[DAGContainer]] = {
    null
  }

}
