package fr.eurecom.dsg.sparksqlserver.container

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Created by hoang on 6/17/15.
 * A container to contain each DAG of client and the information it brings
 */
class DAGContainer {

  private var dag : RDD[_] = null

  private var info : DAGPiggyback = new DAGPiggyback()

  def setDAG(dg : RDD[_]) : Unit = {
    dag = dg
  }

  def setPiggyback(ppb : DAGPiggyback) : Unit = {
    info = ppb
  }

  def getDAG(): RDD[_] = dag

  def getPiggyback() : DAGPiggyback = info

  def updatePiggyback(key : String, vl : Any) : Unit = {
    info.addToDescTable(key, vl)
  }
}

/**
 * Information which is piggybacked within a DAGContainer
 * Example: scheduling information, cost-model information...
 */
class DAGPiggyback {

  private var descriptor : mutable.HashMap[String, Any] = null

  def getDescriptor : mutable.HashMap[String, Any] = descriptor

  def addToDescTable(key : String, vl : Any) : Unit = {
    descriptor.put(key, vl)
  }

}
