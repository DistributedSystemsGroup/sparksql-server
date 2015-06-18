package fr.eurecom.dsg.sparksqlserver.listener

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * Created by hoang on 6/2/15.
 */
class ClientPackage (sqlC : SQLContext) {
  //DAG created before DataFrame creation
  var initialDAG : RDD[_]= null
  //SQL Query
  var query : String = ""
  //DAG created before an action call
  var finalDAG : RDD[_] = null

  def ClientPackage(initDAG : RDD[_], sql: String, finDAG : RDD[_]): Unit = {
    initialDAG = initDAG
    query = sql
    finalDAG = finalDAG
  }

  def getInitialDAG : RDD[_] = initialDAG

  def getQuery : String = query

  def getFinalDAG : RDD[_] = finalDAG

  //fullDAG which is connected by initialDAG, sparkPlan generated from the query and finalDAG
  /*def getFullDAG : RDD[_] = {
    sqlC.sql(query)
  }*/
}
