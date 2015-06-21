package fr.eurecom.dsg.sparksqlserver.container

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/18/15.
 * A bag which contains a list of grouped-DAGs depend on their sharing opportunities.
 * AnalysedBags are the output of the WSDetector
 * We can have many types of sharing so each bag is attached with a label to indicate its sharing type.
 */
class AnalysedBag(sharingType : String, listDag : ArrayBuffer[Array[DAGContainer]]) {

  //label to indicate sharing type
  private val label : String = sharingType

  //ArrayBuffer of Array of DAGContainer. The ArrayBuffer contains two types of Array[DAGContainer]
  //sharing and no-sharing. An Array[DAGContainer] which has size equals to 1 is known as no-sharing bag, and vice-versa.
  private val lstDag : ArrayBuffer[Array[DAGContainer]] = listDag

  def getSharingType() : String = label

  def getListDag() : ArrayBuffer[Array[DAGContainer]] = lstDag

  //after an no-sharing bag went through the optimizer
  def toOptimizedBag() : OptimizedBag = {
    new OptimizedBag(false)
  }

}
