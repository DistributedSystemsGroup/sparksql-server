package fr.eurecom.dsg.sparksqlserver.container

import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/18/15.
 * Output of Optimizer. An no-sharing or sharing AnalysedBag after going through the optimizer
 * is also turned into an OptimizedBag.
 */
class OptimizedBag(listDag : Array[DAGContainer], sharingType : String, canShare : Boolean, r : RewriteRule) {

  //label to indicate sharing type
  private val label : String = sharingType

  private val rule : RewriteRule = r

  //ArrayBuffer of Array of DAGContainer. The ArrayBuffer contains two types of Array[DAGContainer]
  //sharing and no-sharing. An Array[DAGContainer] which has size equals to 1 is known as no-sharing bag, and vice-versa.
  private val lstDag : Array[DAGContainer] = listDag

  def getSharingType() : String = label

  def getListDag() : Array[DAGContainer] = lstDag

  def getRule() : RewriteRule = rule

  private val sharable : Boolean = canShare

  def isSharable() : Boolean = sharable



}
