package fr.eurecom.dsg.sparksqlserver.container

/**
 * Created by hoang on 6/18/15.
 */
class RewrittenBag(listDag : Array[DAGContainer], rl : String) {
  private val rewriteRuleName : String = rl

  private val lstDag : Array[DAGContainer] = listDag

  def getRewriteRuleName : String = rewriteRuleName

  def getListDag() : Array[DAGContainer] = lstDag

}
