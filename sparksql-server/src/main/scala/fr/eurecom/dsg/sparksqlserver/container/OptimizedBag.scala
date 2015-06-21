package fr.eurecom.dsg.sparksqlserver.container

/**
 * Created by hoang on 6/18/15.
 * Output of Optimizer. An no-sharing or sharing AnalysedBag after going through the optimizer
 * is also turned into an OptimizedBag.
 */
class OptimizedBag(canShare : Boolean) {

  private val sharable : Boolean = canShare

  def isSharable() : Boolean = sharable

}
