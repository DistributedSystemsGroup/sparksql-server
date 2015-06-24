package fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule

/**
 * Created by hoang on 6/18/15.
 */
class InputTagging extends RewriteRule {

  private val rewriteRuleName : String = "INPUTTAGGING_SCAN"

  override def execute(opt : OptimizedBag): RewrittenBag = {
    null
  }

}
