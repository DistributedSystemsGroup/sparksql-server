package fr.eurecom.dsg.sparksqlserver.rewriter

import fr.eurecom.dsg.sparksqlserver.container.{RewrittenBag, OptimizedBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan.Caching

/**
 * Created by hoang on 6/1/15.
 */
class RewriteExecutor() {

  def rewrite(optimizedBag : Array[OptimizedBag]): Array[RewrittenBag] = {
    var res : Array[RewrittenBag] = Array.empty[RewrittenBag]
    for (i <-0 to optimizedBag.length - 1)
      if(optimizedBag{i}.getSharingType() == "SCAN")
        if(optimizedBag{i}.isSharable()) {
          if(optimizedBag{i}.getRule().isInstanceOf[Caching])
            res = res :+ optimizedBag{i}.getRule.execute()
        }

    res
  }

}
