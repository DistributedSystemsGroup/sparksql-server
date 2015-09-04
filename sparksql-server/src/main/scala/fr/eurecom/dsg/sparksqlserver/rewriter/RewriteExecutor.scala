/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.eurecom.dsg.sparksqlserver.rewriter

import fr.eurecom.dsg.sparksqlserver.container.{RewrittenBag, OptimizedBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan.Caching
import org.apache.spark.SparkContext

/**
 * Created by hoang on 6/1/15.
 * Rewrite Executor will iterate the optimizedBag,
 * if a bag is sharable, it will get the attached rule
 * of the bag and execute (rewrite) the bag.
 */
class RewriteExecutor() {

  def rewrite(sc : SparkContext, optimizedBag : Array[OptimizedBag]): Array[RewrittenBag] = {
    var res : Array[RewrittenBag] = Array.empty[RewrittenBag]
    for (i <-0 to optimizedBag.length - 1)
      if(optimizedBag{i}.isSharable())
        res = res :+ optimizedBag{i}.getRule().execute(sc, optimizedBag{i})

    res
  }

}
