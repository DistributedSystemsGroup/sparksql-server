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

package fr.eurecom.dsg.sparksqlserver.rewriter.rules.scan

import fr.eurecom.dsg.sparksqlserver.container.{DAGContainer, OptimizedBag, RewrittenBag}
import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule
import org.apache.spark.{SparkContext, Dependency}
import org.apache.spark.rdd.{ShuffledRDD, RDD}

/**
 * Created by hoang on 6/18/15.
 * Caching rewrite rule. The idea is caching the common RDD from the first DAG, then replace
 * the common RDD from the rest RDDs which the cached.
 */
class Caching extends RewriteRule{

  private val rewriteRuleName : String = "CACHING_SCAN"

  /**
   * get the RDD which its name contains "Scan".
   * This is a HadoopRDD
   * @param dep
   * @return
   */
  def getScanRDD(dep : Seq[Dependency[_]]): RDD[_] = {
    if(dep{0}.rdd.toString.contains("Scan"))
      dep{0}.rdd
    else
      getScanRDD(dep{0}.rdd.getDP)
  }

  /**
   * When we read from textFile, a HadoopRDD will be followed by an
   * MapPartitionsRDD, we need to find it.
   * @param dep
   * @param scan
   * @return
   */
  def getPostScanRDD(dep : Seq[Dependency[_]], scan : RDD[_]): RDD[_] = {
    if(dep{0}.rdd.getDP{0}.rdd == scan)
      dep{0}.rdd
    else
      getPostScanRDD(dep{0}.rdd.getDP, scan)
  }

  /**
   * We replaced the (HadoopRDD,MapPartitionsRDD) pair
   * of each RDD which the cached pair.
   * @param dag
   * @param pst
   */
  def replaceScan(dag : RDD[_], pst : RDD[_]): Unit = {

    var scan : RDD[_] = null
    var post : RDD[_] = null

    if(dag.isInstanceOf[ShuffledRDD[_,_,_]]) {
      scan = getScanRDD(dag.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP)
      post = getPostScanRDD(dag.asInstanceOf[ShuffledRDD[_,_,_]].getPrev().getDP, scan)
    }
    else {
      scan = getScanRDD(dag.getDP)
      post = getPostScanRDD(dag.getDP, scan)
    }

    post.setDeps(pst.getDP)

  }

  override def execute(sc: SparkContext, opt : OptimizedBag): RewrittenBag = {
    val lstDag : Array[DAGContainer] = opt.getListDag()
    val firstDag : RDD[_] = lstDag{0}.getDAG()
    var scan : RDD[_] = null
    var post : RDD[_] = null
    if(firstDag.isInstanceOf[ShuffledRDD[_,_,_]]) {
      scan = getScanRDD(firstDag.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP)
      post = getPostScanRDD(firstDag.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP, scan)
    }
    else {
      scan = getScanRDD(firstDag.getDP)
      post = getPostScanRDD(firstDag.getDP, scan)
    }

    //cache it!
    scan.cache()

    //replace this cached scanRDD of this firstDAG to the rest DAGs in the Bag
    for (i <- 1 to lstDag.length - 1) {
      replaceScan(lstDag{i}.getDAG(), post)
    }
    //turn it into a rewrittenBag and return it
    new RewrittenBag(lstDag, rewriteRuleName)
  }

}
