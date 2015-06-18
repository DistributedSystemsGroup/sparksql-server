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

package org.apache.spark.graphx.lib

import org.apache.spark.annotation.Experimental

import scala.util.Random
import org.jblas.DoubleMatrix
import org.apache.spark.rdd._
import org.apache.spark.graphx._

/** Implementation of SVD++ algorithm. */
object SVDPlusPlus {

  /** Configuration parameters for SVDPlusPlus. */
  class Conf(
      var rank: Int,
      var maxIters: Int,
      var minVal: Double,
      var maxVal: Double,
      var gamma1: Double,
      var gamma2: Double,
      var gamma6: Double,
      var gamma7: Double)
    extends Serializable

  /**
   * :: Experimental ::
   *
   * Implement SVD++ based on "Factorization Meets the Neighborhood:
   * a Multifaceted Collaborative Filtering Model",
   * available at [[http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf]].
   *
   * The prediction rule is rui = u + bu + bi + qi*(pu + |N(u)|^(-0.5)*sum(y)),
   * see the details on page 6.
   *
   * This method temporarily replaces `run()`, and replaces `DoubleMatrix` in `run()`'s return
   * value with `Array[Double]`. In 1.4.0, this method will be deprecated, but will be copied
   * to replace `run()`, which will then be undeprecated.
   *
   * @param edges edges for constructing the graph
   *
   * @param conf SVDPlusPlus parameters
   *
   * @return a graph with vertex attributes containing the trained model
   */
  @Experimental
  def runSVDPlusPlus(edges: RDD[Edge[Double]], conf: Conf)
    : (Graph[(Array[Double], Array[Double], Double, Double), Double], Double) =
  {
    val (graph, u) = run(edges, conf)
    // Convert DoubleMatrix to Array[Double]:
    val newVertices = graph.vertices.mapValues(v => (v._1.toArray, v._2.toArray, v._3, v._4))
    (Graph(newVertices, graph.edges), u)
  }

  /**
   * This method is deprecated in favor of `runSVDPlusPlus()`, which replaces `DoubleMatrix`
   * with `Array[Double]` in its return value. This method is deprecated. It will effectively
   * be removed in 1.4.0 when `runSVDPlusPlus()` is copied to replace `run()`, and hence the
   * return type of this method changes.
   */
  @deprecated("Call runSVDPlusPlus", "1.3.0")
  def run(edges: RDD[Edge[Double]], conf: Conf)
    : (Graph[(DoubleMatrix, DoubleMatrix, Double, Double), Double], Double) =
  {
    // Generate default vertex attribute
    def defaultF(rank: Int): (DoubleMatrix, DoubleMatrix, Double, Double) = {
      val v1 = new DoubleMatrix(rank)
      val v2 = new DoubleMatrix(rank)
      for (i <- 0 until rank) {
        v1.put(i, Random.nextDouble())
        v2.put(i, Random.nextDouble())
      }
      (v1, v2, 0.0, 0.0)
    }

    // calculate global rating mean
    edges.cache()
    val (rs, rc) = edges.map(e => (e.attr, 1L)).reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc

    // construct graph
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()
    materialize(g)
    edges.unpersist()

    // Calculate initial bias and norm
    val t0 = g.aggregateMessages[(Long, Double)](
      ctx => { ctx.sendToSrc((1L, ctx.attr)); ctx.sendToDst((1L, ctx.attr)) },
      (g1, g2) => (g1._1 + g2._1, g1._2 + g2._2))

    val gJoinT0 = g.outerJoinVertices(t0) {
      (vid: VertexId, vd: (DoubleMatrix, DoubleMatrix, Double, Double),
       msg: Option[(Long, Double)]) =>
        (vd._1, vd._2, msg.get._2 / msg.get._1, 1.0 / scala.math.sqrt(msg.get._1))
    }.cache()
    materialize(gJoinT0)
    g.unpersist()
    g = gJoinT0

    def sendMsgTrainF(conf: Conf, u: Double)
        (ctx: EdgeContext[
          (DoubleMatrix, DoubleMatrix, Double, Double),
          Double,
          (DoubleMatrix, DoubleMatrix, Double)]) {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      var pred = u + usr._3 + itm._3 + q.dot(usr._2)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = ctx.attr - pred
      val updateP = q.mul(err)
        .subColumnVector(p.mul(conf.gamma7))
        .mul(conf.gamma2)
      val updateQ = usr._2.mul(err)
        .subColumnVector(q.mul(conf.gamma7))
        .mul(conf.gamma2)
      val updateY = q.mul(err * usr._4)
        .subColumnVector(itm._2.mul(conf.gamma7))
        .mul(conf.gamma2)
      ctx.sendToSrc((updateP, updateY, (err - conf.gamma6 * usr._3) * conf.gamma1))
      ctx.sendToDst((updateQ, updateY, (err - conf.gamma6 * itm._3) * conf.gamma1))
    }

    for (i <- 0 until conf.maxIters) {
      // Phase 1, calculate pu + |N(u)|^(-0.5)*sum(y) for user nodes
      g.cache()
      val t1 = g.aggregateMessages[DoubleMatrix](
        ctx => ctx.sendToSrc(ctx.dstAttr._2),
        (g1, g2) => g1.addColumnVector(g2))
      val gJoinT1 = g.outerJoinVertices(t1) {
        (vid: VertexId, vd: (DoubleMatrix, DoubleMatrix, Double, Double),
         msg: Option[DoubleMatrix]) =>
          if (msg.isDefined) (vd._1, vd._1
            .addColumnVector(msg.get.mul(vd._4)), vd._3, vd._4) else vd
      }.cache()
      materialize(gJoinT1)
      g.unpersist()
      g = gJoinT1

      // Phase 2, update p for user nodes and q, y for item nodes
      g.cache()
      val t2 = g.aggregateMessages(
        sendMsgTrainF(conf, u),
        (g1: (DoubleMatrix, DoubleMatrix, Double), g2: (DoubleMatrix, DoubleMatrix, Double)) =>
          (g1._1.addColumnVector(g2._1), g1._2.addColumnVector(g2._2), g1._3 + g2._3))
      val gJoinT2 = g.outerJoinVertices(t2) {
        (vid: VertexId,
         vd: (DoubleMatrix, DoubleMatrix, Double, Double),
         msg: Option[(DoubleMatrix, DoubleMatrix, Double)]) =>
          (vd._1.addColumnVector(msg.get._1), vd._2.addColumnVector(msg.get._2),
            vd._3 + msg.get._3, vd._4)
      }.cache()
      materialize(gJoinT2)
      g.unpersist()
      g = gJoinT2
    }

    // calculate error on training set
    def sendMsgTestF(conf: Conf, u: Double)
        (ctx: EdgeContext[(DoubleMatrix, DoubleMatrix, Double, Double), Double, Double]) {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      var pred = u + usr._3 + itm._3 + q.dot(usr._2)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = (ctx.attr - pred) * (ctx.attr - pred)
      ctx.sendToDst(err)
    }

    g.cache()
    val t3 = g.aggregateMessages[Double](sendMsgTestF(conf, u), _ + _)
    val gJoinT3 = g.outerJoinVertices(t3) {
      (vid: VertexId, vd: (DoubleMatrix, DoubleMatrix, Double, Double), msg: Option[Double]) =>
        if (msg.isDefined) (vd._1, vd._2, vd._3, msg.get) else vd
    }.cache()
    materialize(gJoinT3)
    g.unpersist()
    g = gJoinT3

    (g, u)
  }

  /**
   * Forces materialization of a Graph by count()ing its RDDs.
   */
  private def materialize(g: Graph[_,_]): Unit = {
    g.vertices.count()
    g.edges.count()
  }

}
