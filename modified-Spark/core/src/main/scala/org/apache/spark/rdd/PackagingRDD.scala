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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._

private[spark] abstract class PackagingBaseRDD(
                                                sc: SparkContext,
                                                var rdds: Seq[RDD[_]],
                                                preservesPartitioning: Boolean = false)
  extends RDD[Tuple2[_, _]](sc, rdds.map(x => new OneToOneDependency(x))) {

  override val partitioner =
    if (preservesPartitioning) {
      firstParent[Any].partitioner
    } else {
      None
    }

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.size
    if (!rdds.forall(rdd => rdd.partitions.size == numParts)) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    Array.tabulate[Partition](numParts) { i =>
      val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i)))
      // Check whether there are any hosts that match all RDDs; otherwise return the union
      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
      val locs = if (!exactMatchLocations.isEmpty) {
        exactMatchLocations
      } else {
        prefs.flatten.distinct
      }
      new ZippedPartitionsPartition(i, rdds, locs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

private[spark] class LabellingRDD(
                                   sc: SparkContext,
                                   rdds: Array[RDD[_]],
                                   preservesPartitioning: Boolean = false) extends
PackagingBaseRDD(sc, rdds, preservesPartitioning) {

  var noJob = rdds.size

  var x: Array[Iterator[_]] = new Array[Iterator[_]](noJob)

  var hasShuffled: Array[Boolean] = Array[Boolean]()

  def setHasShuffled(arr: Array[Boolean]): Unit = {
    hasShuffled = arr
  }

  def atLeastOneShuffle(): Boolean = {
    var res: Boolean = false
    for (i <- 0 to rdds.size - 1)
      res = res || hasShuffled {
        i
      }
    res
  }

  var turn = 0

  var newtemp: Tuple2[_, _] = null

  var noWriter: Int = 0

  var initWriter: Array[Boolean] = new Array[Boolean](noWriter)

  var writerArr: Array[SparkHadoopWriter] = null

  var temp1: Tuple2[_, _] = null
  var temp: Tuple2[_, _] = null

  var currentTurn = 0

  def setSparkWriterArray(w: Array[SparkHadoopWriter]): Unit = {
    writerArr = w
  }

  def setNoWriter(nw: Int): Unit = {
    noWriter = nw
    initWriter = new Array[Boolean](noWriter)
  }

  override def compute(s: Partition, context: TaskContext): Iterator[Tuple2[_, _]] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions

    if (turn == noJob) {
      turn = 0
    }

    if (x {turn} == null) {
      x {turn} = rdds {turn}.iterator(partitions(turn), context)
    }

    if (!x {turn}.hasNext) {
      rdds {turn}.iterator(partitions(0), context).asInstanceOf[Iterator[Tuple2[_, _]]]
    }
    else {
      new Iterator[Tuple2[_, _]] {
        override def next: Tuple2[_, _] = {

          if (turn == noJob) {
            turn = 0
          }

          if (x {turn} == null) {
            x {turn} = rdds {turn}.iterator(partitions(turn), context)
          }

          if (!x {turn}.hasNext) {
            turn = turn + 1
          }

          if (turn == noJob) {
            turn = 0
          }

          temp1 = x {turn}.next.asInstanceOf[Tuple2[_, _]]

          currentTurn = turn
          turn = turn + 1
          ((currentTurn, temp1._1), temp1._2)
          //temp1.copy(_1 = (currentTurn.toString, temp1._1))
        }

        def checkHasNext(): Boolean = {
          var res = false
          for (i <- 0 to x.size - 1)
            res = res || x {
              i
            }.hasNext
          res
        }

        def allNotNull(): Boolean = {
          var res = true
          for (i <- 0 to noJob - 1)
            if (x {
              i
            } == null) {
              res = false
            }
          res
        }

        override def hasNext: Boolean = {
          if (allNotNull) {
            var res = false
            for (i <- 0 to x.size - 1) {
              res = res || x {i}.hasNext
            }
            res
          } else {
            true
          }
        }
      }
    }
  }
}

private[spark] class DeLabellingRDD(
                                     sc: SparkContext,
                                     rdds: Array[RDD[_]],
                                     preservesPartitioning: Boolean = false) extends
PackagingBaseRDD(sc, rdds, preservesPartitioning) {

  var noWriter: Int = 0

  var initWriter: Array[Boolean] = new Array[Boolean](noWriter)

  var writerArr: Array[SparkHadoopWriter] = null

  def setSparkWriterArray(w: Array[SparkHadoopWriter]): Unit = {
    writerArr = w
  }

  def setNoWriter(nw: Int): Unit = {
    noWriter = nw
    initWriter = new Array[Boolean](noWriter)
  }

  var hasShuffled: Array[Boolean] = Array[Boolean]()

  def setHasShuffled(arr: Array[Boolean]): Unit = {
    hasShuffled = arr
  }


  def atLeastOneShuffle(): Boolean = {
    var res: Boolean = false
    for (i <- 0 to rdds.size - 1)
      res = res || hasShuffled {
        i
      }
    res
  }

  var newtemp: Tuple2[_, _] = null

  var noJob = rdds.size

  var x: Array[Iterator[_]] = new Array[Iterator[_]](noJob)

  var turn = 0

  var lastRecord: Tuple2[_, _] = null

  def notAllHasNoShuffledEmpty(): Boolean = {

    var hasNoShuff: Array[Iterator[_]] = Array[Iterator[_]]()
    for (i <- 0 to x.size - 1) {
      if (hasShuffled(i)) {
        hasNoShuff = hasNoShuff :+ x {
          i
        }
      }
    }
    var res = false
    for (i <- 0 to hasNoShuff.size - 1)
      res = res || hasNoShuff {
        i
      }.hasNext
    res
  }

  def notAllEmpty(): Boolean = {
    var res = false
    for (i <- 0 to x.size - 1)
      res = res || x {
        i
      }.hasNext
    res
  }

  def notAllHasShuffledEmpty(): Boolean = {

    var hasShuff: Array[Iterator[_]] = Array[Iterator[_]]()
    for (i <- 0 to x.size - 1) {
      if (hasShuffled(i)) {
        hasShuff = hasShuff :+ x {
          i
        }
      }
    }
    var res = false
    for (i <- 0 to hasShuff.size - 1)
      res = res || hasShuff {
        i
      }.hasNext
    res
  }

  def removeLabel(temp: Tuple2[_, _]): Tuple2[_, _] = {
    var res: Tuple2[_, _] = null
    res = temp.copy(_1 = temp._1.asInstanceOf[Tuple2[_, _]]._2)
    res
  }

  override def compute(s: Partition, context: TaskContext): Iterator[Tuple2[_, _]] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions

    new Iterator[Tuple2[_, _]] {
      override def next: Tuple2[_, _] = {

        if (x {
          turn
        } == null) {
          x {
            turn
          } = rdds {
            turn
          }.iterator(partitions(0), context)
        }

        var temp = x {
          turn
        }.next

        if (getDeppestTuple(temp.asInstanceOf[Tuple2[_, _]])._1 != null) {
          if (!hasShuffled {
            turn
          }) {
            if (initWriter {
              turn
            } == false) {
              initWriter {
                turn
              } = true
              writerArr {
                turn
              }.preSetup()
              val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt
              writerArr {
                turn
              }.setup(context.stageId, context.partitionId, taskAttemptId)
              writerArr {
                turn
              }.open()
            }
            //println("SAVE: " + temp)
            writerArr {
              turn
            }.write(temp.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[AnyRef], temp
              .asInstanceOf[Tuple2[_, _]]._2.asInstanceOf[AnyRef])
            //modify temp to null
            val newtemp: Tuple2[_, _] = temp.asInstanceOf[Tuple2[_, _]].copy(_1 = null)
            temp = newtemp
            turn = turn - 1
          }
        }

        while (getDeppestTuple(temp.asInstanceOf[Tuple2[_, _]])._1 == null && notAllEmpty()) {
          turn = turn + 1
          if (turn == noJob) {
            turn = 0
          }
          if (x {
            turn
          } == null) {
            x {
              turn
            } = rdds {
              turn
            }.iterator(partitions(0), context)
          }
          temp = x {
            turn
          }.next
          if (!hasShuffled {
            turn
          }) {
            if (getDeppestTuple(temp.asInstanceOf[Tuple2[_, _]])._1 != null) {
              //writer.getOut.writeUTF(temp.asInstanceOf[Tuple2[_,_]]._1.toString + "\t" + temp
              // .asInstanceOf[Tuple2[_,_]]._2.toString)
              //println("SAVE: " + temp)
              if (initWriter {
                turn
              } == false) {
                initWriter {
                  turn
                } = true
                writerArr {
                  turn
                }.preSetup()
                val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt
                writerArr {
                  turn
                }.setup(context.stageId, context.partitionId, taskAttemptId)
                writerArr {
                  turn
                }.open()
              }
              writerArr {
                turn
              }.write(temp.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[AnyRef], temp
                .asInstanceOf[Tuple2[_, _]]._2.asInstanceOf[AnyRef])
              val newtemp: Tuple2[_, _] = temp.asInstanceOf[Tuple2[_, _]].copy(_1 = null)
              temp = newtemp
              turn = turn - 1
            }
          }
        }
        //println("OLD: " + temp)
        if (getDeppestTuple(temp.asInstanceOf[Tuple2[_, _]])._1 != null) {
          val currentTurn = turn
          newtemp = temp.asInstanceOf[Tuple2[_, _]]
            .copy(_1 = (currentTurn.toString, temp.asInstanceOf[Tuple2[_, _]]._1))
          //println(newtemp)
          newtemp
        } else {
          //newtemp = temp.asInstanceOf[Tuple2[_,_]].copy(_1 = null)
          //println(newtemp)
          //newtemp
          //writer.getOut.close()
          for (i <- 0 to writerArr.size - 1)
            if (initWriter {
              i
            } == true) {
              writerArr {
                i
              }.close()
              writerArr {
                i
              }.commit()
              writerArr {
                i
              }.commitJob()
            }
          temp.asInstanceOf[Tuple2[_, _]]
        }
      }

      def getDeppestTuple(temp: Tuple2[_, _]): Tuple2[_, _] = {
        var res: Tuple2[_, _] = null
        if (temp._1.isInstanceOf[Tuple2[_, _]]) {
          res = getDeppestTuple(temp._1.asInstanceOf[Tuple2[_, _]])
        }
        else {
          res = temp
        }
        res
      }

      def allNotNull(): Boolean = {
        var res = true
        for (i <- 0 to noJob - 1)
          if (x {
            i
          } == null) {
            res = false
          }
        res
      }

      override def hasNext: Boolean = {
        if (allNotNull) {
          var res = false
          for (i <- 0 to x.size - 1) {
            res = res || x {
              i
            }.hasNext
          }
          res
        } else {
          true
        }
      }
    }
  }
}

private[spark] class PackagingRDD2[A: ClassTag, B: ClassTag](
                                                              sc: SparkContext,
                                                              var f: (Iterator[A], Iterator[B])
                                                                => Iterator[Tuple2[_, _]],
                                                              var rdd1: RDD[A],
                                                              var rdd2: RDD[B],
                                                              preservesPartitioning: Boolean =
                                                              false)
  extends PackagingBaseRDD(sc, List(rdd1, rdd2), preservesPartitioning) {

  var turn: Boolean = true

  var x: Iterator[A] = null

  var y: Iterator[B] = null

  var newtemp: Tuple2[_, _] = null

  override def compute(s: Partition, context: TaskContext): Iterator[Tuple2[_, _]] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions

    new Iterator[Tuple2[_, _]] {
      override def next: Tuple2[_, _] = {
        if (turn == true) {
          turn = false
          if (x == null) {
            x = rdd1.iterator(partitions(0), context)
          }
          val temp = x.next.asInstanceOf[Tuple2[_, _]]
          newtemp = temp.copy(_1 = ("1", temp._1))
        } else {
          turn = true
          if (y == null) {
            y = rdd2.iterator(partitions(1), context)
          }
          val temp = y.next.asInstanceOf[Tuple2[_, _]]
          newtemp = temp.copy(_1 = ("2", temp._1))
        }
        newtemp
      }

      override def hasNext: Boolean = {
        if (x != null && y != null) {
          x.hasNext || y.hasNext
        }
        else {
          true
        }
      }
    }

    /*val x = rdd1.iterator(partitions(0), context)
    val y = rdd2.iterator(partitions(1), context)

    var xRet : List[A] = List[A]()
    var yRet : List[B] = List[B]()
    var returnRet : List[Tuple2[_,_]] = List[Tuple2[_,_]]()

    while(x.hasNext || y.hasNext) {
      if(x.hasNext) {
        var xret = x.next
        xRet ::= xret
        var temp = xret.asInstanceOf[Tuple2[_,_]]
        var newtemp = temp.copy(_1 = ("1", temp._1))

        returnRet ::= newtemp
        println(newtemp)
      }
      if(y.hasNext) {
        var yret = y.next
        yRet ::= yret
        var temp = yret.asInstanceOf[Tuple2[_,_]]
        var newtemp = temp.copy(_1 = ("2", temp._1))
        returnRet ::= newtemp
        println(newtemp)
      }
    }*/

    //f(xRet.iterator, yRet.iterator)
    //var testRet = (yRet ::: xRet).asInstanceOf[Iterator[Tuple2[_,_]]]
    //var result = f(xRet.iterator, yRet.iterator)
    //result

    /* if(turn == true) {
       turn = false
       var x = rdd1.iterator(partitions(0),context).asInstanceOf[Iterator[Tuple2[_,_]]]
       x
     } else
     {
       turn = true
       var y = rdd2.iterator(partitions(1), context).asInstanceOf[Iterator[Tuple2[_,_]]]
       y
     }*/
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

private[spark] class PackagingReduceRDD2[A: ClassTag, B: ClassTag](
                                                                    sc: SparkContext,
                                                                    var f: (Iterator[A],
                                                                      Iterator[B]) =>
                                                                      Iterator[Tuple2[_, _]],
                                                                    var rdd1: RDD[A],
                                                                    var rdd2: RDD[B],
                                                                    preservesPartitioning:
                                                                    Boolean = false)
  extends PackagingBaseRDD(sc, List(rdd1, rdd2), preservesPartitioning) {

  var turn: Boolean = true

  var x: Iterator[A] = null

  var y: Iterator[B] = null

  override def compute(s: Partition, context: TaskContext): Iterator[Tuple2[_, _]] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions

    new Iterator[Tuple2[_, _]] {
      override def next: Tuple2[_, _] = {
        if (x == null) {
          x = rdd1.iterator(partitions(0), context)
        }
        var res = x.next
        if (res.asInstanceOf[Tuple2[_, _]]._1 == null) {
          if (y == null) {
            y = rdd2.iterator(partitions(0), context)
          }
          var resy = y.next.asInstanceOf[Tuple2[_, _]]
          //println(resy)
          resy
        } else {
          //println(res)
          res.asInstanceOf[Tuple2[_, _]]
        }
      }

      override def hasNext: Boolean = {
        if (x != null && y != null) {
          x.hasNext || y.hasNext
        }
        else {
          true
        }
      }
    }

  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

