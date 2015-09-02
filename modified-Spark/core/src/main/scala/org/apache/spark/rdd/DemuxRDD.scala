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

import org.apache.spark.{SparkHadoopWriter, Partition, TaskContext}

private[spark] class DemuxRDD[T: ClassTag](
                                            prev: RDD[T],
                                            preservesPartitioning: Boolean = false)
  extends RDD[T](prev) {

  var x: Iterator[T] = null

  var buff: List[T] = List[T]()
  var buff2: List[T] = List[T]()
  var dummyBuff: List[T] = List[T]()

  private var noJob = 0

  private var noWriter = 0

  var turn = 0

  var temp: Tuple2[_, _] = null

  var removelb: Tuple2[_, _] = null

  var newtemp: Tuple2[_, _] = null

  var initWriter: Array[Boolean] = Array[Boolean]()

  var writerArr: Array[SparkHadoopWriter] = null

  def setSparkWriterArray(w: Array[SparkHadoopWriter]): Unit = {
    writerArr = w
  }

  def setNoWriter(nw: Int): Unit = {
    noWriter = nw
    initWriter = new Array[Boolean](noWriter)
  }

  def setNoJob(nj: Int): Unit = {
    noJob = nj
  }

  override val partitioner = if (preservesPartitioning) {
    firstParent[T].partitioner
  } else {
    None
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  def removeLabel(temp: Tuple2[_, _]): Tuple2[_, _] = {
    var res: Tuple2[_, _] = null
    res = temp.copy(_1 = temp._1.asInstanceOf[Tuple2[_, _]]._2)
    res
  }

  override def compute(split: Partition, context: TaskContext) = {
    new Iterator[T] {
      override def next: T = {
        if (x == null) {
          x = firstParent[T].iterator(split, context)
        }
        if (buff.size != 0) {
          temp = buff(0).asInstanceOf[Tuple2[_, _]]
          if (temp._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[Int] == turn) {
            buff2 = buff
            buff = List[T]()
            //println(temp)
            removeLabel(buff2(0).asInstanceOf[Tuple2[_, _]]).asInstanceOf[T]
          } else {
            turn = turn + 1
            if (turn == noJob) {
              turn = 0
            }
            null.asInstanceOf[T]
          }
        } else {
          temp = x.next.asInstanceOf[Tuple2[_, _]]
          if (temp._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[Int] == turn) {
            //println(temp)
            removeLabel(temp).asInstanceOf[T]
            //temp.asInstanceOf[T]
          }
          else {
            if (temp._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[String].toInt >= noJob) {
              val signal = temp._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[Int]
              if (initWriter {
                signal
              } == false) {
                initWriter {
                  signal
                } = true
                writerArr {
                  signal
                }.preSetup()
                val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt
                writerArr {
                  signal
                }.setup(context.stageId, context.partitionId, taskAttemptId)
                writerArr {
                  signal
                }.open()
              }
              removelb = removeLabel(temp)
              writerArr {
                signal
              }.write(removelb.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[AnyRef], removelb
                .asInstanceOf[Tuple2[_, _]]._2.asInstanceOf[AnyRef])
              //println("SAVE: " + removelb)
              if (buff.size == 0 && x.hasNext == false) {
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
              }
            }
            turn = turn + 1
            if (turn == noJob) {
              turn = 0
            }
            if (noJob != 1 &&
              !(temp._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[Int] >= noJob)) {
              buff ::= temp.asInstanceOf[T]
            }
            null.asInstanceOf[T]
          }
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

      override def hasNext: Boolean = {
        if (x != null) {
          if (buff.size != 0) {
            true
          }
          else {
            x.hasNext
          }
        }
        else {
          true
        }
      }
    }
  }
}

/*
private[spark] class DemuxRDD[T: ClassTag](
                                            prev: RDD[T],
                                            preservesPartitioning: Boolean = false)
  extends RDD[T](prev) {

  var x : Iterator[T] = null

  var buff : List[T] = List[T]()
  var buff2 : List[T] = List[T]()
  var dummyBuff : List[T] = List[T]()

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    new Iterator[T] {
      override def next : T = {
        if (x == null)
          x = firstParent[T].iterator(split, context)
        if(buff.size != 0) {
          buff2 = buff
          buff = List[T]()
          buff2(0)
        } else {
          val temp = x.next.asInstanceOf[Tuple2[_,_]]
          if(temp._1.asInstanceOf[Tuple2[String,_]]._1 == "1")
            temp.asInstanceOf[T]
          else {
            buff ::= temp.asInstanceOf[T]
            null.asInstanceOf[T]
          }
        }
      }

      override def hasNext : Boolean = {
        if(x != null)
          x.hasNext
        else
          true
      }
    }
  }
}*/
