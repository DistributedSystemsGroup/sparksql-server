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

import org.apache.spark.{Logging, Partition, TaskContext}

class MuxRDD[T: ClassTag](
                           prev: RDD[T],
                           preservesPartitioning: Boolean = false) extends RDD[T](prev)
with Logging {

  var buff: Array[T] = Array[T]()

  buff = buff :+ null.asInstanceOf[T]

  var buff2: Array[T] = Array[T]()

  buff2 = buff2 :+ null.asInstanceOf[T]

  var buffer: List[T] = List[T]()

  var lastRead: Array[Boolean] = null

  var x: Iterator[T] = null

  private var noJob = 0

  var noRead = 0

  def setNoJob(no: Int): Unit = {
    noJob = no
    noRead = noJob - 1
    lastRead = new Array[Boolean](noRead)
    for (i <- 0 to lastRead.size - 1)
      lastRead {
        i
      } = false
  }

  override val partitioner = if (preservesPartitioning) {
    firstParent[T].partitioner
  } else {
    None
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {

    if (x == null) {
      x = firstParent[T].iterator(split, context)
    }

    if (buff(0) == null.asInstanceOf[T] && buff2(0) == null.asInstanceOf[T] && !x.hasNext) {
      scala.collection.Iterator.empty
    }
    else {
      new Iterator[T] {
        override def next(): T = {
          if (buff(0) == null.asInstanceOf[T]) {
            if (x == null) {
              x = firstParent[T].iterator(split, context)
            }
            buff(0) = null.asInstanceOf[T]
            if (x.hasNext) {
              buff(0) = x.next
            }
            if (!x.hasNext) {
              for (i <- 0 to lastRead.size - 1)
                lastRead {i} = true
            }
            buff2(0) = buff(0)
          } else {
            buff2(0) = buff(0)
            noRead = noRead - 1
            lastRead {noRead} = false
            if (noRead == 0) {
              buff(0) = null.asInstanceOf[T]
              noRead = noJob - 1
            }
          }
          buff2(0)
        }

        def checkLastRead(): Boolean = {
          var res = false
          for (i <- 0 to lastRead.size - 1)
            res = res || lastRead {i}
          res
        }

        override def hasNext(): Boolean = {
          if (x != null) {
            if (x.hasNext == false && checkLastRead()) {
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
}
