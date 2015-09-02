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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{IndexDelabelingExternalAppendOnlyMap,
DelabelingExternalAppendOnlyMap,
AppendOnlyMap, ExternalAppendOnlyMap}

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C](
                                createCombiner: V => C,
                                mergeValue: (C, V) => C,
                                mergeCombiners: (C, C) => C) {

  // When spilling is enabled sorting will happen externally, but not necessarily with an 
  // ExternalSorter. 
  private val isSpillEnabled = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)

  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) {
          mergeValue(oldValue, kv._2)
        } else {
          createCombiner(kv._2)
        }
      }
      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }

  def combineDelabelingValuesByKey(aggs: Array[Aggregator[K, V, C]],
                                   iter: Iterator[_ <: Product2[K, V]],
                                   context: TaskContext): Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      var mergeVals: Array[(C, V) => C] = Array[(C, V) => C]()

      var createCombiners: Array[(V) => C] = Array[(V) => C]()

      for (i <- 0 to aggs.size - 1)
        mergeVals = mergeVals :+ aggs {
          i
        }.mergeValue

      for (i <- 0 to aggs.size - 1)
        createCombiners = createCombiners :+ aggs {
          i
        }.createCombiner

      val combiners = new AppendOnlyMap[K, C]
      var kv: Product2[K, V] = null

      var updates: Array[(Boolean, C) => C] = Array[(Boolean, C) => C]()
      for (i <- 0 to aggs.size - 1) {
        val update = (hadValue: Boolean, oldValue: C) => {
          if (hadValue) {
            mergeVals {
              i
            }(oldValue, kv._2)
          }
          else {
            createCombiners {
              i
            }(kv._2)
          }
        }
        updates = updates :+ update
      }

      while (iter.hasNext) {
        kv = iter.next()
        val signal = kv._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[String].toInt
        combiners.changeValue(kv._1, updates {
          signal
        })
      }
      combiners.iterator
    } else {
      var mergeCombinersArr: Array[(C, C) => C] = Array[(C, C) => C]()

      for (i <- 0 to aggs.size - 1)
        mergeCombinersArr = mergeCombinersArr :+ aggs {
          i
        }.mergeCombiners

      var mergeVals: Array[(C, V) => C] = Array[(C, V) => C]()
      var createCombiners: Array[(V) => C] = Array[(V) => C]()
      var updates: Array[(Boolean, C) => C] = Array[(Boolean, C) => C]()
      for (i <- 0 to aggs.size - 1)
        mergeVals = mergeVals :+ aggs {
          i
        }.mergeValue

      for (i <- 0 to aggs.size - 1)
        createCombiners = createCombiners :+ aggs {
          i
        }.createCombiner

      val curEntry: Product2[K, V] = null

      for (i <- 0 to aggs.size - 1) {
        val update: (Boolean, C) => C = (hadVal, oldVal) => {
          if (hadVal) {
            mergeVals {
              i
            }(oldVal, curEntry._2)
          }
          else {
            createCombiners {
              i
            }(curEntry._2)
          }
        }
        updates = updates :+ update
      }

      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue,
        mergeCombiners) //new
      // DelabelingExternalAppendOnlyMap[K, V, C](updates, mergeCombinersArr)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }


  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]): Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
  : Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) {
          mergeCombiners(oldValue, kc._2)
        } else {
          kc._2
        }
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }

  def combineDelabelingCombinersByKey(aggs: Array[Aggregator[K, V, C]],
                                      iter: Iterator[_ <: Product2[K, C]],
                                      context: TaskContext)
  : Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: Product2[K, C] = null

      var mergeCombinersArr: Array[(C, C) => C] = Array[(C, C) => C]()

      for (i <- 0 to aggs.size - 1)
        mergeCombinersArr = mergeCombinersArr :+ aggs {
          i
        }.mergeCombiners

      var updateArr: Array[(Boolean, C) => C] = Array[(Boolean, C) => C]()

      for (i <- 0 to aggs.size - 1) {
        val update = (hadValue: Boolean, oldValue: C) => {
          if (hadValue) {
            mergeCombinersArr {
              i
            }(oldValue, kc._2)
          }
          else {
            kc._2
          }
        }
        updateArr = updateArr :+ update
      }

      while (iter.hasNext) {
        kc = iter.next()
        val signal = kc._1.asInstanceOf[Tuple2[_, _]]._1.asInstanceOf[String].toInt
        combiners.changeValue(kc._1, updateArr {
          signal
        })
      }
      combiners.iterator
    } else {

      var mergeCombinersArr: Array[(C, C) => C] = Array[(C, C) => C]()

      for (i <- 0 to aggs.size - 1)
        mergeCombinersArr = mergeCombinersArr :+ aggs {
          i
        }.mergeCombiners

      //var mergeVals : Array[(C,V) => C] = Array[(C,V) => C]()
      var createCombiners: Array[C => C] = Array[C => C]()
      /*var updates : Array[(Boolean, C) => C] = Array[(Boolean, C) => C]()
      for(i<-0 to aggs.size - 1)
        mergeVals = mergeVals :+ aggs{i}.mergeValue*/

      for (i <- 0 to aggs.size - 1)
        createCombiners = createCombiners :+ aggs {
          i
        }.createCombiner.asInstanceOf[(C) => C]

      /*val curEntry: Product2[K, V] = null

      for(i<-0 to aggs.size - 1) {
        val update: (Boolean, C) => C = (hadVal, oldVal) => {
          //if (hadVal) mergeVals{i}(oldVal, curEntry._2) else createCombiners{i}(curEntry._2)
        }
        updates = updates :+ update
      }*/

      //val combiners = new DelabelingExternalAppendOnlyMap[K, C, C](updates, mergeCombinersArr)

      val combiners = new DelabelingExternalAppendOnlyMap[K, C, C](createCombiners,
        mergeCombinersArr,
        mergeCombinersArr)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }

  def combineDelabelingCombinersByKey(index: Integer, iter: Iterator[_ <: Product2[K, C]],
                                      context: TaskContext)
  : Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) {
          mergeCombiners(oldValue, kc._2)
        } else {
          kc._2
        }
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new IndexDelabelingExternalAppendOnlyMap[K, C, C](identity, mergeCombiners,
        mergeCombiners, index)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }

  def combineDelabelingValuesByKey(index: Integer, iter: Iterator[_ <: Product2[K, V]],
                                   context: TaskContext): Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) {
          mergeValue(oldValue, kv._2)
        } else {
          createCombiner(kv._2)
        }
      }
      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.incMemoryBytesSpilled(combiners.memoryBytesSpilled)
        c.taskMetrics.incDiskBytesSpilled(combiners.diskBytesSpilled)
      }
      combiners.iterator
    }
  }

}
