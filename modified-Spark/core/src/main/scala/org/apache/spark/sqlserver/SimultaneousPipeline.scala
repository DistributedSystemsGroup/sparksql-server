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

package org.apache.spark.sqlserver

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat,
RecordWriter => NewRecordWriter}
import org.apache.spark._
import org.apache.spark.rdd._


class SimultaneousPipeline(sc : SparkContext, rdds : Array[RDD[_]], outputPath : String) {

  /**
   * Check an DAG if it contains a ShuffledRDD or not
   * Recursively traverse and check a DAG if it has a ShuffledRDD
   * @param rdd
   * @return
   */
  def hasShuffledRDD(rdd: RDD[_]): Boolean = {
    var res = false
    if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      res = true
    }
    else
    if (rdd.getDP.size != 0) {
      res = hasShuffledRDD(rdd.getDP {
        0
      }.rdd)
    }
    res
  }

  /**
   * get shuffledRDD of a DAG
   * Recursively traverse and check a DAG until it meets a ShuffledRDD
   * @param rdd
   * @return
   */
  def getShuffledRDD(rdd: RDD[_]): RDD[_] = {
    var res: RDD[_] = null
    if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      res = rdd
    }
    else {
      res = getShuffledRDD(rdd.getDP {
        0
      }.rdd)
    }
    res
  }

  /**
   * get aggregator of each ShuffledRDD, then, add them into an Array of Aggregator
   * This array will be passed to an "united" ShuffledRDD
   * @param rdds
   * @return
   */
  def getShuffleAggregators(rdds: Array[RDD[_]]): Array[Aggregator[_, _, _]] = {
    var aggs: Array[Aggregator[_, _, _]] = Array[Aggregator[_, _, _]]()
    for (i <- 0 to rdds.size - 1)
      if (hasShuffledRDD(rdds {
        i
      })) {
        val temp = getShuffledRDD(rdds {
          i
        }).asInstanceOf[ShuffledRDD[_, _, _]].getAggregator()
        if (temp.isDefined) {
          aggs = aggs :+ temp.get
        }
      }
    aggs
  }

  /**
   * create a new and united shuffledRDD which contains list of aggregators
   * @param listShuffle
   * @param dummy
   * @return
   */
  def createUnitedShuffledRDD(listShuffle: Array[Array[ShuffledRDD[_, _, _]]],
                              dummy: RDD[_]): RDD[_] = {
    for (i <- 0 to listShuffle.size - 1)
      if (listShuffle {
        i
      }.size > 0) {
        dummy.addToAggregatorArray(listShuffle {
          i
        } {
          listShuffle {
            i
          }.size - 1
        }.getAggregator().get)
      }
    dummy
  }

  /**
   * find the prev part of a shuffledRDD in a DAG
   * recursively traverse and check
   * @param rdd
   * @return
   */
  def getPreShuffledRDD(rdd: RDD[_]): RDD[_] = {
    var res: RDD[_] = null
    if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      res = rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev
    }
    else {
      res = getPreShuffledRDD(rdd.getDP {
        0
      }.rdd)
    }
    res
  }

  def attachMuxToPostScanRDD(rdd: RDD[_], post: RDD[_], newDep: Seq[Dependency[_]]): Unit = {
    if (rdd == post) {
      rdd.setDeps(newDep)
    }
    else {
      if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
        if (rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev() == post) {
          rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().setDeps(newDep)
          if (rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP_() != null) {
            rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().setDeps_(newDep)
          }
        }
        else {
          attachMuxToPostScanRDD(rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev(), post, newDep)
        }
      }
      else {
        if (rdd.getDP {
          0
        }.rdd == post) {
          rdd.getDP {
            0
          }.rdd.setDeps(newDep)
          if (rdd.getDP {
            0
          }.rdd.getDP_() != null) {
            rdd.getDP {
              0
            }.rdd.setDeps_(newDep)
          }
        }
        else {
          attachMuxToPostScanRDD(rdd.getDP {
            0
          }.rdd, post, newDep)
        }
      }
    }
  }

  //not used
  def findPreShuffledRDD(rdd: RDD[_]): RDD[_] = {
    if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      rdd
    }
    else
    if (rdd.getDP != null) {
      findPreShuffledRDD(rdd.getDP {
        0
      }.rdd)
    }
    null
  }

  /**
   * get the post part of a shuffleRDD (post-shuffle-pre)
   * @param rdd
   * @return
   */
  def getPostShuffeldRDD(rdd: RDD[_]): RDD[_] = {
    var res: RDD[_] = null
    if (rdd.getDP {
      0
    }.rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      res = rdd
    }
    else {
      res = getPostShuffeldRDD(rdd.getDP {
        0
      }.rdd)
    }
    res
  }

  /**
   * get the RDD after a scan operations in a DAG
   * @param rdd
   * @param scan
   * @return
   */
  def findPostScanRDD(rdd: RDD[_], scan: RDD[_]): RDD[_] = {
    if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      if (rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev().getDP {
        0
      }.rdd == scan) {
        rdd
      }
      else {
        findPostScanRDD(rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev(), scan)
      }
    }
    else {
      if (rdd.getDP {
        0
      }.rdd == scan) {
        rdd
      }
      else {
        findPostScanRDD(rdd.getDP {
          0
        }.rdd, scan)
      }
    }
  }

  /**
   * get the RDD in charge of scanning data
   * @param rdd
   * @return
   */
  def findScanRDD(rdd: RDD[_]): RDD[_] = {
    var res: RDD[_] = null
    if (rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      if (rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev.getDP.size != 0) {
        if (rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev.getDP {
          0
        }.rdd.isInstanceOf[HadoopRDD[_, _]]) {
          res = rdd
        }
        else {
          res = findScanRDD(rdd.asInstanceOf[ShuffledRDD[_, _, _]].getPrev)
        }
      }
      else {
        res
      }
    } else {
      if (rdd.getDP.size != 0) {
        if (rdd.getDP {
          0
        }.rdd.isInstanceOf[HadoopRDD[_, _]]) {
          res = rdd
        }
        else {
          res = findScanRDD(rdd.getDP {
            0
          }.rdd)
        }
      }
      else {
        res
      }
    }
    res
  }

  //not used
  def insertCommonMux(rdds: Array[RDD[_]],
                      shuffledList: Array[Array[ShuffledRDD[_, _, _]]]): Array[RDD[_]] = {

    var mapRdds: Array[RDD[_]] = Array[RDD[_]]()

    for (i <- 0 to rdds.size - 1)
      if (hasShuffledRDD(rdds {
        i
      })) {
        mapRdds = mapRdds :+ shuffledList {
          i
        } {
          0
        }.getPrev()
      } //getPreShuffledRDD(rdds{i})
      else {
        mapRdds = mapRdds :+ rdds {
          i
        }
      }

    val scan = findScanRDD(mapRdds {
      0
    })
    val muxing = scan.mux
    val newDepSeq = Seq[Dependency[_]]() :+ new OneToOneDependency(muxing)
    muxing.asInstanceOf[MuxRDD[_]].setNoJob(mapRdds.size)
    for (i <- 0 to mapRdds.size - 1) {
      val scan = findScanRDD(mapRdds {
        i
      })
      val postScan = findPostScanRDD(mapRdds {
        i
      }, scan)
      attachMuxToPostScanRDD(mapRdds {
        i
      }, postScan, newDepSeq)
    }
    mapRdds
  }

  /**
   * insert a muxRDD after a scanRDD and do the multiplex the input
   * @param rdds
   */
  def insertCommonMux2(rdds: Array[RDD[_]]): Unit = {

    val scan = findScanRDD(rdds {
      0
    })
    val muxing = scan.mux
    val newDepSeq = Seq[Dependency[_]]() :+ new OneToOneDependency(muxing)
    muxing.asInstanceOf[MuxRDD[_]].setNoJob(rdds.size)

    for (i <- 0 to rdds.size - 1) {
      val scan = findScanRDD(rdds {
        i
      })
      val postScan = findPostScanRDD(rdds {
        i
      }, scan)
      attachMuxToPostScanRDD(rdds {
        i
      }, postScan, newDepSeq)
      //println(rdds{i}.toDebugString)
    }
  }

  /**
   * insert a PullerRDD to pull the record
   * @param rdds
   * @param rdd
   * @return
   */
  def insertCommonDemux(rdds: Array[RDD[_]], rdd: RDD[_]): RDD[_] = {
    //insert demux on top
    val demuxing = rdd.demux

    var reduceRdds: Array[RDD[_]] = Array[RDD[_]]()

    var count = 0

    for (i <- 0 to rdds.size - 1)
      if (hasShuffledRDD(rdds {
        i
      })) {
        reduceRdds = reduceRdds :+ getPostShuffeldRDD(rdds {
          i
        })
        count = count + 1
      }

    demuxing.asInstanceOf[DemuxRDD[_]].setNoJob(count)

    val newDep = Seq[Dependency[_]]() :+ new OneToOneDependency(demuxing)



    for (i <- 0 to reduceRdds.size - 1) {
      reduceRdds {
        i
      }.setDeps(newDep)
      if (reduceRdds {
        i
      }.getDP_() != null) {
        reduceRdds {
          i
        }.setDeps_(newDep)
      }
    }

    val res = rdds {
      0
    }.delabel(rdds)
    res
    //get pre
  }

  /**
   * get the ancestor RDD of a DAG
   * @param rdd
   * @return
   */
  def getFirstRDDinDAG(rdd: RDD[_]): RDD[_] = {
    var res: RDD[_] = null
    if (rdd.getDP == null) {
      res = rdd
    }
    else
    if (rdd.getDP != null && rdd.getDP.size != 0) {
      res = getFirstRDDinDAG(rdd.getDP {
        0
      }.rdd)
    }
    res
  }

  /**
   * insert a Puller to pull data and some writer to do the saving
   * @param shufflePart
   * @param unitedShuffle
   * @param noRDD
   * @return
   */
  def insertCommonDemux2(shufflePart: Array[RDD[_]], unitedShuffle: RDD[_],
                         noRDD: Int, path: String): RDD[Tuple2[_, _]] = {
    //insert demux on top
    var writerArr: Array[SparkHadoopWriter] = new Array[SparkHadoopWriter](noRDD)

    for (i <- 0 to writerArr.size - 1)
      writerArr {
        i
      } = createWriter(path + i.toString)

    val demuxing = unitedShuffle.demux

    demuxing.asInstanceOf[DemuxRDD[_]].setNoWriter(writerArr.size)

    demuxing.asInstanceOf[DemuxRDD[_]].setSparkWriterArray(writerArr)

    demuxing.asInstanceOf[DemuxRDD[_]].setNoJob(shufflePart.size)

    val newDep = Seq[Dependency[_]]() :+ new OneToOneDependency(demuxing)

    for (i <- 0 to shufflePart.size - 1) {
      val firstRDD = getFirstRDDinDAG(shufflePart {
        i
      })
      firstRDD.setDeps(newDep)
      if (firstRDD.getDP_() != null) {
        firstRDD.setDeps_(newDep)
      }
    }

    val res = shufflePart {
      0
    }.delabel(shufflePart)

    res.asInstanceOf[DeLabellingRDD].setNoWriter(noRDD)

    writerArr = new Array[SparkHadoopWriter](noRDD)

    for (i <- 0 to writerArr.size - 1)
      writerArr {
        i
      } = createWriter(path + i.toString)

    res.asInstanceOf[DeLabellingRDD].setSparkWriterArray(writerArr)
    res
  }

  /**
   * create SparkHadoopWriter, mostly bring from saveAsTextFile function of RDD
   * @param path
   * @return
   */
  def createWriter(path: String): SparkHadoopWriter = {
    var res: SparkHadoopWriter = null
    val confs = new JobConf(sc.hadoopConfiguration)
    val hadoopConf = confs
    hadoopConf.setOutputKeyClass(Class.forName("org.apache.hadoop.io.NullWritable"))
    hadoopConf.setOutputValueClass(Class.forName("org.apache.hadoop.io.Text"))
    hadoopConf.set("mapred.output.format.class", "org.apache.hadoop.mapred.TextOutputFormat")

    // Use configured output committer if already set
    if (confs.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriter.createPathFromString(path, hadoopConf))

    val job = new NewAPIHadoopJob(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    res = new SparkHadoopWriter(hadoopConf)
    res
  }

  def isOutputSpecValidationEnabled: Boolean = {
    val validationDisabled = PairRDDFunctions.disableOutputSpecValidation.value
    val enabledInConf = sc.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
    enabledInConf && !validationDisabled
  }

  /**
   * Check if all DAGs has at least one ShuffledRDD
   * @param hasShuffled
   * @return
   */
  def atLeastOneShuffle(hasShuffled: Array[Boolean]): Boolean = {
    var res: Boolean = false
    for (i <- 0 to hasShuffled.size - 1)
      res = res || hasShuffled {
        i
      }
    res
  }

  def atLeastOneShuffle(rdds: Array[RDD[_]]): Boolean = {
    var res: Boolean = false
    for (i <- 0 to rdds.size - 1)
      res = res || hasShuffledRDD(rdds {
        i
      })
    res
  }

  def findListShuffledRDD(rdd: RDD[_]): Array[ShuffledRDD[_, _, _]] = {
    var res: Array[ShuffledRDD[_, _, _]] = Array[ShuffledRDD[_, _, _]]()
    var temp: RDD[_] = rdd
    while (hasShuffledRDD(temp)) {
      val shuff = getShuffledRDD(temp).asInstanceOf[ShuffledRDD[_, _, _]]
      res = res :+ shuff
      temp = getPreShuffledRDD(temp)
    }
    res
  }

  def findListPostShuffledRDD(listShuffled: Array[ShuffledRDD[_, _, _]],
                              rdd: RDD[_]): Array[RDD[_]] = {
    var res: Array[RDD[_]] = Array[RDD[_]]()
    var temp: RDD[_] = rdd
    for (i <- 0 to listShuffled.size - 1) {
      val r: RDD[_] = getPostShuffeldRDD(getPreShuffledRDD(temp))
      r.setDeps(null)
      res = res :+ r
      temp = getPreShuffledRDD(temp)
    }
    res
  }

  /**
   * Check if List of ShuffledRDD is empty or not
   * @param shuffleList
   * @return
   */
  def shuffleNotYetRemoved(shuffleList: Array[Array[ShuffledRDD[_, _, _]]]): Boolean = {
    var res = false
    for (i <- 0 to shuffleList.size - 1)
      if (shuffleList {
        i
      }.size > 0) {
        res = true
      }
    res
  }

  /**
   * set the ShuffledRDD (if has) of an RDD to null
   * @param rdd
   */
  def testSetShuffleNull(rdd: RDD[_]): Unit = {
    if (rdd.getDP {
      0
    }.rdd.isInstanceOf[ShuffledRDD[_, _, _]]) {
      rdd.setDeps(null)
      rdd.setDeps_(null)
    } else {
      testSetShuffleNull(rdd.getDP {
        0
      }.rdd)
    }
  }

  /**
   * Check if all parts of DAGs have been processed
   * @param parts: list of parts of DAGs
   * @return
   */
  def isEmpty(parts: Array[Array[RDD[_]]]): Boolean = {
    var res = true
    for (i <- 0 to parts.size - 1)
      res = res && parts {
        i
      }.isEmpty
    res
  }

  def execute(): RDD[_] = {

    var res: RDD[Tuple2[_, _]] = null

    val noRdd = rdds.size

    //get shuffledRDD of each DAG
    //each DAG can contain one or more than one shuffledRDD
    //so we create an array, each element is also an array of ShuffledRDD which is associated to
    // each DAG
    var listShuffle: Array[Array[ShuffledRDD[_, _, _]]] = Array[Array[ShuffledRDD[_, _, _]]]()

    for (i <- 0 to rdds.size - 1) {
      val res: Array[ShuffledRDD[_, _, _]] = findListShuffledRDD(rdds {
        i
      })
      listShuffle = listShuffle :+ res
    }

    //break each DAG into parts
    //we have at least one part which has no shuffle at all
    //the other part will be the transformation after a shuffle
    var parts: Array[Array[RDD[_]]] = Array[Array[RDD[_]]]()
    for (i <- 0 to rdds.size - 1) {
      var partI: Array[RDD[_]] = Array[RDD[_]]()
      if (!rdds {
        i
      }.isInstanceOf[ShuffledRDD[_, _, _]]) {
        partI = partI :+ rdds {
          i
        }
        if (hasShuffledRDD(partI {
          partI.size - 1
        })) {
          testSetShuffleNull(partI {
            partI.size - 1
          })
        }
      }
      for (j <- 0 to listShuffle {
        i
      }.size - 1) {
        partI = partI :+ listShuffle {
          i
        } {
          j
        }.getPrev()
        listShuffle {
          i
        } {
          j
        }.setPrev(null)
        if (hasShuffledRDD(partI {
          partI.size - 1
        })) {
          testSetShuffleNull(partI {
            partI.size - 1
          })
        }
      }
      parts = parts :+ partI
    }

    //pack first part of each DAG together: Mux - Labeling
    //first part contains no ShuffledRDD, so we just pack them using a Mux and a Labeling
    var firstParts: Array[RDD[_]] = Array[RDD[_]]()
    for (i <- 0 to parts.size - 1)
      firstParts = firstParts :+ parts {
        i
      } {
        parts {
          i
        }.size - 1
      }
    insertCommonMux2(firstParts)
    val labeled2 = firstParts {
      0
    }.label(firstParts)
    labeled2.asInstanceOf[LabellingRDD].setNoWriter(noRdd)

    //check each DAG has shuffledRDD or not, then we will decide to write its output out or pass
    // it to the shuffle
    val hasShuffled2: Array[Boolean] = new Array[Boolean](parts.size)

    for (i <- 0 to listShuffle.size - 1)
      if (listShuffle {
        i
      }.size >= 1) {
        hasShuffled2 {
          i
        } = true
      }

    for (i <- 0 to parts.size - 1)
      parts {
        i
      } = parts {
        i
      }.dropRight(1)

    //set hasShuffled to labelRDD
    labeled2.asInstanceOf[LabellingRDD].setHasShuffled(hasShuffled2)
    val path: String = outputPath
    val writerArr: Array[SparkHadoopWriter] = new Array[SparkHadoopWriter](noRdd)
    for (i <- 0 to writerArr.size - 1)
      writerArr {
        i
      } = createWriter(path + i.toString)
    labeled2.asInstanceOf[LabellingRDD].setNoWriter(noRdd)
    labeled2.asInstanceOf[LabellingRDD].setSparkWriterArray(writerArr)

    if (!atLeastOneShuffle(hasShuffled2)) {
      res = labeled2
    }
    else {
      var puller = labeled2
      while (!isEmpty(parts)) {
        val hasShuffled: Array[Boolean] = new Array[Boolean](listShuffle.size)
        var shuffleParts: Array[RDD[_]] = Array[RDD[_]]()
        val dummyReduce = puller.map(x => (x._1.asInstanceOf[Tuple2[_, _]], x._2.asInstanceOf[Int]))
          .reduceByKey(_ - _)
        val unitedShuffled = createUnitedShuffledRDD(listShuffle, dummyReduce)
        for (i <- 0 to parts.size - 1)
          if (parts {
            i
          }.size > 0) {
            shuffleParts = shuffleParts :+ parts {
              i
            } {
              parts {
                i
              }.size - 1
            }
          }
        res = insertCommonDemux2(shuffleParts, unitedShuffled, noRdd, outputPath)

        //drop shuffledRDD after one pass
        for (i <- 0 to listShuffle.size - 1)
          listShuffle {
            i
          } = listShuffle {
            i
          }.dropRight(1)

        for (i <- 0 to listShuffle.size - 1)
          if (listShuffle {
            i
          }.size > 0) {
            hasShuffled {
              i
            } = true
          }
          else {
            hasShuffled {
              i
            } = false
          }

        //drop processed parts after one pass
        for (i <- 0 to parts.size - 1)
          parts {
            i
          } = parts {
            i
          }.dropRight(1)

        res.asInstanceOf[DeLabellingRDD].setHasShuffled(hasShuffled)

        puller = res
      }
      if (isEmpty(parts)) {
        if (shuffleNotYetRemoved(listShuffle)) {
          val dummyReduce = puller
            .map(x => (x._1.asInstanceOf[Tuple2[_, _]], x._2.asInstanceOf[Int])).reduceByKey(_ - _)
          val unitedShuffled = createUnitedShuffledRDD(listShuffle, dummyReduce)
          unitedShuffled.asInstanceOf[ShuffledRDD[_, _, _]].setPrev(puller)
          res = unitedShuffled.asInstanceOf[RDD[Tuple2[_, _]]]
        }
      }
      //println(res.toDebugString)

      //res.saveAsTextFile(args{6})//.collect.foreach(println)
      //res.saveAsTextFile("/Users/suwax/test/")
    }
    res
  }

}
