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

package fr.eurecom.dsg.sparksqlserver.scheduler.postscheduler

import fr.eurecom.dsg.sparksqlserver.container.RewrittenBag
import org.apache.spark.SparkContext

/**
 * Created by hoang on 6/24/15.
 * Abstract class for post scheduling strategy before submitting to the cluster
 */
class PostSchedulingStrategy {

  def initiate(): Unit = {

  }

  def execute(sc : SparkContext, rewritten : Array[RewrittenBag]) : Unit = {

  }
}
