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

package fr.eurecom.dsg.sparksqlserver.util

/**
 * Created by hoang on 6/2/15.
 * Contain all the parameters and configurations of Server
 */
object ServerConstants {
  //port to listen on to get DAG
  val DAG_LISTENER_PORT = 9991
  //port to listen on to get jar file
  val JAR_LISTENER_PORT = 9992
  //the worksharing Executor time-period check, we should sleep an amount of time
  //because it contains a while loop and can exhaust the CPU
  val DAG_QUEUE_SLEEP_PERIOD = 5000
  //window size of the queue to do the optimizations
  val DAG_QUEUE_WINDOW_SIZE = 2
  val PRE_SCHEDULING_STRATEGY = "DUMMY"
  val POST_SCHEDULING_STRATEGY = "FIFO"
}
