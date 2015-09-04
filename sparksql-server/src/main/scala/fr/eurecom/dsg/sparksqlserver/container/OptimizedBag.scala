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

package fr.eurecom.dsg.sparksqlserver.container

import fr.eurecom.dsg.sparksqlserver.rewriter.RewriteRule

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/18/15.
 * Output of Optimizer. An no-sharing or sharing AnalysedBag after going through the optimizer
 * is also turned into an OptimizedBag.
 */
class OptimizedBag(listDag : Array[DAGContainer], sharingType : String, canShare : Boolean, r : RewriteRule) {

  //label to indicate sharing type
  private val label : String = sharingType

  private val rule : RewriteRule = r

  //ArrayBuffer of Array of DAGContainer. The ArrayBuffer contains two types of Array[DAGContainer]
  //sharing and no-sharing. An Array[DAGContainer] which has size equals to 1 is known as no-sharing bag, and vice-versa.
  private val lstDag : Array[DAGContainer] = listDag

  def getSharingType() : String = label

  def getListDag() : Array[DAGContainer] = lstDag

  def getRule() : RewriteRule = rule

  private val sharable : Boolean = canShare

  def isSharable() : Boolean = sharable



}
