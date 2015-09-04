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

import scala.collection.mutable.ArrayBuffer

/**
 * Created by hoang on 6/18/15.
 * A bag which contains a list of grouped-DAGs depend on their sharing opportunities.
 * AnalysedBags are the output of the WSDetector
 * We can have many types of sharing so each bag is attached with a label to indicate its sharing type.
 */
class AnalysedBag(sharingType : String, listDag : ArrayBuffer[Array[DAGContainer]]) {

  //label to indicate sharing type
  private val label : String = sharingType

  //ArrayBuffer of Array of DAGContainer. The ArrayBuffer contains two types of Array[DAGContainer]
  //sharing and no-sharing. An Array[DAGContainer] which has size equals to 1 is known as no-sharing bag, and vice-versa.
  private val lstDag : ArrayBuffer[Array[DAGContainer]] = listDag

  def getSharingType() : String = label

  def getListDag() : ArrayBuffer[Array[DAGContainer]] = lstDag

}
