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

package fr.eurecom.dsg.sparksqlserver.rewriter

import fr.eurecom.dsg.sparksqlserver.container.{OptimizedBag, RewrittenBag}
import org.apache.spark.SparkContext

/**
 * Created by hoang on 6/18/15.
 * Abstract class for a Rewrite Rule
 */
class RewriteRule {

  def execute(sc: SparkContext, opt : OptimizedBag): RewrittenBag = {
    null
  }
}
