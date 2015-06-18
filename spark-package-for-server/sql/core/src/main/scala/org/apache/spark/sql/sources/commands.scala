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
package org.apache.spark.sql.sources

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand

private[sql] case class InsertIntoDataSource(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = DataFrame(sqlContext, query)
    // Apply the schema of the existing table to the new data.
    val df = sqlContext.createDataFrame(
      data.queryExecution.toRdd, logicalRelation.schema, needsConversion = false)
    relation.insert(df, overwrite)

    // Invalidate the cache.
    sqlContext.cacheManager.invalidateCache(logicalRelation)

    Seq.empty[Row]
  }
}
