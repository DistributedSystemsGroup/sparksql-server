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

package org.apache.spark.sql.hive.execution

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.hive.test.TestHive

/**
 * A set of tests that validates support for Hive SerDe.
 */
class HiveSerDeSuite extends HiveComparisonTest with BeforeAndAfterAll {

  override def beforeAll() = {
    TestHive.cacheTables = false
  }

  createQueryTest(
    "Read and write with LazySimpleSerDe (tab separated)",
    "SELECT * from serdeins")

  createQueryTest("Read with RegexSerDe", "SELECT * FROM sales")

  createQueryTest("Read with AvroSerDe", "SELECT * FROM episodes")

  createQueryTest("Read Partitioned with AvroSerDe", "SELECT * FROM episodes_part")
}
