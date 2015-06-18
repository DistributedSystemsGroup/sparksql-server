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

import scala.language.existentials

import org.apache.spark.sql._
import org.apache.spark.sql.types._


class FilteredScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleFilteredScan(parameters("from").toInt, parameters("to").toInt)(sqlContext)
  }
}

case class SimpleFilteredScan(from: Int, to: Int)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan {

  override def schema: StructType =
    StructType(
      StructField("a", IntegerType, nullable = false) ::
      StructField("b", IntegerType, nullable = false) ::
      StructField("c", StringType, nullable = false) :: Nil)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    val rowBuilders = requiredColumns.map {
      case "a" => (i: Int) => Seq(i)
      case "b" => (i: Int) => Seq(i * 2)
      case "c" => (i: Int) => Seq((i - 1 + 'a').toChar.toString * 10)
    }

    FiltersPushed.list = filters

    // Predicate test on integer column
    def translateFilterOnA(filter: Filter): Int => Boolean = filter match {
      case EqualTo("a", v) => (a: Int) => a == v
      case LessThan("a", v: Int) => (a: Int) => a < v
      case LessThanOrEqual("a", v: Int) => (a: Int) => a <= v
      case GreaterThan("a", v: Int) => (a: Int) => a > v
      case GreaterThanOrEqual("a", v: Int) => (a: Int) => a >= v
      case In("a", values) => (a: Int) => values.map(_.asInstanceOf[Int]).toSet.contains(a)
      case IsNull("a") => (a: Int) => false // Int can't be null
      case IsNotNull("a") => (a: Int) => true
      case Not(pred) => (a: Int) => !translateFilterOnA(pred)(a)
      case And(left, right) => (a: Int) =>
        translateFilterOnA(left)(a) && translateFilterOnA(right)(a)
      case Or(left, right) => (a: Int) =>
        translateFilterOnA(left)(a) || translateFilterOnA(right)(a)
      case _ => (a: Int) => true
    }

    // Predicate test on string column
    def translateFilterOnC(filter: Filter): String => Boolean = filter match {
      case StringStartsWith("c", v) => _.startsWith(v)
      case StringEndsWith("c", v) => _.endsWith(v)
      case StringContains("c", v) => _.contains(v)
      case _ => (c: String) => true
    }

    def eval(a: Int) = {
      val c = (a - 1 + 'a').toChar.toString * 10
      !filters.map(translateFilterOnA(_)(a)).contains(false) &&
        !filters.map(translateFilterOnC(_)(c)).contains(false)
    }

    sqlContext.sparkContext.parallelize(from to to).filter(eval).map(i =>
      Row.fromSeq(rowBuilders.map(_(i)).reduceOption(_ ++ _).getOrElse(Seq.empty)))
  }
}

// A hack for better error messages when filter pushdown fails.
object FiltersPushed {
  var list: Seq[Filter] = Nil
}

class FilteredScanSuite extends DataSourceTest {

  import caseInsensisitiveContext._

  before {
    sql(
      """
        |CREATE TEMPORARY TABLE oneToTenFiltered
        |USING org.apache.spark.sql.sources.FilteredScanSource
        |OPTIONS (
        |  from '1',
        |  to '10'
        |)
      """.stripMargin)
  }

  sqlTest(
    "SELECT * FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i, i * 2, (i - 1 + 'a').toChar.toString * 10)).toSeq)

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT b, a FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i * 2, i)).toSeq)

  sqlTest(
    "SELECT a FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i)).toSeq)

  sqlTest(
    "SELECT b FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT a * 2 FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i * 2)).toSeq)

  sqlTest(
    "SELECT A AS b FROM oneToTenFiltered",
    (1 to 10).map(i => Row(i)).toSeq)

  sqlTest(
    "SELECT x.b, y.a FROM oneToTenFiltered x JOIN oneToTenFiltered y ON x.a = y.b",
    (1 to 5).map(i => Row(i * 4, i)).toSeq)

  sqlTest(
    "SELECT x.a, y.b FROM oneToTenFiltered x JOIN oneToTenFiltered y ON x.a = y.b",
    (2 to 10 by 2).map(i => Row(i, i)).toSeq)

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE a = 1",
    Seq(1).map(i => Row(i, i * 2)))

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE a IN (1,3,5)",
    Seq(1,3,5).map(i => Row(i, i * 2)))

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE A = 1",
    Seq(1).map(i => Row(i, i * 2)))

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE b = 2",
    Seq(1).map(i => Row(i, i * 2)))

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE a IS NULL",
    Seq.empty[Row])

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE a IS NOT NULL",
    (1 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE a < 5 AND a > 1",
    (2 to 4).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE a < 3 OR a > 8",
    Seq(1, 2, 9, 10).map(i => Row(i, i * 2)))

  sqlTest(
    "SELECT a, b FROM oneToTenFiltered WHERE NOT (a < 6)",
    (6 to 10).map(i => Row(i, i * 2)).toSeq)

  sqlTest(
    "SELECT a, b, c FROM oneToTenFiltered WHERE c like 'c%'",
    Seq(Row(3, 3 * 2, "c" * 10)))

  sqlTest(
    "SELECT a, b, c FROM oneToTenFiltered WHERE c like 'd%'",
    Seq(Row(4, 4 * 2, "d" * 10)))

  sqlTest(
    "SELECT a, b, c FROM oneToTenFiltered WHERE c like '%e%'",
    Seq(Row(5, 5 * 2, "e" * 10)))

  testPushDown("SELECT * FROM oneToTenFiltered WHERE A = 1", 1)
  testPushDown("SELECT a FROM oneToTenFiltered WHERE A = 1", 1)
  testPushDown("SELECT b FROM oneToTenFiltered WHERE A = 1", 1)
  testPushDown("SELECT a, b FROM oneToTenFiltered WHERE A = 1", 1)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE a = 1", 1)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE 1 = a", 1)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE a > 1", 9)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE a >= 2", 9)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE 1 < a", 9)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE 2 <= a", 9)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE 1 > a", 0)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE 2 >= a", 2)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE a < 1", 0)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE a <= 2", 2)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE a > 1 AND a < 10", 8)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE a IN (1,3,5)", 3)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE a = 20", 0)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE b = 1", 10)

  testPushDown("SELECT * FROM oneToTenFiltered WHERE a < 5 AND a > 1", 3)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE a < 3 OR a > 8", 4)
  testPushDown("SELECT * FROM oneToTenFiltered WHERE NOT (a < 6)", 5)

  def testPushDown(sqlString: String, expectedCount: Int): Unit = {
    test(s"PushDown Returns $expectedCount: $sqlString") {
      val queryExecution = sql(sqlString).queryExecution
      val rawPlan = queryExecution.executedPlan.collect {
        case p: execution.PhysicalRDD => p
      } match {
        case Seq(p) => p
        case _ => fail(s"More than one PhysicalRDD found\n$queryExecution")
      }
      val rawCount = rawPlan.execute().count()

      if (rawCount != expectedCount) {
        fail(
          s"Wrong # of results for pushed filter. Got $rawCount, Expected $expectedCount\n" +
            s"Filters pushed: ${FiltersPushed.list.mkString(",")}\n" +
            queryExecution)
      }
    }
  }
}

