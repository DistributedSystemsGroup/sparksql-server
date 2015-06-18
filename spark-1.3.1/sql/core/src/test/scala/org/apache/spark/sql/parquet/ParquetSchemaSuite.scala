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

package org.apache.spark.sql.parquet

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import parquet.schema.MessageTypeParser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._

class ParquetSchemaSuite extends SparkFunSuite with ParquetTest {
  val sqlContext = TestSQLContext

  /**
   * Checks whether the reflected Parquet message type for product type `T` conforms `messageType`.
   */
  private def testSchema[T <: Product: ClassTag: TypeTag](
      testName: String, messageType: String, isThriftDerived: Boolean = false): Unit = {
    test(testName) {
      val actual = ParquetTypesConverter.convertFromAttributes(
        ScalaReflection.attributesFor[T], isThriftDerived)
      val expected = MessageTypeParser.parseMessageType(messageType)
      actual.checkContains(expected)
      expected.checkContains(actual)
    }
  }

  testSchema[(Boolean, Int, Long, Float, Double, Array[Byte])](
    "basic types",
    """
      |message root {
      |  required boolean _1;
      |  required int32   _2;
      |  required int64   _3;
      |  required float   _4;
      |  required double  _5;
      |  optional binary  _6;
      |}
    """.stripMargin)

  testSchema[(Byte, Short, Int, Long, java.sql.Date)](
    "logical integral types",
    """
      |message root {
      |  required int32 _1 (INT_8);
      |  required int32 _2 (INT_16);
      |  required int32 _3 (INT_32);
      |  required int64 _4 (INT_64);
      |  optional int32 _5 (DATE);
      |}
    """.stripMargin)

  // Currently String is the only supported logical binary type.
  testSchema[Tuple1[String]](
    "binary logical types",
    """
      |message root {
      |  optional binary _1 (UTF8);
      |}
    """.stripMargin)

  testSchema[Tuple1[Seq[Int]]](
    "array",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin)

  testSchema[Tuple1[Map[Int, String]]](
    "map",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testSchema[Tuple1[Pair[Int, String]]](
    "struct",
    """
      |message root {
      |  optional group _1 {
      |    required int32 _1;
      |    optional binary _2 (UTF8);
      |  }
      |}
    """.stripMargin)

  testSchema[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    "deeply nested type",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group bag {
      |            optional group array {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin)

  testSchema[(Option[Int], Map[Int, Option[Double]])](
    "optional types",
    """
      |message root {
      |  optional int32 _1;
      |  optional group _2 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional double value;
      |    }
      |  }
      |}
    """.stripMargin)

  // Test for SPARK-4520 -- ensure that thrift generated parquet schema is generated
  // as expected from attributes
  testSchema[(Array[Byte], Array[Byte], Array[Byte], Seq[Int], Map[Array[Byte], Seq[Int]])](
    "thrift generated parquet schema",
    """
      |message root {
      |  optional binary _1 (UTF8);
      |  optional binary _2 (UTF8);
      |  optional binary _3 (UTF8);
      |  optional group _4 (LIST) {
      |    repeated int32 _4_tuple;
      |  }
      |  optional group _5 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required binary key (UTF8);
      |      optional group value (LIST) {
      |        repeated int32 value_tuple;
      |      }
      |    }
      |  }
      |}
    """.stripMargin, isThriftDerived = true)

  test("DataType string parser compatibility") {
    // This is the generated string from previous versions of the Spark SQL, using the following:
    // val schema = StructType(List(
    //  StructField("c1", IntegerType, false),
    //  StructField("c2", BinaryType, true)))
    val caseClassString =
      "StructType(List(StructField(c1,IntegerType,false), StructField(c2,BinaryType,true)))"

    val jsonString =
      """
        |{"type":"struct","fields":[{"name":"c1","type":"integer","nullable":false,"metadata":{}},{"name":"c2","type":"binary","nullable":true,"metadata":{}}]}
      """.stripMargin

    val fromCaseClassString = ParquetTypesConverter.convertFromString(caseClassString)
    val fromJson = ParquetTypesConverter.convertFromString(jsonString)

    (fromCaseClassString, fromJson).zipped.foreach { (a, b) =>
      assert(a.name == b.name)
      assert(a.dataType === b.dataType)
      assert(a.nullable === b.nullable)
    }
  }

  test("merge with metastore schema") {
    // Field type conflict resolution
    assertResult(
      StructType(Seq(
        StructField("lowerCase", StringType),
        StructField("UPPERCase", DoubleType, nullable = false)))) {

      ParquetRelation2.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("lowercase", StringType),
          StructField("uppercase", DoubleType, nullable = false))),

        StructType(Seq(
          StructField("lowerCase", BinaryType),
          StructField("UPPERCase", IntegerType, nullable = true))))
    }

    // MetaStore schema is subset of parquet schema
    assertResult(
      StructType(Seq(
        StructField("UPPERCase", DoubleType, nullable = false)))) {

      ParquetRelation2.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("uppercase", DoubleType, nullable = false))),

        StructType(Seq(
          StructField("lowerCase", BinaryType),
          StructField("UPPERCase", IntegerType, nullable = true))))
    }

    // Metastore schema contains additional non-nullable fields.
    assert(intercept[Throwable] {
      ParquetRelation2.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("uppercase", DoubleType, nullable = false),
          StructField("lowerCase", BinaryType, nullable = false))),

        StructType(Seq(
          StructField("UPPERCase", IntegerType, nullable = true))))
    }.getMessage.contains("detected conflicting schemas"))

    // Conflicting non-nullable field names
    intercept[Throwable] {
      ParquetRelation2.mergeMetastoreParquetSchema(
        StructType(Seq(StructField("lower", StringType, nullable = false))),
        StructType(Seq(StructField("lowerCase", BinaryType))))
    }
  }

  test("merge missing nullable fields from Metastore schema") {
    // Standard case: Metastore schema contains additional nullable fields not present
    // in the Parquet file schema.
    assertResult(
      StructType(Seq(
        StructField("firstField", StringType, nullable = true),
        StructField("secondField", StringType, nullable = true),
        StructField("thirdfield", StringType, nullable = true)))) {
      ParquetRelation2.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("firstfield", StringType, nullable = true),
          StructField("secondfield", StringType, nullable = true),
          StructField("thirdfield", StringType, nullable = true))),
        StructType(Seq(
          StructField("firstField", StringType, nullable = true),
          StructField("secondField", StringType, nullable = true))))
    }

    // Merge should fail if the Metastore contains any additional fields that are not
    // nullable.
    assert(intercept[Throwable] {
      ParquetRelation2.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("firstfield", StringType, nullable = true),
          StructField("secondfield", StringType, nullable = true),
          StructField("thirdfield", StringType, nullable = false))),
        StructType(Seq(
          StructField("firstField", StringType, nullable = true),
          StructField("secondField", StringType, nullable = true))))
    }.getMessage.contains("detected conflicting schemas"))
  }
}
