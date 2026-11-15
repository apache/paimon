/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

class SchemaEvolutionNullTypeTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")

  private def write(mode: String, projection: String): Unit =
    withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
      if (mode == "MERGE") {
        sql(s"""MERGE INTO t USING (SELECT $projection) s ON false
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      } else {
        sql(s"INSERT INTO t BY NAME SELECT $projection")
      }
    }

  Seq("MERGE", "INSERT").foreach {
    mode =>
      test(s"$mode: NULL and empty collections preserve existing types") {
        withTable("t") {
          sql("""CREATE TABLE t (
                | id INT, label STRING, items ARRAY<STRING>, attrs MAP<STRING, INT>,
                | payload STRUCT<name: STRING>) USING paimon""".stripMargin)
          val before = spark.table("t").schema
          withSparkSQLConf("spark.paimon.write.merge-schema.type-widening" -> "true") {
            write(mode, "1 AS id, NULL AS label, array() AS items, map() AS attrs, NULL AS payload")
          }
          assert(spark.table("t").schema == before)
          checkAnswer(sql("SELECT * FROM t"), Row(1, null, Seq.empty, Map.empty, null))
        }
      }

      test(s"$mode: nested NULL preserves typed additions in structs, arrays and maps") {
        withTable("t") {
          sql("""CREATE TABLE t (
                | id INT, payload STRUCT<name: STRING>,
                | items ARRAY<STRUCT<name: STRING>>,
                | attrs MAP<STRING, STRUCT<name: STRING>>) USING paimon""".stripMargin)
          sql("""INSERT INTO t VALUES (
                | 0, named_struct('name', 'old'), array(named_struct('name', 'old')),
                | map('key', named_struct('name', 'old')))""".stripMargin)

          write(
            mode,
            """1 AS id, named_struct('name', NULL, 'added', 10) AS payload,
              |array(named_struct('name', NULL, 'added', 20)) AS items,
              |map('key', named_struct('name', NULL, 'added', 30)) AS attrs""".stripMargin
          )

          checkAnswer(
            sql("SELECT * FROM t ORDER BY id"),
            Seq(
              Row(0, Row("old", null), Seq(Row("old", null)), Map("key" -> Row("old", null))),
              Row(1, Row(null, 10), Seq(Row(null, 20)), Map("key" -> Row(null, 30))))
          )
          assert(
            spark.table("t").schema("payload").dataType.asInstanceOf[StructType].fieldNames.toSeq ==
              Seq("name", "added"))
        }
      }

      test(s"$mode: nested NULL does not suppress sibling type widening") {
        withTable("t") {
          sql("CREATE TABLE t (id INT, payload STRUCT<value: INT, label: STRING>) USING paimon")
          sql("INSERT INTO t VALUES (0, named_struct('value', 10, 'label', 'old'))")
          withSparkSQLConf("spark.paimon.write.merge-schema.type-widening" -> "true") {
            write(mode, "1 AS id, named_struct('value', 2147483648L, 'label', NULL) AS payload")
          }
          val payload = spark.table("t").schema("payload").dataType.asInstanceOf[StructType]
          assert(payload("value").dataType == LongType)
          assert(payload("label").dataType == StringType)
          checkAnswer(
            sql("SELECT * FROM t ORDER BY id"),
            Seq(Row(0, Row(10L, "old")), Row(1, Row(2147483648L, null))))
        }
      }

      test(s"$mode: explicit casts allow new NULL and empty-array columns") {
        withTable("t") {
          sql("CREATE TABLE t (id INT) USING paimon")
          write(
            mode,
            "1 AS id, CAST(NULL AS STRING) AS label, CAST(array() AS ARRAY<STRING>) AS items")
          assert(spark.table("t").schema("label").dataType == StringType)
          assert(spark.table("t").schema("items").dataType == ArrayType(StringType))
          checkAnswer(sql("SELECT * FROM t"), Row(1, null, Seq.empty))
        }
      }

      Seq(
        ("NULL AS extra", "`extra`"),
        ("array() AS extra", "`extra`.`element`"),
        ("map() AS extra", "`extra`.`key`"),
        ("named_struct('name', NULL, 'extra', NULL) AS payload", "`payload`.`extra`"),
        ("array(named_struct('name', NULL, 'extra', NULL)) AS items", "`items`.`element`.`extra`"),
        (
          "map('key', named_struct('name', NULL, 'extra', NULL)) AS attrs",
          "`attrs`.`value`.`extra`")
      ).foreach {
        case (projection, path) =>
          test(s"$mode: reject an untyped new field at $path without changing the table") {
            withTable("t") {
              sql("""CREATE TABLE t (
                    | id INT, payload STRUCT<name: STRING>,
                    | items ARRAY<STRUCT<name: STRING>>,
                    | attrs MAP<STRING, STRUCT<name: STRING>>) USING paimon""".stripMargin)
              val before = spark.table("t").schema
              val error = intercept[Exception] {
                write(mode, s"1 AS id, $projection")
              }
              assert(error.getMessage.contains(s"Cannot infer the type of field $path"))
              assert(error.getMessage.contains("CAST"))
              assert(spark.table("t").schema == before)
              checkAnswer(sql("SELECT COUNT(*) FROM t"), Row(0L))
            }
          }
      }
  }

  Seq(false, true).foreach {
    caseSensitive =>
      test(s"NULL resolution respects nested field case sensitivity: $caseSensitive") {
        withTable("t") {
          sql("CREATE TABLE t (id INT, payload STRUCT<Name: STRING>) USING paimon")
          withSparkSQLConf("spark.sql.caseSensitive" -> caseSensitive.toString) {
            if (caseSensitive) {
              val error = intercept[Exception] {
                write("MERGE", "1 AS id, named_struct('name', NULL) AS payload")
              }
              assert(error.getMessage.contains("`payload`.`name`"))
              write("MERGE", "1 AS id, named_struct('Name', NULL) AS payload")
            } else {
              write("MERGE", "1 AS ID, named_struct('name', NULL) AS PAYLOAD")
            }
          }
          checkAnswer(sql("SELECT * FROM t"), Row(1, Row(null)))
          assert(
            spark.table("t").schema("payload").dataType.asInstanceOf[StructType].fieldNames.toSeq ==
              Seq("Name"))
        }
      }
  }

  test("MERGE UPDATE resolves nested NULL while adding a typed field") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, payload STRUCT<name: STRING>) USING paimon
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('name', 'old'))")
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t
              |USING (SELECT 1 AS id, named_struct('name', NULL, 'added', 10) AS payload) s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }
      checkAnswer(sql("SELECT * FROM t"), Row(1, Row(null, 10)))
    }
  }
}
