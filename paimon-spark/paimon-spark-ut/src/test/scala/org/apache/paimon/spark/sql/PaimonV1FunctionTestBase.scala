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

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.spark.function.FunctionResources._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.expressions.Explode

import java.io.File

abstract class PaimonV1FunctionTestBase extends PaimonSparkTestWithRestCatalogBase {

  private val hivePosExplode = "org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode"

  test("Paimon V1 Function: persistent Hive UDTF in lateral view") {
    withUserDefinedFunction("udtf_pos_explode" -> false) {
      sql(s"CREATE FUNCTION udtf_pos_explode AS '$hivePosExplode'")
      withTable("udtf_input") {
        sql("CREATE TABLE udtf_input (id INT, items ARRAY<INT>) USING paimon")
        sql("INSERT INTO udtf_input VALUES (1, array(10, 20)), (2, array()), (3, NULL)")

        // Resolve a cold REST catalog function before any SELECT invocation populates its registry.
        Seq("udtf_pos_explode", "test.udtf_pos_explode").foreach {
          name =>
            checkAnswer(
              sql(s"""SELECT id, position, item FROM udtf_input
                     |LATERAL VIEW $name(items) u AS position, item
                     |""".stripMargin),
              Seq(Row(1, 0, 10), Row(1, 1, 20))
            )
        }

        checkAnswer(
          sql("""SELECT id, position, item FROM udtf_input
                |LATERAL VIEW OUTER udtf_pos_explode(items) u AS position, item
                |""".stripMargin),
          Seq(Row(1, 0, 10), Row(1, 1, 20), Row(2, null, null), Row(3, null, null))
        )
        checkAnswer(
          sql("SELECT id, paimon.test.udtf_pos_explode(items) FROM udtf_input"),
          Seq(Row(1, 0, 10), Row(1, 1, 20)))

        val session = spark.newSession()
        session.sql("USE paimon.test")
        checkAnswer(
          session.sql("""SELECT id, position, item FROM udtf_input
                        |LATERAL VIEW udtf_pos_explode(items) u AS position, item
                        |""".stripMargin),
          Seq(Row(1, 0, 10), Row(1, 1, 20))
        )
      }
    }
  }

  test("Paimon V1 Function: lateral view database name can match a catalog") {
    withDatabase("paimon.paimon") {
      sql("CREATE DATABASE paimon.paimon")
      withUserDefinedFunction("paimon.paimon.udtf_pos_explode" -> false) {
        sql(s"CREATE FUNCTION paimon.paimon.udtf_pos_explode AS '$hivePosExplode'")
        checkAnswer(
          sql("""SELECT position, item FROM VALUES (array(10, 20)) AS t(items)
                |LATERAL VIEW paimon.udtf_pos_explode(items) u AS position, item
                |""".stripMargin),
          Seq(Row(0, 10), Row(1, 20))
        )
      }
    }
  }

  test("Paimon V1 Function: Hive UDTF in CTE and dependent lateral views") {
    withUserDefinedFunction("udtf_pos_explode" -> false) {
      sql(s"CREATE FUNCTION udtf_pos_explode AS '$hivePosExplode'")
      checkAnswer(
        sql("""WITH expanded AS (
              |  SELECT id, item FROM VALUES (1, array(10, 20)) AS t(id, items)
              |  LATERAL VIEW udtf_pos_explode(items) u AS position, item
              |)
              |SELECT * FROM expanded
              |""".stripMargin),
        Seq(Row(1, 10), Row(1, 20))
      )
      checkAnswer(
        sql("""SELECT id, a.item, b.item FROM VALUES (1, array(10, 20)) AS t(id, items)
              |LATERAL VIEW udtf_pos_explode(items) a AS position, item
              |LATERAL VIEW udtf_pos_explode(array(a.item, a.item + 1)) b AS position, item
              |""".stripMargin),
        Seq(Row(1, 10, 10), Row(1, 10, 11), Row(1, 20, 20), Row(1, 20, 21))
      )
    }
  }

  test("Paimon V1 Function: Hive UDTF in scalar subquery") {
    withUserDefinedFunction("udtf_pos_explode" -> false) {
      sql(s"CREATE FUNCTION udtf_pos_explode AS '$hivePosExplode'")
      Seq("posexplode", "udtf_pos_explode").foreach {
        name =>
          checkAnswer(
            sql(s"""SELECT (
                   |  SELECT count(*) FROM VALUES (array(10, 20)) AS t(items)
                   |  LATERAL VIEW $name(items) u AS position, item
                   |) AS n
                   |""".stripMargin),
            Seq(Row(2L))
          )
      }
    }
  }

  test("Paimon V1 Function: Hive UDTF in EXISTS subquery") {
    withUserDefinedFunction("udtf_pos_explode" -> false) {
      sql(s"CREATE FUNCTION udtf_pos_explode AS '$hivePosExplode'")
      Seq(10 -> true, 30 -> false).foreach {
        case (item, expected) =>
          checkAnswer(
            sql(s"""SELECT EXISTS (
                   |  SELECT 1 FROM VALUES (array(10, 20)) AS t(items)
                   |  LATERAL VIEW test.udtf_pos_explode(items) u AS position, item
                   |  WHERE u.item = $item
                   |)
                   |""".stripMargin),
            Seq(Row(expected))
          )
      }
      checkAnswer(
        sql("""SELECT id FROM VALUES (10), (30) AS outer_t(id)
              |WHERE EXISTS (
              |  SELECT 1 FROM VALUES (array(10, 20)) AS t(items)
              |  LATERAL VIEW udtf_pos_explode(items) u AS position, item
              |  WHERE u.item = outer_t.id
              |)
              |""".stripMargin),
        Seq(Row(10))
      )
    }
  }

  test("Paimon V1 Function: Hive UDTF in nested expression subqueries and CTEs") {
    withUserDefinedFunction("udtf_pos_explode" -> false) {
      sql(s"CREATE FUNCTION udtf_pos_explode AS '$hivePosExplode'")
      checkAnswer(
        sql("""SELECT (
              |  WITH expanded AS (
              |    SELECT (
              |      SELECT count(*) FROM VALUES (array(10, 20)) AS t(items)
              |      LATERAL VIEW udtf_pos_explode(items) u AS position, item
              |    ) AS n
              |  )
              |  SELECT n FROM expanded
              |) AS n
              |""".stripMargin),
        Seq(Row(2L))
      )
    }
  }

  test("Paimon V1 Function: lateral view preserves built-in and temporary function lookup") {
    withUserDefinedFunction("udtf_pos_explode" -> false) {
      sql(s"CREATE FUNCTION udtf_pos_explode AS '$hivePosExplode'")
      withUserDefinedFunction("udtf_pos_explode" -> true) {
        spark.sessionState.functionRegistry.createOrReplaceTempFunction(
          "udtf_pos_explode",
          children => Explode(children.head),
          "scala_udf")
        checkAnswer(
          sql("""SELECT item FROM VALUES (array(10, 20)) AS t(items)
                |LATERAL VIEW udtf_pos_explode(items) u AS item
                |""".stripMargin),
          Seq(Row(10), Row(20))
        )
        checkAnswer(
          sql("""SELECT position, item FROM VALUES (array(10, 20)) AS t(items)
                |LATERAL VIEW test.udtf_pos_explode(items) u AS position, item
                |""".stripMargin),
          Seq(Row(0, 10), Row(1, 20))
        )
        checkAnswer(
          sql("""SELECT position, item FROM VALUES (array(10, 20)) AS t(items)
                |LATERAL VIEW posexplode(items) u AS position, item
                |""".stripMargin),
          Seq(Row(0, 10), Row(1, 20))
        )
      }
    }
  }

  test("Paimon V1 Function: reject a scalar function in lateral view") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class' USING JAR '$testUDFJarPath'")
      val error = intercept[AnalysisException] {
        sql("""SELECT item FROM VALUES (1, 2) AS t(a, b)
              |LATERAL VIEW udf_add2(a, b) u AS item
              |""".stripMargin)
      }
      assert(error.getErrorClass == "UNSUPPORTED_GENERATOR.NOT_GENERATOR")
    }
  }

  test("Paimon V1 Function: create or replace function") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      checkAnswer(sql("SELECT udf_add2(1, 2)"), Seq(Row(3)))

      // create again should throw exception
      intercept[Exception] {
        sql(s"""
               |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
               |USING JAR '$testUDFJarPath'
               |""".stripMargin)
      }

      sql(s"""
             |CREATE FUNCTION IF NOT EXISTS udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)

      // create or replace
      sql(s"""
             |CREATE OR REPLACE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      checkAnswer(sql("SELECT udf_add2(3, 4)"), Seq(Row(7)))
    }
  }

  test("Paimon V1 Function: use function with catalog name and database name") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      checkAnswer(sql("SELECT udf_add2(3, 4)"), Seq(Row(7)))
      sql("DROP FUNCTION udf_add2")

      sql(s"""
             |CREATE FUNCTION test.udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      checkAnswer(sql("SELECT test.udf_add2(3, 4)"), Seq(Row(7)))
      sql("DROP FUNCTION test.udf_add2")

      sql(s"""
             |CREATE FUNCTION paimon.test.udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      checkAnswer(sql("SELECT paimon.test.udf_add2(3, 4)"), Seq(Row(7)))
      sql("DROP FUNCTION paimon.test.udf_add2")
    }
  }

  test("Paimon V1 Function: select with attribute") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      withTable("t") {
        sql("CREATE TABLE t (a INT, b INT)")
        sql("INSERT INTO t VALUES (1, 2), (3, 4)")
        checkAnswer(sql("SELECT udf_add2(a, b) FROM t"), Seq(Row(3), Row(7)))
      }
    }
  }

  test("Paimon V1 Function: default output column name excludes the catalog") {
    // The registry key is the fully qualified 3-part identifier, but the name handed to the
    // expression builder must stay 2-part, or an unaliased scalar/aggregate `SELECT udf(...)`
    // column gets renamed from `db.udf(...)` to `catalog.db.udf(...)`. See
    // `PaimonV1FunctionRegistry`'s `makeFunctionBuilder` for the mechanism.
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      // `dbName0`, not a literal: the expected name follows the session's current database, which
      // the test base sets. A literal would keep passing if that ever changed.
      assert(sql("SELECT udf_add2(3, 4)").columns.toSeq === Seq(s"$dbName0.udf_add2(3, 4)"))
    }
  }

  test("Paimon V1 Function: select with built-in function") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      withTable("t") {
        sql("CREATE TABLE t (a INT, pt INT) USING paimon PARTITIONED BY (pt)")
        sql("INSERT INTO t VALUES (1, 2), (3, 4)")
        checkAnswer(
          sql(
            "SELECT a, udf_add2(pow(a, pt), sys.max_pt('t')), pow(a, udf_add2(a, pt)) FROM t ORDER BY a"),
          Seq(Row(1, 5.0d, 1.0d), Row(3, 85.0d, 2187.0d))
        )
      }
    }
  }

  test("Paimon V1 Function: drop function") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      checkAnswer(sql("SELECT udf_add2(1, 2)"), Seq(Row(3)))

      sql("DROP FUNCTION udf_add2")
      intercept[Exception] {
        sql("SELECT udf_add2(3, 4)")
      }
      sql("DROP FUNCTION IF EXISTS udf_add2")
    }
  }

  test("Paimon V1 Function: describe function") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)

      checkAnswer(
        sql("DESCRIBE FUNCTION udf_add2"),
        Seq(Row("Function: test.udf_add2"), Row(s"Class: $UDFExampleAdd2Class"))
      )
    }
  }

  test("Paimon V1 Function: unsupported operation") {
    // create a built-in function
    assert(intercept[Exception] {
      sql(s"""
             |CREATE FUNCTION sys.max_pt AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
    }.getMessage.contains("Can't create built-in function"))

    // drop a built-in function
    assert(intercept[Exception] {
      sql("DROP FUNCTION sys.max_pt")
    }.getMessage.contains("Can't drop built-in function"))
  }

  test("Paimon V1 Function: user defined aggregate function") {
    for (isTemp <- Seq(true, false)) {
      withUserDefinedFunction("myIntSum" -> isTemp) {
        if (isTemp) {
          sql(s"CREATE TEMPORARY FUNCTION myIntSum AS '$MyIntSumClass'")
        } else {
          sql(s"CREATE FUNCTION myIntSum AS '$MyIntSumClass'")
        }
        withTable("t") {
          sql("CREATE TABLE t (id INT) USING paimon")
          sql("INSERT INTO t VALUES (1), (2), (3)")
          checkAnswer(sql("SELECT myIntSum(id) FROM t"), Row(6))
        }
      }
    }
  }

  test("Paimon V1 Function: select with CTE and subquery") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      withTable("t") {
        sql("CREATE TABLE t (a INT, b INT)")
        sql("INSERT INTO t VALUES (1, 2), (3, 4)")

        checkAnswer(
          sql("""
                |WITH tmp_view AS (
                |  SELECT udf_add2(a, b) AS c1 FROM t
                |)
                |SELECT * FROM tmp_view
                |""".stripMargin),
          Seq(Row(3), Row(7))
        )

        checkAnswer(
          sql("""
                |WITH tmp_view AS (
                |  SELECT udf_add2(1, 2)
                |)
                |SELECT * FROM tmp_view
                |""".stripMargin),
          Seq(Row(3))
        )

        checkAnswer(
          sql("""
                |SELECT * FROM (SELECT udf_add2(a, b) AS c1 FROM t)
                |""".stripMargin),
          Seq(Row(3), Row(7))
        )
      }
    }
  }

  test("Paimon V1 Function: select with view") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      withTable("t") {
        withView("v") {
          sql("CREATE TABLE t (a INT, b INT)")
          sql("INSERT INTO t VALUES (1, 2), (3, 4)")
          sql("CREATE VIEW v AS SELECT udf_add2(a, b) AS c1 FROM t")
          checkAnswer(sql("SELECT * FROM v"), Seq(Row(3), Row(7)))
        }
      }
    }
  }

  test("Paimon V1 Function: create or drop function on an existing temporary function") {
    withUserDefinedFunction("udf_add2" -> true) {
      sql(s"""
             |CREATE TEMPORARY FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)

      assert(intercept[Exception] {
        sql(s"""
               |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
               |USING JAR '$testUDFJarPath'
               |""".stripMargin)
      }.getMessage.contains("udf_add2 is a temporary function and already exists"))

      assert(intercept[Exception] {
        sql(s"""
               |CREATE OR REPLACE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
               |USING JAR '$testUDFJarPath'
               |""".stripMargin)
      }.getMessage.contains(
        "udf_add2 is a temporary function, you should use `CREATE OR REPLACE TEMPORARY FUNCTION udf_add2`"))

      sql(s"""
             |CREATE OR REPLACE TEMPORARY FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)

      // Spark reworded this in 4.2: `error-conditions.json` dropped the "built-in/temporary"
      // phrasing in favour of pointing at DROP TEMPORARY FUNCTION. Both mean the same refusal, and
      // this suite runs on every supported version, so accept either.
      val dropTempMessage = intercept[Exception] {
        sql(s"""
               |DROP FUNCTION udf_add2
               |""".stripMargin)
      }.getMessage
      assert(
        dropTempMessage.contains("udf_add2 is a built-in/temporary function") ||
          dropTempMessage.contains("'DROP FUNCTION' expects a persistent function"),
        s"unexpected DROP FUNCTION refusal: $dropTempMessage"
      )
    }
  }

  test("Paimon V1 Function: COPY INTO location FROM (SELECT udf(...))") {
    withUserDefinedFunction("udf_add2" -> false) {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
      withTable("t") {
        sql("CREATE TABLE t (a INT, b INT)")
        sql("INSERT INTO t VALUES (1, 2), (3, 4)")

        withTempDir {
          dir =>
            val exportPath = new File(dir, "udf_export").getAbsolutePath
            // The inline query references a Paimon v1 function. This only resolves if the query is
            // parsed through the session (Paimon) parser, which applies the v1 function rewrite.
            val result = sql(s"""
                                |COPY INTO '$exportPath'
                                |FROM (SELECT udf_add2(a, b) AS c FROM t)
                                |FILE_FORMAT = (TYPE = CSV)
                                |""".stripMargin)
            checkAnswer(result.selectExpr("rows_written"), Row(2L))
            checkAnswer(
              spark.read.csv(exportPath).selectExpr("CAST(_c0 AS INT)"),
              Seq(Row(3), Row(7)))
        }
      }
    }
  }
}

class DisablePaimonV1FunctionTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.v1Function.enabled", "false")
  }

  test("Paimon V1 Function: disable paimon v1 function") {
    intercept[Exception] {
      sql(s"""
             |CREATE FUNCTION udf_add2 AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
    }
  }
}
