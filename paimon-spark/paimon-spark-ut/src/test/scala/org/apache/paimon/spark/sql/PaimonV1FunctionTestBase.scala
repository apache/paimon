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
import org.apache.spark.sql.Row

abstract class PaimonV1FunctionTestBase extends PaimonSparkTestWithRestCatalogBase {

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

  test("Paimon V1 Function: select with build-in function") {
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
    // create a build-in function
    assert(intercept[Exception] {
      sql(s"""
             |CREATE FUNCTION sys.max_pt AS '$UDFExampleAdd2Class'
             |USING JAR '$testUDFJarPath'
             |""".stripMargin)
    }.getMessage.contains("Can't create build-in function"))

    // drop a build-in function
    assert(intercept[Exception] {
      sql("DROP FUNCTION sys.max_pt")
    }.getMessage.contains("Can't drop build-in function"))
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

      assert(intercept[Exception] {
        sql(s"""
               |DROP FUNCTION udf_add2
               |""".stripMargin)
      }.getMessage.contains("udf_add2 is a built-in/temporary function"))
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
