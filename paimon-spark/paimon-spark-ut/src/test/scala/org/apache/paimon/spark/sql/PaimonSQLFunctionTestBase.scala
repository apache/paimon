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

import org.apache.spark.sql.Row

abstract class PaimonSQLFunctionTestBase extends PaimonSparkTestWithRestCatalogBase {

  test("Paimon SQL Function: scalar function with expression body") {
    withUserDefinedFunction("area" -> false) {
      sql("""
            |CREATE FUNCTION area(width DOUBLE, height DOUBLE)
            |RETURNS DOUBLE
            |RETURN width * height
            |""".stripMargin)
      checkAnswer(sql("SELECT area(2, 3)"), Row(6.0))

      withTable("t") {
        sql("CREATE TABLE t (a DOUBLE, b DOUBLE)")
        sql("INSERT INTO t VALUES (1, 2), (3, 4)")
        checkAnswer(sql("SELECT area(a, b) FROM t ORDER BY a"), Seq(Row(2.0), Row(12.0)))
      }
    }
  }

  test("Paimon SQL Function: create with qualified name and call across catalog") {
    withUserDefinedFunction("area" -> false) {
      sql("""
            |CREATE FUNCTION test.area(width DOUBLE, height DOUBLE)
            |RETURNS DOUBLE
            |RETURN width * height
            |""".stripMargin)
      checkAnswer(sql("SELECT test.area(2, 3)"), Row(6.0))
      checkAnswer(sql("SELECT paimon.test.area(2, 4)"), Row(8.0))
    }
  }

  test("Paimon SQL Function: create or replace / if not exists") {
    withUserDefinedFunction("inc" -> false) {
      sql("CREATE FUNCTION inc(x INT) RETURNS INT RETURN x + 1")
      checkAnswer(sql("SELECT inc(10)"), Row(11))

      // create again should fail
      intercept[Exception] {
        sql("CREATE FUNCTION inc(x INT) RETURNS INT RETURN x + 1")
      }

      // if not exists: no-op, keeps the old definition
      sql("CREATE FUNCTION IF NOT EXISTS inc(x INT) RETURNS INT RETURN x + 100")
      checkAnswer(sql("SELECT inc(10)"), Row(11))

      // or replace: new definition takes effect
      sql("CREATE OR REPLACE FUNCTION inc(x INT) RETURNS INT RETURN x + 100")
      checkAnswer(sql("SELECT inc(10)"), Row(110))
    }
  }

  test("Paimon SQL Function: scalar function with query body referencing a table") {
    withUserDefinedFunction("dept_total" -> false) {
      withTable("emp") {
        sql("CREATE TABLE emp (id INT, dept_id INT, salary INT)")
        sql("INSERT INTO emp VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)")
        sql("""
              |CREATE FUNCTION dept_total(d INT) RETURNS INT
              |RETURN SELECT SUM(salary) FROM emp WHERE dept_id = d
              |""".stripMargin)
        checkAnswer(sql("SELECT dept_total(10)"), Row(300))
        checkAnswer(sql("SELECT dept_total(20)"), Row(300))
      }
    }
  }

  test("Paimon SQL Function: parameter with DEFAULT value") {
    withUserDefinedFunction("addd" -> false) {
      sql("CREATE FUNCTION addd(x INT, y INT DEFAULT 10) RETURNS INT RETURN x + y")
      checkAnswer(sql("SELECT addd(5)"), Row(15))
      checkAnswer(sql("SELECT addd(5, 1)"), Row(6))
    }
  }

  test("Paimon SQL Function: describe function") {
    withUserDefinedFunction("area" -> false) {
      sql("CREATE FUNCTION area(width DOUBLE, height DOUBLE) RETURNS DOUBLE RETURN width * height")

      val desc = sql("DESCRIBE FUNCTION area").collect().map(_.getString(0))
      assert(desc.exists(_.contains("Type: SCALAR")), desc.mkString("\n"))
      assert(desc.exists(_.contains("Input:")), desc.mkString("\n"))
      assert(desc.exists(_.contains("width")), desc.mkString("\n"))
      assert(desc.exists(_.contains("Returns: DOUBLE")), desc.mkString("\n"))

      val descExt = sql("DESCRIBE FUNCTION EXTENDED area").collect().map(_.getString(0))
      assert(descExt.exists(_.contains("width * height")), descExt.mkString("\n"))
    }
  }

  test("Paimon SQL Function: show functions lists the created function") {
    withUserDefinedFunction("area" -> false) {
      sql("CREATE FUNCTION area(w DOUBLE, h DOUBLE) RETURNS DOUBLE RETURN w * h")
      val names = sql("SHOW USER FUNCTIONS").collect().map(_.getString(0).toLowerCase)
      assert(names.exists(_.contains("area")), names.mkString(", "))
    }
  }

  test("Paimon SQL Function: drop function") {
    withUserDefinedFunction("area" -> false) {
      sql("CREATE FUNCTION area(w DOUBLE, h DOUBLE) RETURNS DOUBLE RETURN w * h")
      checkAnswer(sql("SELECT area(2, 3)"), Row(6.0))

      sql("DROP FUNCTION area")
      intercept[Exception] {
        sql("SELECT area(2, 3)")
      }
      sql("DROP FUNCTION IF EXISTS area")
    }
  }

  test("Paimon SQL Function: table function is not supported yet") {
    val e = intercept[Exception] {
      sql("""
            |CREATE FUNCTION rows_of(x INT)
            |RETURNS TABLE(a INT)
            |RETURN SELECT x + 1 AS a
            |""".stripMargin)
    }
    assert(e.getMessage.contains("does not support creating SQL table functions"))
  }
}
