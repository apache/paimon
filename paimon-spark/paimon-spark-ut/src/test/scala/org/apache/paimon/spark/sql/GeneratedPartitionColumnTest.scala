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

import org.apache.spark.sql.Row

import java.sql.Date

/** SQL and catalog contracts for physical generated columns. */
class GeneratedPartitionColumnTest extends PaimonSparkTestBase {

  test("resolve quoted and case-insensitive source columns") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (
            |id INT, `event time` TIMESTAMP_NTZ,
            |dt DATE GENERATED ALWAYS AS (CAST(`EVENT TIME` AS DATE))
            |) USING paimon PARTITIONED BY (dt)""".stripMargin)
      sql("""INSERT INTO generated_events (`event time`, id)
            |VALUES (CAST('2026-10-30 12:00:00' AS TIMESTAMP_NTZ), 1)""".stripMargin)
      checkAnswer(
        sql("SELECT id, dt FROM generated_events"),
        Seq(Row(1, Date.valueOf("2026-10-30"))))
    }
  }

  for (mode <- Seq("primary-key", "data-evolution")) {
    test(s"generated partition insert for $mode tables") {
      assume(gteqSpark3_5)
      withTable("generated_events") {
        val properties = if (mode == "primary-key") {
          "'primary-key'='id,dt', 'bucket'='1'"
        } else {
          "'row-tracking.enabled'='true', 'data-evolution.enabled'='true'"
        }
        sql(s"""CREATE TABLE generated_events (
               |id INT, event_time TIMESTAMP_NTZ,
               |dt DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
               |) USING paimon PARTITIONED BY (dt) TBLPROPERTIES ($properties)""".stripMargin)
        sql("""INSERT INTO generated_events (id, event_time)
              |VALUES (1, CAST('2026-10-30 12:00:00' AS TIMESTAMP_NTZ))""".stripMargin)
        checkAnswer(
          sql("SELECT id, dt FROM generated_events"),
          Seq(Row(1, Date.valueOf("2026-10-30"))))
      }
    }
  }

  test("generate omitted partition columns before routing") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""
            |CREATE TABLE generated_events (
            |  id INT,
            |  event_time TIMESTAMP_NTZ,
            |  dt DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
            |) USING paimon PARTITIONED BY (dt)
            |""".stripMargin)
      assert(
        loadTable("generated_events").rowType().getField("dt").generationExpression() ==
          "CAST(event_time AS DATE)")

      sql("""INSERT INTO generated_events
            |SELECT 1, CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP_NTZ)
            |""".stripMargin)
      sql("""INSERT INTO generated_events (id, event_time)
            |VALUES (2, CAST('2026-10-30 00:00:00' AS TIMESTAMP_NTZ))
            |""".stripMargin)
      sql("""INSERT INTO generated_events
            |VALUES (3, CAST('2026-10-31 12:00:00' AS TIMESTAMP_NTZ), DATE '2026-10-31')
            |""".stripMargin)
      checkAnswer(
        sql("SELECT id, dt FROM generated_events"),
        Seq(
          Row(1, Date.valueOf("1969-12-31")),
          Row(2, Date.valueOf("2026-10-30")),
          Row(3, Date.valueOf("2026-10-31")))
      )

      sql("ALTER TABLE generated_events ALTER COLUMN dt COMMENT 'generated partition'")
      assert(
        loadTable("generated_events").rowType().getField("dt").generationExpression() ==
          "CAST(event_time AS DATE)")
    }
  }

  test("reject inconsistent generated partition values without committing rows") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (
            |id INT, event_time TIMESTAMP_NTZ,
            |dt DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
            |) USING paimon PARTITIONED BY (dt)""".stripMargin)
      for (partition <- Seq("DATE '2026-10-31'", "CAST(NULL AS DATE)")) {
        val error = intercept[Throwable] {
          sql(s"""INSERT INTO generated_events
                 |VALUES (1, CAST('2026-10-30 12:00:00' AS TIMESTAMP_NTZ), $partition)
                 |""".stripMargin)
        }
        val messages = Iterator
          .iterate(error)(_.getCause)
          .takeWhile(_ != null)
          .map(_.getMessage)
          .mkString("\n")
        assert(messages.contains("does not match generation expression"))
        checkAnswer(sql("SELECT id FROM generated_events"), Seq.empty)
      }
      sql("INSERT INTO generated_events (id, event_time) VALUES (2, NULL)")
      checkAnswer(sql("SELECT id, dt FROM generated_events"), Seq(Row(2, null)))
    }
  }

  test("generate non-partition columns with transforms and nested scalar calls") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (
            |id INT, name STRING, event_time TIMESTAMP_NTZ, days INT,
            |label STRING GENERATED ALWAYS AS (CONCAT(UPPER(`NAME`), '-', SUBSTRING(name, 2, 2))),
            |event_year INT GENERATED ALWAYS AS (YEAR(event_time)),
            |dt DATE GENERATED ALWAYS AS (DATE_ADD(CAST(event_time AS DATE), days))
            |) USING paimon PARTITIONED BY (dt)""".stripMargin)
      sql("""INSERT INTO generated_events (days, event_time, name, id)
            |VALUES (2, CAST('1969-12-31 12:00:00' AS TIMESTAMP_NTZ), 'abc', 1)""".stripMargin)
      checkAnswer(
        sql("SELECT id, label, event_year, dt FROM generated_events"),
        Seq(Row(1, "ABC-bc", 1969, Date.valueOf("1970-01-02"))))
      val error = intercept[Throwable] {
        sql("""INSERT INTO generated_events
              |VALUES (2, 'abc', CAST('1969-12-31 12:00:00' AS TIMESTAMP_NTZ), 2,
              |'wrong', 1969, DATE '1970-01-02')""".stripMargin)
      }
      assert(
        Iterator
          .iterate(error)(_.getCause)
          .takeWhile(_ != null)
          .exists(
            e => Option(e.getMessage).exists(_.contains("does not match generation expression"))))
      checkAnswer(sql("SELECT id FROM generated_events"), Seq(Row(1)))
    }
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (name STRING,
            |label STRING GENERATED ALWAYS AS (LOWER(name))) USING paimon""".stripMargin)
      sql("INSERT INTO generated_events VALUES ('AbC')")
      checkAnswer(sql("SELECT label FROM generated_events"), Seq(Row("abc")))
    }
  }

  test("reject nested references and unsupported generation functions") {
    assume(gteqSpark3_5)
    for (
      (expression, dataType, expected) <- Seq(
        ("ABS(id)", "INT", "Unsupported generation"),
        ("RAND()", "DOUBLE", "not deterministic"),
        ("UPPER(nested.name)", "STRING", "top-level field"))
    ) {
      withTable("generated_events") {
        val error = intercept[Throwable] {
          sql(s"""CREATE TABLE generated_events (
                 |id INT, nested STRUCT<name: STRING>,
                 |value $dataType GENERATED ALWAYS AS ($expression)
                 |) USING paimon""".stripMargin)
        }
        assert(error.getMessage.contains(expected))
      }
    }
  }
}
