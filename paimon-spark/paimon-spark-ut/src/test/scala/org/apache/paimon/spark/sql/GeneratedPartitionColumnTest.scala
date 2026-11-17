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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.{DataTypes => PaimonTypes}

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

  test("preserve supplied generated partition values and explicit nulls") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (
            |id INT, event_time TIMESTAMP_NTZ,
            |dt DATE GENERATED ALWAYS AS (CAST(event_time AS DATE))
            |) USING paimon PARTITIONED BY (dt)""".stripMargin)
      for ((partition, id) <- Seq("DATE '2026-10-31'", "CAST(NULL AS DATE)").zipWithIndex) {
        sql(s"""INSERT INTO generated_events
               |VALUES ($id, CAST('2026-10-30 12:00:00' AS TIMESTAMP_NTZ), $partition)
               |""".stripMargin)
      }
      sql("INSERT INTO generated_events (id, event_time) VALUES (2, NULL)")
      checkAnswer(
        sql("SELECT id, dt FROM generated_events"),
        Seq(Row(0, Date.valueOf("2026-10-31")), Row(1, null), Row(2, null)))
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
      sql("""INSERT INTO generated_events
            |VALUES (2, 'abc', CAST('1969-12-31 12:00:00' AS TIMESTAMP_NTZ), 2,
            |'wrong', 1969, DATE '1970-01-02')""".stripMargin)
      checkAnswer(
        sql("SELECT id, label FROM generated_events"),
        Seq(Row(1, "ABC-bc"), Row(2, "wrong")))
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

  test("canonicalize quoted and escaped string literals in generation expressions") {
    assume(gteqSpark3_5)
    for (literal <- Seq("'NAME''s'", """'C:\\path'""", """'line\nend'""", """'quote\'end'""")) {
      withTable("generated_events") {
        sql(
          s"""CREATE TABLE generated_events (name STRING,
             |label STRING GENERATED ALWAYS AS (CONCAT(name, $literal))) USING paimon""".stripMargin)
        sql("INSERT INTO generated_events (name) VALUES ('ab')")
        checkAnswer(
          sql("SELECT label FROM generated_events"),
          Seq(sql(s"SELECT CONCAT('ab', $literal)").head()))
      }
    }
  }

  test("use storage transforms and defaults for omitted generated values") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (id INT, ts TIMESTAMP_NTZ, name STRING,
            |text STRING GENERATED ALWAYS AS (CAST(ts AS STRING)),
            |trimmed STRING GENERATED ALWAYS AS (TRIM('x', name)),
            |literal STRING GENERATED ALWAYS AS (CONCAT(name, 'NAME''s'))
            |) USING paimon""".stripMargin)
      sql("""INSERT INTO generated_events (id, ts, name)
            |VALUES (1, CAST('1970-01-01 00:00:00' AS TIMESTAMP_NTZ), 'xabcx')""".stripMargin)
      sql("""INSERT INTO generated_events VALUES
            |(2, CAST('1970-01-01 00:00:00' AS TIMESTAMP_NTZ), 'xabcx',
            |'1970-01-01 00:00:00.000000', 'abc', CONCAT('xabcx', 'NAME''s'))""".stripMargin)
      val suffix = sql("SELECT 'NAME''s'").head().getString(0)
      checkAnswer(
        sql("SELECT text, trimmed, literal FROM generated_events"),
        Seq.fill(2)(Row("1970-01-01 00:00:00.000000", "abc", "xabcx" + suffix)))
    }
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (id INT, name STRING DEFAULT 'abc',
            |label STRING GENERATED ALWAYS AS (UPPER(name))) USING paimon""".stripMargin)
      sql("INSERT INTO generated_events (id, name) VALUES (1, NULL)")
      sql("INSERT INTO generated_events VALUES (2, NULL, 'ABC')")
      checkAnswer(
        sql("SELECT id, name, label FROM generated_events"),
        Seq(Row(1, "abc", "ABC"), Row(2, "abc", "ABC")))
    }
  }

  test("skip supplied expressions when generating other omitted columns") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (id INT, text STRING,
            |number INT GENERATED ALWAYS AS (CAST(text AS INT)),
            |label STRING GENERATED ALWAYS AS (UPPER(text))) USING paimon""".stripMargin)
      sql("INSERT INTO generated_events (id, text, number) VALUES (1, 'invalid', 42)")
      sql("INSERT INTO generated_events VALUES (2, 'invalid', NULL, NULL)")
      checkAnswer(
        sql("SELECT id, number, label FROM generated_events"),
        Seq(Row(1, 42, "INVALID"), Row(2, null, null)))
    }
  }

  test("preserve extra columns and generation metadata when merging schemas") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      sql("""CREATE TABLE generated_events (id INT, name STRING,
            |label STRING GENERATED ALWAYS AS (UPPER(name))) USING paimon""".stripMargin)
      sql("SELECT 1 AS id, 'ab' AS name, 99 AS extra")
        .writeTo("generated_events")
        .option("write.merge-schema", "true")
        .append()
      sql("REFRESH TABLE generated_events")
      sql("SELECT 2 AS id, 'cd' AS name, 'CD' AS label, 100 AS extra, 7 AS other")
        .writeTo("generated_events")
        .option("write.merge-schema", "true")
        .append()
      sql("REFRESH TABLE generated_events")
      assert(
        loadTable("generated_events").rowType().getField("label").generationExpression() != null)
      checkAnswer(
        sql("SELECT id, label, extra, other FROM generated_events"),
        Seq(Row(1, "AB", 99, null), Row(2, "CD", 100, 7)))
    }
  }

  test("reject generated results Spark cannot represent without losing nanoseconds") {
    assume(gteqSpark3_5)
    val timestamp = PaimonTypes.TIMESTAMP(9)
    for (
      (sourceType, targetType, expression, value) <- Seq(
        (
          PaimonTypes.STRING(),
          timestamp,
          "CAST(source AS TIMESTAMP(9))",
          "'2026-01-01 00:00:00.123456789'"),
        (
          PaimonTypes.ARRAY(timestamp),
          PaimonTypes.ARRAY(timestamp),
          "source",
          "array(CAST('2026-01-01 00:00:00' AS TIMESTAMP_NTZ))")
      )
    ) {
      withTable("generated_events") {
        paimonCatalog.createTable(
          Identifier.create(dbName0, "generated_events"),
          Schema
            .newBuilder()
            .column("source", sourceType)
            .generatedColumn("generated", targetType, expression)
            .build(),
          false
        )
        val error = intercept[Throwable] {
          sql(s"INSERT INTO generated_events (source) SELECT $value")
        }
        assert(error.getMessage.contains("TIMESTAMP precision greater than 6"))
        assert(loadTable("generated_events").snapshotManager().latestSnapshotId() == null)
        sql(s"INSERT INTO generated_events (source, generated) SELECT $value, $value")
        checkAnswer(sql("SELECT generated = source FROM generated_events"), Seq(Row(true)))
      }
    }
  }

  test("reject generated BLOB results before writing through a lossy Spark representation") {
    assume(gteqSpark3_5)
    withTable("generated_events") {
      paimonCatalog.createTable(
        Identifier.create(dbName0, "generated_events"),
        Schema
          .newBuilder()
          .column("id", PaimonTypes.INT())
          .column("source", PaimonTypes.BLOB())
          .generatedColumn("generated", PaimonTypes.BLOB(), "source")
          .option("row-tracking.enabled", "true")
          .option("data-evolution.enabled", "true")
          .build(),
        false
      )
      val error = intercept[Throwable] {
        sql("INSERT INTO generated_events (id, source) SELECT 1, unhex('010203')")
      }
      assert(error.getMessage.contains("BLOB results"))
      assert(loadTable("generated_events").snapshotManager().latestSnapshotId() == null)
      sql("INSERT INTO generated_events VALUES (1, unhex('010203'), unhex('aabb'))")
      checkAnswer(sql("SELECT hex(generated) FROM generated_events"), Seq(Row("AABB")))
    }
  }

  test("preserve supplied and untouched generated values in row-tracking and DE updates") {
    assume(gteqSpark3_5)
    for (dataEvolution <- Seq(false, true)) {
      withTable("generated_events") {
        sql(s"""CREATE TABLE generated_events (id INT, name STRING,
               |label STRING GENERATED ALWAYS AS (UPPER(name))) USING paimon
               |TBLPROPERTIES ('row-tracking.enabled'='true',
               |'data-evolution.enabled'='$dataEvolution')""".stripMargin)
        sql("INSERT INTO generated_events (id, name) VALUES (1, 'old'), (2, 'other')")
        sql("UPDATE generated_events SET name='new' WHERE id=1")
        checkAnswer(
          sql("SELECT id, name, label FROM generated_events"),
          Seq(Row(1, "new", "OLD"), Row(2, "other", "OTHER")))
        sql("UPDATE generated_events SET label='external' WHERE id=1")
        checkAnswer(
          sql("SELECT id, name, label FROM generated_events"),
          Seq(Row(1, "new", "external"), Row(2, "other", "OTHER")))
        sql("UPDATE generated_events SET name='changed', label=NULL WHERE id=1")
        checkAnswer(
          sql("SELECT id, name, label FROM generated_events"),
          Seq(Row(1, "changed", null), Row(2, "other", "OTHER")))
      }
    }
  }
}
