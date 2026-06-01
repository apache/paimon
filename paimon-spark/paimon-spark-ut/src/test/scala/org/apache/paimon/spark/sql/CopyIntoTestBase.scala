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

import java.io.{File, PrintWriter}
import java.nio.file.Files

class CopyIntoTestBase extends PaimonSparkTestBase {

  protected def createCsvFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    val writer = new PrintWriter(file)
    try writer.write(content)
    finally writer.close()
    file
  }

  protected def withCsvDir(testBody: File => Unit): Unit = {
    val dir = Files.createTempDirectory("copy_into_test").toFile
    try testBody(dir)
    finally deleteRecursively(dir)
  }

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  test("COPY INTO: basic CSV import") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_basic")
    spark.sql(s"CREATE TABLE $dbName0.copy_basic (id INT, name STRING, age INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice,30\n2,Bob,25\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_basic
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |""".stripMargin)

        assert(result.collect().length > 0)
        val row = result.collect()(0)
        assert(row.getString(1) == "LOADED")

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_basic ORDER BY id"),
          Seq(Row(1, "Alice", 30), Row(2, "Bob", 25)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_basic")
  }

  test("COPY INTO: CSV with custom delimiter") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_delim")
    spark.sql(s"CREATE TABLE $dbName0.copy_delim (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1|Alice\n2|Bob\n")

        spark.sql(s"""COPY INTO $dbName0.copy_delim
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = '|')
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_delim ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_delim")
  }

  test("COPY INTO: CSV with SKIP_HEADER") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_header")
    spark.sql(s"CREATE TABLE $dbName0.copy_header (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "id,name\n1,Alice\n2,Bob\n")

        spark.sql(s"""COPY INTO $dbName0.copy_header
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_header ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_header")
  }

  test("COPY INTO: CSV with quote and escape") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_quote")
    spark.sql(s"CREATE TABLE $dbName0.copy_quote (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,\"Alice \"\"the great\"\"\"\n2,\"Bob\"\n")

        spark.sql(s"""COPY INTO $dbName0.copy_quote
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV, QUOTE = '"', ESCAPE = '"')
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_quote ORDER BY id"),
          Seq(Row(1, "Alice \"the great\""), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_quote")
  }

  test("COPY INTO: NULL_IF with multiple values") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_null")
    spark.sql(s"CREATE TABLE $dbName0.copy_null (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,NULL\n2,\\N\n3,Alice\n")

        spark.sql(s"""COPY INTO $dbName0.copy_null
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV, NULL_IF = ('NULL', '\\N'))
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_null ORDER BY id"),
          Seq(Row(1, null), Row(2, null), Row(3, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_null")
  }

  test("COPY INTO: EMPTY_FIELD_AS_NULL") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_empty")
    spark.sql(s"CREATE TABLE $dbName0.copy_empty (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,\n2,Alice\n")

        spark.sql(s"""COPY INTO $dbName0.copy_empty
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV, EMPTY_FIELD_AS_NULL = TRUE)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_empty ORDER BY id"),
          Seq(Row(1, null), Row(2, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_empty")
  }

  test("COPY INTO: PATTERN filters files") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_pattern")
    spark.sql(s"CREATE TABLE $dbName0.copy_pattern (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data1.csv", "1,Alice\n")
        createCsvFile(dir, "data2.csv", "2,Bob\n")
        createCsvFile(dir, "ignore.txt", "3,Charlie\n")

        spark.sql(s"""COPY INTO $dbName0.copy_pattern
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |PATTERN = '.*\\.csv'
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_pattern ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_pattern")
  }

  test("COPY INTO: explicit column list") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_cols")
    spark.sql(s"CREATE TABLE $dbName0.copy_cols (id INT, name STRING, age INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n2,Bob\n")

        spark.sql(s"""COPY INTO $dbName0.copy_cols (id, name)
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_cols ORDER BY id"),
          Seq(Row(1, "Alice", null), Row(2, "Bob", null)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_cols")
  }

  test("COPY INTO: unsupported TYPE errors at validation time") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_unsup")
    spark.sql(s"CREATE TABLE $dbName0.copy_unsup (id INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1\n")

        val e = intercept[IllegalArgumentException] {
          spark.sql(s"""COPY INTO $dbName0.copy_unsup
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = ORC)
                       |""".stripMargin)
        }
        assert(
          e.getMessage.contains("Unsupported file format type") ||
            e.getMessage.contains("Supported types"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_unsup")
  }

  test("COPY INTO: missing TYPE errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_notype")
    spark.sql(s"CREATE TABLE $dbName0.copy_notype (id INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1\n")

        val e = intercept[IllegalArgumentException] {
          spark.sql(s"""COPY INTO $dbName0.copy_notype
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (FIELD_DELIMITER = ',')
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("FILE_FORMAT must include TYPE"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_notype")
  }

  test("COPY INTO: duplicate option key errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_dup")
    spark.sql(s"CREATE TABLE $dbName0.copy_dup (id INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1\n")

        val e = intercept[IllegalArgumentException] {
          spark.sql(s"""COPY INTO $dbName0.copy_dup
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV, TYPE = CSV)
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("Duplicate FILE_FORMAT option"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_dup")
  }

  test("COPY INTO: export table to CSV") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export")
    spark.sql(s"CREATE TABLE $dbName0.copy_export (id INT, name STRING)")
    spark.sql(s"INSERT INTO $dbName0.copy_export VALUES (1, 'Alice'), (2, 'Bob')")

    withCsvDir {
      dir =>
        val outputPath = new File(dir, "output").getAbsolutePath

        val result = spark.sql(s"""COPY INTO '$outputPath'
                                  |FROM $dbName0.copy_export
                                  |FILE_FORMAT = (TYPE = CSV, HEADER = TRUE)
                                  |""".stripMargin)

        val row = result.collect()(0)
        assert(row.getString(0) == outputPath)
        assert(row.getLong(2) == 2L)

        // Verify exported data
        val exported = spark.read.option("header", "true").csv(outputPath)
        assert(exported.count() == 2)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export")
  }

  test("COPY INTO: export OVERWRITE=FALSE fails on existing path") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export_fail")
    spark.sql(s"CREATE TABLE $dbName0.copy_export_fail (id INT)")
    spark.sql(s"INSERT INTO $dbName0.copy_export_fail VALUES (1)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "existing.csv", "data\n")

        intercept[Exception] {
          spark.sql(s"""COPY INTO '${dir.getAbsolutePath}'
                       |FROM $dbName0.copy_export_fail
                       |FILE_FORMAT = (TYPE = CSV)
                       |""".stripMargin)
        }
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export_fail")
  }

  test("COPY INTO: export OVERWRITE=TRUE succeeds") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export_ow")
    spark.sql(s"CREATE TABLE $dbName0.copy_export_ow (id INT, name STRING)")
    spark.sql(s"INSERT INTO $dbName0.copy_export_ow VALUES (1, 'Alice')")

    withCsvDir {
      dir =>
        createCsvFile(dir, "existing.csv", "old data\n")

        val result = spark.sql(s"""COPY INTO '${dir.getAbsolutePath}'
                                  |FROM $dbName0.copy_export_ow
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |OVERWRITE = TRUE
                                  |""".stripMargin)

        assert(result.collect()(0).getLong(2) == 1L)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export_ow")
  }

  test("COPY INTO: FORCE=FALSE skips already-loaded files") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_force")
    spark.sql(s"CREATE TABLE $dbName0.copy_force (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n2,Bob\n")

        // First load
        spark.sql(s"""COPY INTO $dbName0.copy_force
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |""".stripMargin)

        // Second load with FORCE=FALSE (default) should skip
        val result = spark.sql(s"""COPY INTO $dbName0.copy_force
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |FORCE = FALSE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "SKIPPED")

        // Table should still have only original 2 rows
        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_force ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_force")
  }

  test("COPY INTO: FORCE=TRUE reloads already-loaded files") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_force_true")
    spark.sql(s"CREATE TABLE $dbName0.copy_force_true (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n")

        // First load
        spark.sql(s"""COPY INTO $dbName0.copy_force_true
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |""".stripMargin)

        // Second load with FORCE=TRUE should re-import
        val result = spark.sql(s"""COPY INTO $dbName0.copy_force_true
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |FORCE = TRUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "LOADED")

        // Table should have duplicated data (2 rows total)
        assert(spark.sql(s"SELECT * FROM $dbName0.copy_force_true").count() == 2)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_force_true")
  }

  test("COPY INTO: bad numeric cast fails with ABORT_STATEMENT") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_badcast")
    spark.sql(s"CREATE TABLE $dbName0.copy_badcast (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "abc,Alice\n")

        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_badcast
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV)
                       |""".stripMargin)
        }
        val msg = e.getMessage
        assert(
          msg.contains("Cast failure") ||
            msg.contains("ABORT_STATEMENT") ||
            msg.contains("CAST_INVALID_INPUT") ||
            msg.contains("cannot be cast to") ||
            e.getCause != null)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_badcast")
  }

  test("COPY INTO: no namespace table works") {
    spark.sql(s"DROP TABLE IF EXISTS copy_no_ns")
    spark.sql(s"CREATE TABLE copy_no_ns (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n")

        spark.sql(s"""COPY INTO copy_no_ns
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |""".stripMargin)

        checkAnswer(spark.sql(s"SELECT * FROM copy_no_ns"), Seq(Row(1, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS copy_no_ns")
  }

  test("COPY INTO: lowercase options are accepted") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_lcase")
    spark.sql(s"CREATE TABLE $dbName0.copy_lcase (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1|Alice\n")

        spark.sql(s"""COPY INTO $dbName0.copy_lcase
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (type = csv, field_delimiter = '|')
                     |""".stripMargin)

        checkAnswer(spark.sql(s"SELECT * FROM $dbName0.copy_lcase"), Seq(Row(1, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_lcase")
  }

  test("COPY INTO: unknown option key errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_unknown_opt")
    spark.sql(s"CREATE TABLE $dbName0.copy_unknown_opt (id INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1\n")

        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_unknown_opt
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV, BOGUS_OPTION = TRUE)
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("Unsupported FILE_FORMAT options"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_unknown_opt")
  }

  test("COPY INTO: SKIP_HEADER > 1 errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skip2")
    spark.sql(s"CREATE TABLE $dbName0.copy_skip2 (id INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "h1\nh2\n1\n")

        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_skip2
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 2)
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("SKIP_HEADER supports only 0 or 1"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skip2")
  }

  test("COPY INTO: export rejects import-only options") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export_bad")
    spark.sql(s"CREATE TABLE $dbName0.copy_export_bad (id INT)")
    spark.sql(s"INSERT INTO $dbName0.copy_export_bad VALUES (1)")

    withCsvDir {
      dir =>
        val outputPath = new File(dir, "out").getAbsolutePath
        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO '$outputPath'
                       |FROM $dbName0.copy_export_bad
                       |FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("Unsupported FILE_FORMAT options for export"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_export_bad")
  }

  test("COPY INTO: explicit column list with default value column") {
    assume(gteqSpark3_4)
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_default")
    spark.sql(
      s"CREATE TABLE $dbName0.copy_default (id INT, name STRING, status STRING DEFAULT 'active')")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n2,Bob\n")

        spark.sql(s"""COPY INTO $dbName0.copy_default (id, name)
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_default ORDER BY id"),
          Seq(Row(1, "Alice", "active"), Row(2, "Bob", "active")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_default")
  }

  test("COPY INTO: too many CSV columns fails") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_toomany")
    spark.sql(s"CREATE TABLE $dbName0.copy_toomany (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice,extra,columns\n")

        intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_toomany
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV)
                       |""".stripMargin)
        }
        assert(spark.sql(s"SELECT * FROM $dbName0.copy_toomany").count() == 0)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_toomany")
  }

  test("COPY INTO: too few CSV columns fails") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_toofew")
    spark.sql(s"CREATE TABLE $dbName0.copy_toofew (id INT, name STRING, age INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n")

        intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_toofew
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV)
                       |""".stripMargin)
        }
        assert(spark.sql(s"SELECT * FROM $dbName0.copy_toofew").count() == 0)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_toofew")
  }

  test("COPY INTO: rows_loaded count is accurate") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_count")
    spark.sql(s"CREATE TABLE $dbName0.copy_count (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n2,Bob\n3,Charlie\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_count
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "LOADED")
        assert(rows(0).getLong(2) == 3L)
        assert(rows(0).getLong(3) == 3L)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_count")
  }

  test("COPY INTO: duplicate column list errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_dup_col")
    spark.sql(s"CREATE TABLE $dbName0.copy_dup_col (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n")

        val e = intercept[IllegalArgumentException] {
          spark.sql(s"""COPY INTO $dbName0.copy_dup_col (id, id)
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV)
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("Duplicate columns"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_dup_col")
  }

  protected def createJsonFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    val writer = new PrintWriter(file)
    try writer.write(content)
    finally writer.close()
    file
  }

  protected def withJsonDir(testBody: File => Unit): Unit = {
    val dir = Files.createTempDirectory("copy_into_json_test").toFile
    try testBody(dir)
    finally deleteRecursively(dir)
  }

  test("COPY INTO: basic JSON import") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_basic")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_basic (id INT, name STRING, age INT)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"Alice","age":"30"}
            |{"id":"2","name":"Bob","age":"25"}
            |""".stripMargin)

        val result = spark.sql(s"""COPY INTO $dbName0.copy_json_basic
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = JSON)
                                  |""".stripMargin)

        assert(result.collect().length > 0)
        assert(result.collect()(0).getString(1) == "LOADED")

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_basic ORDER BY id"),
          Seq(Row(1, "Alice", 30), Row(2, "Bob", 25)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_basic")
  }

  test("COPY INTO: JSON column name matching ignores order") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_order")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_order (id INT, name STRING, age INT)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"age":"30","name":"Alice","id":"1"}
            |{"name":"Bob","id":"2","age":"25"}
            |""".stripMargin)

        spark.sql(s"""COPY INTO $dbName0.copy_json_order
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_order ORDER BY id"),
          Seq(Row(1, "Alice", 30), Row(2, "Bob", 25)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_order")
  }

  test("COPY INTO: JSON with MULTI_LINE") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_ml")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_ml (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """[
            |  {"id":"1","name":"Alice"},
            |  {"id":"2","name":"Bob"}
            |]""".stripMargin)

        spark.sql(s"""COPY INTO $dbName0.copy_json_ml
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON, MULTI_LINE = TRUE)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_ml ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_ml")
  }

  test("COPY INTO: JSON with explicit column list") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_cols")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_cols (id INT, name STRING, age INT)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"Alice"}
            |{"id":"2","name":"Bob"}
            |""".stripMargin)

        spark.sql(s"""COPY INTO $dbName0.copy_json_cols (id, name)
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_cols ORDER BY id"),
          Seq(Row(1, "Alice", null), Row(2, "Bob", null)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_cols")
  }

  test("COPY INTO: JSON NULL_IF") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_null")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_null (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"NULL"}
            |{"id":"2","name":"\\N"}
            |{"id":"3","name":"Alice"}
            |""".stripMargin
        )

        spark.sql(s"""COPY INTO $dbName0.copy_json_null
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON, NULL_IF = ('NULL', '\\N'))
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_null ORDER BY id"),
          Seq(Row(1, null), Row(2, null), Row(3, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_null")
  }

  test("COPY INTO: JSON export") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_export")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_export (id INT, name STRING)")
    spark.sql(s"INSERT INTO $dbName0.copy_json_export VALUES (1, 'Alice'), (2, 'Bob')")

    withJsonDir {
      dir =>
        val outputPath = new File(dir, "output").getAbsolutePath

        val result = spark.sql(s"""COPY INTO '$outputPath'
                                  |FROM $dbName0.copy_json_export
                                  |FILE_FORMAT = (TYPE = JSON)
                                  |""".stripMargin)

        val row = result.collect()(0)
        assert(row.getString(0) == outputPath)
        assert(row.getLong(2) == 2L)

        val exported = spark.read.json(outputPath)
        assert(exported.count() == 2)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_export")
  }

  test("COPY INTO: JSON rejects CSV-only options") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_bad_opt")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_bad_opt (id INT)")

    withJsonDir {
      dir =>
        createJsonFile(dir, "data.json", """{"id":"1"}""")

        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_json_bad_opt
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = JSON, FIELD_DELIMITER = ',')
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("Unsupported FILE_FORMAT options"))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_bad_opt")
  }

  test("COPY INTO: JSON with date and timestamp columns") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_date")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_date (id INT, dt DATE, ts TIMESTAMP)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","dt":"2024-01-15","ts":"2024-01-15T10:30:00"}
            |{"id":"2","dt":"2024-06-20","ts":"2024-06-20T14:45:30"}
            |""".stripMargin
        )

        spark.sql(s"""COPY INTO $dbName0.copy_json_date
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON)
                     |""".stripMargin)

        val rows = spark.sql(s"SELECT * FROM $dbName0.copy_json_date ORDER BY id").collect()
        assert(rows.length == 2)
        assert(rows(0).getInt(0) == 1)
        assert(rows(0).getDate(1).toString == "2024-01-15")
        assert(rows(1).getInt(0) == 2)
        assert(rows(1).getDate(1).toString == "2024-06-20")
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_date")
  }

  test("COPY INTO: JSON rows_loaded count is accurate") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_count")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_count (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"Alice"}
            |{"id":"2","name":"Bob"}
            |{"id":"3","name":"Charlie"}
            |""".stripMargin
        )

        val result = spark.sql(s"""COPY INTO $dbName0.copy_json_count
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = JSON)
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "LOADED")
        assert(rows(0).getLong(2) == 3L)
        assert(rows(0).getLong(3) == 3L)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_count")
  }

  test("COPY INTO: JSON export then import round-trip") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_rt_src")
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_rt_dst")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_rt_src (id INT, name STRING, score DOUBLE)")
    spark.sql(s"INSERT INTO $dbName0.copy_json_rt_src VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3)")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_rt_dst (id INT, name STRING, score DOUBLE)")

    withJsonDir {
      dir =>
        val exportPath = new File(dir, "exported").getAbsolutePath

        spark.sql(s"""COPY INTO '$exportPath'
                     |FROM $dbName0.copy_json_rt_src
                     |FILE_FORMAT = (TYPE = JSON)
                     |""".stripMargin)

        spark.sql(s"""COPY INTO $dbName0.copy_json_rt_dst
                     |FROM '$exportPath'
                     |FILE_FORMAT = (TYPE = JSON)
                     |PATTERN = '.*\\.json'
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_rt_dst ORDER BY id"),
          Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 87.3)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_rt_src")
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_rt_dst")
  }

  test("COPY INTO: JSON extra fields are ignored") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_extra")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_extra (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"Alice","extra_field":"ignored","another":"also_ignored"}
            |{"id":"2","name":"Bob","extra_field":"ignored2"}
            |""".stripMargin
        )

        spark.sql(s"""COPY INTO $dbName0.copy_json_extra
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_extra ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_extra")
  }

  test("COPY INTO: JSON missing fields become null") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_missing")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_missing (id INT, name STRING, age INT)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"Alice"}
            |{"id":"2"}
            |""".stripMargin
        )

        spark.sql(s"""COPY INTO $dbName0.copy_json_missing
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = JSON)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_json_missing ORDER BY id"),
          Seq(Row(1, "Alice", null), Row(2, null, null)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_missing")
  }

  test("COPY INTO: JSON malformed data fails with ABORT_STATEMENT") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_malformed")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_malformed (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id":"1","name":"Alice"}
            |{this is not valid json}
            |""".stripMargin
        )

        intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_json_malformed
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = JSON)
                       |""".stripMargin)
        }
        assert(spark.sql(s"SELECT * FROM $dbName0.copy_json_malformed").count() == 0)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_malformed")
  }

  test("COPY INTO: JSON bad cast fails with ABORT_STATEMENT") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_badcast")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_badcast (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(dir, "data.json", """{"id":"not_a_number","name":"Alice"}""")

        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_json_badcast
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = JSON)
                       |""".stripMargin)
        }
        val msg = e.getMessage
        assert(
          msg.contains("Cast failure") ||
            msg.contains("ABORT_STATEMENT") ||
            msg.contains("CAST_INVALID_INPUT") ||
            msg.contains("cannot be cast to") ||
            e.getCause != null)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_badcast")
  }

  test("COPY INTO: JSON export with COMPRESSION") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_compress")
    spark.sql(s"CREATE TABLE $dbName0.copy_json_compress (id INT, name STRING)")
    spark.sql(s"INSERT INTO $dbName0.copy_json_compress VALUES (1, 'Alice'), (2, 'Bob')")

    withJsonDir {
      dir =>
        val outputPath = new File(dir, "compressed").getAbsolutePath

        val result = spark.sql(s"""COPY INTO '$outputPath'
                                  |FROM $dbName0.copy_json_compress
                                  |FILE_FORMAT = (TYPE = JSON, COMPRESSION = GZIP)
                                  |OVERWRITE = TRUE
                                  |""".stripMargin)

        assert(result.collect()(0).getLong(2) == 2L)

        val outputDir = new File(outputPath)
        val gzFiles = outputDir.listFiles().filter(_.getName.endsWith(".gz"))
        assert(gzFiles.nonEmpty)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_json_compress")
  }

  test("COPY INTO: case-insensitive column matching") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_case")
    spark.sql(s"CREATE TABLE $dbName0.copy_case (id INT, name STRING, age INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n")

        spark.sql(s"""COPY INTO $dbName0.copy_case (ID, NAME)
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = CSV)
                     |""".stripMargin)

        checkAnswer(spark.sql(s"SELECT * FROM $dbName0.copy_case"), Seq(Row(1, "Alice", null)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_case")
  }

  // ========== Parquet Tests ==========

  protected def withParquetDir(testBody: File => Unit): Unit = {
    val dir = Files.createTempDirectory("copy_into_parquet_test").toFile
    try testBody(dir)
    finally deleteRecursively(dir)
  }

  private def createParquetFile(
      dir: File,
      name: String,
      data: Seq[Row],
      schema: org.apache.spark.sql.types.StructType): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.coalesce(1).write.parquet(new File(dir, name).getAbsolutePath)
  }

  protected def createParquetSingleFile(
      dir: File,
      fileName: String,
      data: Seq[Row],
      schema: org.apache.spark.sql.types.StructType): Unit = {
    val tmpDir = new File(dir, s"_tmp_$fileName")
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.coalesce(1).write.parquet(tmpDir.getAbsolutePath)
    val partFile = tmpDir.listFiles().find(_.getName.endsWith(".parquet")).get
    partFile.renameTo(new File(dir, fileName))
    deleteRecursively(tmpDir)
  }

  test("COPY INTO: basic Parquet import") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_basic")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_basic (id INT, name STRING, age INT)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema = StructType(
          Seq(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("age", IntegerType)))
        createParquetFile(dir, "data", Seq(Row(1, "Alice", 30), Row(2, "Bob", 25)), schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_basic
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_basic ORDER BY id"),
          Seq(Row(1, "Alice", 30), Row(2, "Bob", 25)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_basic")
  }

  test("COPY INTO: Parquet column name matching ignores order") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_order")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_order (id INT, name STRING, age INT)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema = StructType(
          Seq(
            StructField("age", IntegerType),
            StructField("name", StringType),
            StructField("id", IntegerType)))
        createParquetFile(dir, "data", Seq(Row(30, "Alice", 1), Row(25, "Bob", 2)), schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_order
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_order ORDER BY id"),
          Seq(Row(1, "Alice", 30), Row(2, "Bob", 25)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_order")
  }

  test("COPY INTO: Parquet with explicit column list") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_cols")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_cols (id INT, name STRING, age INT)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema =
          StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
        createParquetFile(dir, "data", Seq(Row(1, "Alice"), Row(2, "Bob")), schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_cols (id, name)
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_cols ORDER BY id"),
          Seq(Row(1, "Alice", null), Row(2, "Bob", null)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_cols")
  }

  test("COPY INTO: Parquet export") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_export")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_export (id INT, name STRING)")
    spark.sql(s"INSERT INTO $dbName0.copy_parquet_export VALUES (1, 'Alice'), (2, 'Bob')")

    withParquetDir {
      dir =>
        val outputPath = new File(dir, "output").getAbsolutePath
        spark.sql(s"""COPY INTO '$outputPath'
                     |FROM $dbName0.copy_parquet_export
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        val readBack = spark.read.parquet(outputPath)
        checkAnswer(readBack.orderBy("id"), Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_export")
  }

  test("COPY INTO: Parquet export with COMPRESSION") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_compress")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_compress (id INT, name STRING)")
    spark.sql(s"INSERT INTO $dbName0.copy_parquet_compress VALUES (1, 'Alice'), (2, 'Bob')")

    withParquetDir {
      dir =>
        val outputPath = new File(dir, "output").getAbsolutePath
        spark.sql(s"""COPY INTO '$outputPath'
                     |FROM $dbName0.copy_parquet_compress
                     |FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = GZIP)
                     |""".stripMargin)

        val readBack = spark.read.parquet(outputPath)
        checkAnswer(readBack.orderBy("id"), Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_compress")
  }

  test("COPY INTO: Parquet export then import round-trip") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_rt_src")
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_rt_dst")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_rt_src (id INT, name STRING, score DOUBLE)")
    spark.sql(
      s"INSERT INTO $dbName0.copy_parquet_rt_src VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3)")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_rt_dst (id INT, name STRING, score DOUBLE)")

    withParquetDir {
      dir =>
        val outputPath = new File(dir, "export").getAbsolutePath
        spark.sql(s"""COPY INTO '$outputPath'
                     |FROM $dbName0.copy_parquet_rt_src
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_rt_dst
                     |FROM '$outputPath'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_rt_dst ORDER BY id"),
          Seq(Row(1, "Alice", 95.5), Row(2, "Bob", 87.3)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_rt_src")
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_rt_dst")
  }

  test("COPY INTO: Parquet extra fields are ignored") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_extra")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_extra (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema = StructType(
          Seq(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("extra", StringType)))
        createParquetFile(
          dir,
          "data",
          Seq(Row(1, "Alice", "ignore"), Row(2, "Bob", "ignore")),
          schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_extra
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_extra ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_extra")
  }

  test("COPY INTO: Parquet missing fields become null") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_missing")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_missing (id INT, name STRING, age INT)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema =
          StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
        createParquetFile(dir, "data", Seq(Row(1, "Alice"), Row(2, "Bob")), schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_missing
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_missing ORDER BY id"),
          Seq(Row(1, "Alice", null), Row(2, "Bob", null)))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_missing")
  }

  test("COPY INTO: Parquet FORCE=FALSE skips already-loaded files") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_force")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_force (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema =
          StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
        createParquetFile(dir, "data", Seq(Row(1, "Alice")), schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_force
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |""".stripMargin)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_force
                     |FROM '${dir.getAbsolutePath}/data'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |FORCE = FALSE
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_force ORDER BY id"),
          Seq(Row(1, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_force")
  }

  test("COPY INTO: Parquet PATTERN filters files") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_pattern")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_pattern (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema =
          StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
        createParquetSingleFile(dir, "include_data.parquet", Seq(Row(1, "Alice")), schema)
        createParquetSingleFile(dir, "exclude_data.parquet", Seq(Row(2, "Bob")), schema)

        spark.sql(s"""COPY INTO $dbName0.copy_parquet_pattern
                     |FROM '${dir.getAbsolutePath}'
                     |FILE_FORMAT = (TYPE = PARQUET)
                     |PATTERN = 'include.*'
                     |""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_parquet_pattern ORDER BY id"),
          Seq(Row(1, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_pattern")
  }

  test("COPY INTO: Parquet unsupported option errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_opt_err")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_opt_err (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema =
          StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
        createParquetFile(dir, "data", Seq(Row(1, "Alice")), schema)

        intercept[IllegalArgumentException] {
          spark.sql(s"""COPY INTO $dbName0.copy_parquet_opt_err
                       |FROM '${dir.getAbsolutePath}/data'
                       |FILE_FORMAT = (TYPE = PARQUET, FIELD_DELIMITER = ',')
                       |""".stripMargin)
        }
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_opt_err")
  }

  test("COPY INTO: Parquet rows_loaded count is accurate") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_count")
    spark.sql(s"CREATE TABLE $dbName0.copy_parquet_count (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema =
          StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
        createParquetFile(
          dir,
          "data",
          Seq(Row(1, "Alice"), Row(2, "Bob"), Row(3, "Charlie")),
          schema)

        val result = spark.sql(s"""COPY INTO $dbName0.copy_parquet_count
                                  |FROM '${dir.getAbsolutePath}/data'
                                  |FILE_FORMAT = (TYPE = PARQUET)
                                  |""".stripMargin)

        val rows = result.collect()
        val totalLoaded = rows.map(_.getLong(2)).sum
        assert(totalLoaded == 3)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_parquet_count")
  }
}
