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

  private def createCsvFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    val writer = new PrintWriter(file)
    try writer.write(content)
    finally writer.close()
    file
  }

  private def withCsvDir(testBody: File => Unit): Unit = {
    val dir = Files.createTempDirectory("copy_into_test").toFile
    try testBody(dir)
    finally deleteRecursively(dir)
  }

  private def deleteRecursively(file: File): Unit = {
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
                       |FILE_FORMAT = (TYPE = PARQUET)
                       |""".stripMargin)
        }
        assert(e.getMessage.contains("Unsupported file format type"))
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
}
