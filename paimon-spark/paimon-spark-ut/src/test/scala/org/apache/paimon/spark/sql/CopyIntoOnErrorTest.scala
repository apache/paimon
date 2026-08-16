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

import org.apache.spark.sql.Row

trait CopyIntoOnErrorTest { self: CopyIntoTestBase =>

  test("COPY INTO: ON_ERROR = CONTINUE skips bad CSV rows") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_csv")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_csv (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\nabc,Bob\n3,Charlie\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_csv
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getLong(4) > 0)
        assert(rows(0).getString(1) == "PARTIALLY_LOADED")

        val data = spark.sql(s"SELECT * FROM $dbName0.copy_continue_csv ORDER BY id").collect()
        assert(
          data.length == 2,
          s"Expected 2 rows but got ${data.length} — bad row should NOT be in table")
        assert(data(0).getInt(0) == 1 && data(0).getString(1) == "Alice")
        assert(data(1).getInt(0) == 3 && data(1).getString(1) == "Charlie")
        assert(
          !data.exists(r => r.getString(1) == "Bob"),
          "Bad row with 'abc' as id should not be in the table")
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_csv")
  }

  test("COPY INTO: ON_ERROR = CONTINUE with no errors behaves like ABORT") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_ok")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_ok (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n2,Bob\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_ok
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "LOADED")
        assert(rows(0).getLong(4) == 0L)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_continue_ok ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_ok")
  }

  test("COPY INTO: ON_ERROR = SKIP_FILE skips file with bad data") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile")
    spark.sql(s"CREATE TABLE $dbName0.copy_skipfile (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "good.csv", "1,Alice\n2,Bob\n")
        createCsvFile(dir, "bad.csv", "abc,Charlie\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_skipfile
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = SKIP_FILE
                                  |""".stripMargin)

        val rows = result.collect()
        val loaded = rows.filter(_.getString(1) == "LOADED")
        val failed = rows.filter(_.getString(1) == "LOAD_FAILED")
        assert(loaded.length == 1)
        assert(failed.length == 1)
        assert(failed(0).getLong(4) >= 1L)
        assert(failed(0).getString(5) != null)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_skipfile ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile")
  }

  test("COPY INTO: ON_ERROR = SKIP_FILE discards good rows in a file that also has bad rows") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_mixed")
    spark.sql(s"CREATE TABLE $dbName0.copy_skipfile_mixed (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "good.csv", "1,Alice\n2,Bob\n")
        // 2 good rows (Carol, Eve) and 1 bad row (non-numeric id)
        createCsvFile(dir, "mixed.csv", "3,Carol\nabc,Dave\n4,Eve\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_skipfile_mixed
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = SKIP_FILE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.count(_.getString(1) == "LOADED") == 1)
        assert(rows.count(_.getString(1) == "LOAD_FAILED") == 1)

        // The whole mixed.csv is rejected: its good rows (Carol, Eve) must not be loaded.
        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_skipfile_mixed ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_mixed")
  }

  test("COPY INTO: ON_ERROR = SKIP_FILE with all good files") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_ok")
    spark.sql(s"CREATE TABLE $dbName0.copy_skipfile_ok (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "a.csv", "1,Alice\n")
        createCsvFile(dir, "b.csv", "2,Bob\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_skipfile_ok
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = SKIP_FILE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.forall(_.getString(1) == "LOADED"))
        assert(rows.length == 2)

        assert(spark.sql(s"SELECT * FROM $dbName0.copy_skipfile_ok").count() == 2)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_ok")
  }

  test("COPY INTO: ON_ERROR = CONTINUE with JSON malformed records") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_json")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_json (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id": "1", "name": "Alice"}
            |{bad json here}
            |{"id": "3", "name": "Charlie"}
            |""".stripMargin
        )

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_json
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = JSON)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows(0).getLong(4) > 0)

        val data = spark.sql(s"SELECT * FROM $dbName0.copy_continue_json ORDER BY id").collect()
        assert(
          data.length == 2,
          s"Expected 2 rows but got ${data.length} — malformed JSON should NOT be in table")
        assert(data(0).getInt(0) == 1)
        assert(data(1).getInt(0) == 3)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_json")
  }

  test("COPY INTO: ON_ERROR = SKIP_FILE with JSON") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_json")
    spark.sql(s"CREATE TABLE $dbName0.copy_skipfile_json (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(dir, "good.json", """{"id": "1", "name": "Alice"}""" + "\n")
        createJsonFile(dir, "bad.json", "{bad json}\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_skipfile_json
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = JSON)
                                  |ON_ERROR = SKIP_FILE
                                  |""".stripMargin)

        val rows = result.collect()
        val loaded = rows.filter(_.getString(1) == "LOADED")
        val failed = rows.filter(_.getString(1) == "LOAD_FAILED")
        assert(loaded.length == 1)
        assert(failed.length == 1)

        checkAnswer(spark.sql(s"SELECT * FROM $dbName0.copy_skipfile_json"), Seq(Row(1, "Alice")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_json")
  }

  test("COPY INTO: ABORT_STATEMENT still fails on bad data") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_abort_explicit")
    spark.sql(s"CREATE TABLE $dbName0.copy_abort_explicit (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "abc,Alice\n")

        val e = intercept[Exception] {
          spark.sql(s"""COPY INTO $dbName0.copy_abort_explicit
                       |FROM '${dir.getAbsolutePath}'
                       |FILE_FORMAT = (TYPE = CSV)
                       |ON_ERROR = ABORT_STATEMENT
                       |""".stripMargin)
        }
        assert(
          e.getMessage.contains("Cast failure") ||
            e.getMessage.contains("ABORT_STATEMENT") ||
            e.getMessage.contains("CAST_INVALID_INPUT") ||
            e.getCause != null)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_abort_explicit")
  }

  test("COPY INTO: ON_ERROR = CONTINUE with Parquet cast errors") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_pq")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_pq (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        val schema = StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
        createParquetSingleFile(
          dir,
          "data.parquet",
          Seq(Row("1", "Alice"), Row("abc", "Bad"), Row("3", "Charlie")),
          schema)

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_pq
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = PARQUET)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getLong(4) > 0)

        val data = spark.sql(s"SELECT * FROM $dbName0.copy_continue_pq ORDER BY id").collect()
        assert(
          data.length == 2,
          s"Expected 2 rows but got ${data.length} — bad row should NOT be in table")
        assert(data(0).getInt(0) == 1)
        assert(data(1).getInt(0) == 3)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_pq")
  }

  test("COPY INTO: ON_ERROR = SKIP_FILE with Parquet") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_pq")
    spark.sql(s"CREATE TABLE $dbName0.copy_skipfile_pq (id INT, name STRING)")

    withParquetDir {
      dir =>
        import org.apache.spark.sql.types._
        // Both files share the same physical schema (id STRING) so the directory read succeeds;
        // good rows cast cleanly to the target INT column while bad.parquet's "abc" fails the cast,
        // which is what SKIP_FILE must catch. Using a mixed INT/STRING footer here would instead
        // crash Spark's vectorized Parquet reader before any cast validation runs.
        val schema =
          StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
        createParquetSingleFile(
          dir,
          "good.parquet",
          Seq(Row("1", "Alice"), Row("2", "Bob")),
          schema)

        createParquetSingleFile(dir, "bad.parquet", Seq(Row("abc", "Bad")), schema)

        val result = spark.sql(s"""COPY INTO $dbName0.copy_skipfile_pq
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = PARQUET)
                                  |ON_ERROR = SKIP_FILE
                                  |""".stripMargin)

        val rows = result.collect()
        val loaded = rows.filter(_.getString(1) == "LOADED")
        val failed = rows.filter(_.getString(1) == "LOAD_FAILED")
        assert(loaded.length == 1)
        assert(failed.length == 1)

        checkAnswer(
          spark.sql(s"SELECT * FROM $dbName0.copy_skipfile_pq ORDER BY id"),
          Seq(Row(1, "Alice"), Row(2, "Bob")))
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_skipfile_pq")
  }

  test("COPY INTO: ON_ERROR = CONTINUE clean file has no first_error") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_multi")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_multi (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "good.csv", "1,Alice\n2,Bob\n")
        createCsvFile(dir, "bad.csv", "abc,Charlie\n3,Dave\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_multi
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        val goodFile = rows.find(_.getString(0) == "good.csv").get
        val badFile = rows.find(_.getString(0) == "bad.csv").get

        assert(goodFile.getString(1) == "LOADED")
        assert(goodFile.getLong(4) == 0L)
        assert(goodFile.isNullAt(5), "Clean file should have null first_error")

        assert(badFile.getString(1) == "PARTIALLY_LOADED")
        assert(badFile.getLong(4) > 0)
        assert(!badFile.isNullAt(5), "Bad file should have non-null first_error")
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_multi")
  }

  test("COPY INTO: ON_ERROR = CONTINUE with JSON cast errors reports correctly") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_json_cast")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_json_cast (id INT, name STRING)")

    withJsonDir {
      dir =>
        createJsonFile(
          dir,
          "data.json",
          """{"id": "1", "name": "Alice"}
            |{"id": "abc", "name": "Bad"}
            |{"id": "3", "name": "Charlie"}
            |""".stripMargin
        )

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_json_cast
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = JSON)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getLong(4) > 0, "Should report cast errors")
        assert(
          rows(0).getString(1) == "PARTIALLY_LOADED",
          s"Expected PARTIALLY_LOADED but got ${rows(0).getString(1)}")

        val data =
          spark.sql(s"SELECT * FROM $dbName0.copy_continue_json_cast ORDER BY id").collect()
        assert(data.length == 2, s"Expected 2 rows but got ${data.length}")
        assert(data(0).getInt(0) == 1)
        assert(data(1).getInt(0) == 3)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_json_cast")
  }

  test("COPY INTO: ON_ERROR = CONTINUE all rows fail reports LOAD_FAILED") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_allfail")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_allfail (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "allfail.csv", "abc,Alice\nxyz,Bob\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_allfail
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        val rows = result.collect()
        assert(rows.length == 1)
        assert(
          rows(0).getString(1) == "LOAD_FAILED",
          s"Expected LOAD_FAILED when all rows fail, got ${rows(0).getString(1)}")
        assert(rows(0).getLong(2) == 0L, "rows_loaded should be 0")
        assert(rows(0).getLong(3) == 2L, "rows_parsed should be 2")
        assert(rows(0).getLong(4) == 2L, "errors_seen should be 2")

        assert(
          spark.sql(s"SELECT * FROM $dbName0.copy_continue_allfail").count() == 0,
          "No rows should be in table")
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_allfail")
  }

  test("COPY INTO: ON_ERROR = CONTINUE all rows fail does not block re-run") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_retry")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_retry (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "abc,Alice\n")

        // First run: all rows fail
        val result1 = spark.sql(s"""COPY INTO $dbName0.copy_continue_retry
                                   |FROM '${dir.getAbsolutePath}'
                                   |FILE_FORMAT = (TYPE = CSV)
                                   |ON_ERROR = CONTINUE
                                   |""".stripMargin)
        val rows1 = result1.collect()
        assert(rows1(0).getString(1) == "LOAD_FAILED")
        assert(spark.sql(s"SELECT * FROM $dbName0.copy_continue_retry").count() == 0)

        // Re-run without FORCE: file should NOT be skipped since 0 rows were loaded
        val result2 = spark.sql(s"""COPY INTO $dbName0.copy_continue_retry
                                   |FROM '${dir.getAbsolutePath}'
                                   |FILE_FORMAT = (TYPE = CSV)
                                   |ON_ERROR = CONTINUE
                                   |""".stripMargin)
        val rows2 = result2.collect()
        assert(
          rows2(0).getString(1) != "SKIPPED",
          "File with 0 rows loaded should not be skipped on re-run")
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_retry")
  }

  test("COPY INTO: ON_ERROR = CONTINUE skips CSV rows with fewer columns") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_fewer_cols")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_fewer_cols (id INT, name STRING, age INT)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice\n2,Bob,20\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_fewer_cols
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        // A row with fewer columns than the target schema is a malformed record
        // (Spark CSV PERMISSIVE mode), so CONTINUE skips it and keeps the well-formed row.
        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "PARTIALLY_LOADED")
        assert(rows(0).getLong(2) == 1L, "rows_loaded should be 1")
        assert(rows(0).getLong(4) == 1L, "errors_seen should be 1")

        val data =
          spark.sql(s"SELECT * FROM $dbName0.copy_continue_fewer_cols ORDER BY id").collect()
        assert(data.length == 1)
        assert(data(0).getInt(0) == 2 && data(0).getString(1) == "Bob" && data(0).getInt(2) == 20)
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_fewer_cols")
  }

  test("COPY INTO: ON_ERROR = CONTINUE skips CSV rows with extra columns") {
    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_extra_cols")
    spark.sql(s"CREATE TABLE $dbName0.copy_continue_extra_cols (id INT, name STRING)")

    withCsvDir {
      dir =>
        createCsvFile(dir, "data.csv", "1,Alice,extra\n2,Bob\n")

        val result = spark.sql(s"""COPY INTO $dbName0.copy_continue_extra_cols
                                  |FROM '${dir.getAbsolutePath}'
                                  |FILE_FORMAT = (TYPE = CSV)
                                  |ON_ERROR = CONTINUE
                                  |""".stripMargin)

        // A row with more columns than the target schema is a malformed record
        // (Spark CSV PERMISSIVE mode), so CONTINUE skips it and keeps the well-formed row.
        val rows = result.collect()
        assert(rows.length == 1)
        assert(rows(0).getString(1) == "PARTIALLY_LOADED")
        assert(rows(0).getLong(2) == 1L, "rows_loaded should be 1")
        assert(rows(0).getLong(4) == 1L, "errors_seen should be 1")

        val data =
          spark.sql(s"SELECT * FROM $dbName0.copy_continue_extra_cols ORDER BY id").collect()
        assert(data.length == 1)
        assert(data(0).getInt(0) == 2 && data(0).getString(1) == "Bob")
    }

    spark.sql(s"DROP TABLE IF EXISTS $dbName0.copy_continue_extra_cols")
  }
}
