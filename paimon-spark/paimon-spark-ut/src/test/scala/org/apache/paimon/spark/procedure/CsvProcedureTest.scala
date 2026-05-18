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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

import java.io.{File, PrintWriter}
import java.nio.file.Files

class CsvProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: load_csv basic") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, age INT)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,name,age")
    writer.println("1,Alice,30")
    writer.println("2,Bob,25")
    writer.println("3,Charlie,35")
    writer.close()

    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 3L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Row(1, "Alice", 30) :: Row(2, "Bob", 25) :: Row(3, "Charlie", 35) :: Nil
    )
  }

  test("Paimon procedure: load_csv with custom separator") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_sep_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id|name")
    writer.println("1|Alice")
    writer.println("2|Bob")
    writer.close()

    checkAnswer(
      spark.sql(s"""CALL paimon.sys.load_csv(
                   |  table => 'test.T',
                   |  path => '${csvFile.getPath}',
                   |  options => map('sep', '|'))""".stripMargin),
      Row(true, 2L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Row(1, "Alice") :: Row(2, "Bob") :: Nil
    )
  }

  test("Paimon procedure: load_csv with missing columns") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, city STRING)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_missing_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,name")
    writer.println("1,Alice")
    writer.close()

    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 1L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT * FROM T"),
      Row(1, "Alice", null) :: Nil
    )
  }

  test("Paimon procedure: load_csv with nested types") {
    spark.sql("""
                |CREATE TABLE T (id INT, info STRUCT<city: STRING, zip: INT>)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_nested_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,info")
    writer.println("1,\"{\"\"city\"\":\"\"NYC\"\",\"\"zip\"\":10001}\"")
    writer.close()

    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 1L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT id, info.city, info.zip FROM T"),
      Row(1, "NYC", 10001) :: Nil
    )
  }

  test("Paimon procedure: load_csv with invalid nested JSON writes null fields") {
    spark.sql("""
                |CREATE TABLE T (id INT, info STRUCT<city: STRING, zip: INT>)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_nested_malformed_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,info")
    writer.println("1,\"{\"\"city\"\":\"\"NYC\"\",\"\"zip\"\":10001}\"")
    writer.println("2,not-json")
    writer.println("3,\"{\"\"city\"\":\"\"LA\"\",\"\"zip\"\":90001}\"")
    writer.close()

    // Invalid nested JSON is not detected as a malformed row — from_json produces null fields
    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 3L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT id, info.city, info.zip FROM T ORDER BY id"),
      Row(1, "NYC", 10001) :: Row(2, null, null) :: Row(3, "LA", 90001) :: Nil
    )
  }

  test("Paimon procedure: export_csv basic") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, age INT)
                |USING PAIMON
                |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")

    val outputPath = Files.createTempDirectory("csv_export_test").toFile.getPath + "/output.csv"

    checkAnswer(
      spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$outputPath')"),
      Row(true, 3L) :: Nil
    )

    val exported = spark.read.option("header", "true").option("inferSchema", "true").csv(outputPath)
    assert(exported.count() == 3)
    checkAnswer(
      exported.select("id", "name", "age").orderBy("id"),
      Row(1, "Alice", 30) :: Row(2, "Bob", 25) :: Row(3, "Charlie", 35) :: Nil
    )
  }

  test("Paimon procedure: export_csv with where clause") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")

    val outputPath = Files.createTempDirectory("csv_export_where_test").toFile.getPath + "/out.csv"

    checkAnswer(
      spark.sql(s"""CALL paimon.sys.export_csv(
                   |  table => 'test.T',
                   |  path => '$outputPath',
                   |  where => 'id > 1')""".stripMargin),
      Row(true, 2L) :: Nil
    )

    val exported = spark.read.option("header", "true").option("inferSchema", "true").csv(outputPath)
    assert(exported.count() == 2)
  }

  test("Paimon procedure: export_csv with nested types") {
    spark.sql("""
                |CREATE TABLE T (id INT, tags ARRAY<STRING>)
                |USING PAIMON
                |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, array('a', 'b'))")

    val outputPath =
      Files.createTempDirectory("csv_export_nested_test").toFile.getPath + "/out.csv"

    checkAnswer(
      spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$outputPath')"),
      Row(true, 1L) :: Nil
    )

    val exported = spark.read.option("header", "true").option("escape", "\"").csv(outputPath)
    val tagsStr = exported.collect()(0).getString(1)
    assert(tagsStr.contains("a") && tagsStr.contains("b"))
  }

  test("Paimon procedure: load_csv and export_csv roundtrip") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, score DOUBLE)
                |USING PAIMON
                |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'Alice', 95.5), (2, 'Bob', 88.0), (3, 'Charlie', 72.3)")

    val exportPath =
      Files.createTempDirectory("csv_roundtrip_test").toFile.getPath + "/export.csv"

    spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$exportPath')")

    spark.sql("DROP TABLE T")
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, score DOUBLE)
                |USING PAIMON
                |""".stripMargin)

    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '$exportPath')"),
      Row(true, 3L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Row(1, "Alice", 95.5) :: Row(2, "Bob", 88.0) :: Row(3, "Charlie", 72.3) :: Nil
    )
  }

  test("Paimon procedure: load_csv with _corrupt_record column in target table") {
    spark.sql("""
                |CREATE TABLE T (id INT, _corrupt_record STRING, value INT)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_corrupt_col_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,_corrupt_record,value")
    writer.println("1,some_text,100")
    writer.println("2,other_text,not_int")
    writer.println("3,more_text,300")
    writer.close()

    // Row 2 has a malformed 'value' (not_int for INT column) —
    // internal bad-row counting should still work despite the column name collision
    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 2L, 1L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Row(1, "some_text", 100) :: Row(3, "more_text", 300) :: Nil
    )
  }

  test("Paimon procedure: load_csv with reserved corrupt record column name collision") {
    spark.sql("""
                |CREATE TABLE T (id INT, `_CORRUPT_RECORD` STRING, value INT)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_corrupt_upper_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,_CORRUPT_RECORD,value")
    writer.println("1,hello,10")
    writer.println("2,world,not_int")
    writer.println("3,foo,30")
    writer.close()

    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 2L, 1L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Row(1, "hello", 10) :: Row(3, "foo", 30) :: Nil
    )
  }

  test("Paimon procedure: load_csv and export_csv with dotted column names") {
    spark.sql("""
                |CREATE TABLE T (`id` INT, `user.name` STRING, `user.age` INT)
                |USING PAIMON
                |""".stripMargin)

    val csvDir = Files.createTempDirectory("csv_load_dotted_test").toFile
    val csvFile = new File(csvDir, "data.csv")
    val writer = new PrintWriter(csvFile)
    writer.println("id,user.name,user.age")
    writer.println("1,Alice,30")
    writer.println("2,Bob,25")
    writer.close()

    checkAnswer(
      spark.sql(s"CALL paimon.sys.load_csv(table => 'test.T', path => '${csvFile.getPath}')"),
      Row(true, 2L, 0L) :: Nil
    )

    checkAnswer(
      spark.sql("SELECT `id`, `user.name`, `user.age` FROM T ORDER BY id"),
      Row(1, "Alice", 30) :: Row(2, "Bob", 25) :: Nil
    )

    val exportPath =
      Files.createTempDirectory("csv_export_dotted_test").toFile.getPath + "/out.csv"

    checkAnswer(
      spark.sql(s"CALL paimon.sys.export_csv(table => 'test.T', path => '$exportPath')"),
      Row(true, 2L) :: Nil
    )

    val exported = spark.read.option("header", "true").option("inferSchema", "true").csv(exportPath)
    assert(exported.columns.contains("user.name"))
    assert(exported.count() == 2)
  }
}
