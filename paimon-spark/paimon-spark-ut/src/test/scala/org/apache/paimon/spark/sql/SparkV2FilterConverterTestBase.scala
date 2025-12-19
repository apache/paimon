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

import org.apache.paimon.data.{BinaryString, Decimal, Timestamp}
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.{PaimonSparkTestBase, SparkV2FilterConverter}
import org.apache.paimon.spark.util.shim.TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.RowType

import org.apache.spark.SparkConf
import org.apache.spark.sql.PaimonUtils.translateFilterV2
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.connector.expressions.filter.Predicate

import java.time.{LocalDate, LocalDateTime}

import scala.collection.JavaConverters._

/** Test for [[SparkV2FilterConverter]]. */
abstract class SparkV2FilterConverterTestBase extends PaimonSparkTestBase {

  // Add it to disable the automatic generation of the not null filter which impact test.
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.constraintPropagation.enabled", "false")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql("""
          |CREATE TABLE test_tbl (
          | string_col STRING,
          | byte_col BYTE,
          | short_col SHORT,
          | int_col INT,
          | long_col LONG,
          | float_col FLOAT,
          | double_col DOUBLE,
          | decimal_col DECIMAL(10, 5),
          | boolean_col BOOLEAN,
          | date_col DATE,
          | binary_col BINARY
          |) USING paimon
          |""".stripMargin)

    sql("""
          |INSERT INTO test_tbl VALUES
          |('hello', 1, 1, 1, 1, 1.0, 1.0, 12.12345, true, date('2025-01-15'), binary('b1'))
          |""".stripMargin)
    sql("""
          |INSERT INTO test_tbl VALUES
          |('world', 2, 2, 2, 2, 2.0, 2.0, 22.12345, false, date('2025-01-16'), binary('b2'))
          |""".stripMargin)
    sql("""
          |INSERT INTO test_tbl VALUES
          |('hi', 3, 3, 3, 3, 3.0, 3.0, 32.12345, false, date('2025-01-17'), binary('b3'))
          |""".stripMargin)
    sql("""
          |INSERT INTO test_tbl VALUES
          |('paimon', 4, 4, null, 4, 4.0, 4.0, 42.12345, true, date('2025-01-18'), binary('b4'))
          |""".stripMargin)
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS test_tbl")
    super.afterAll()
  }

  lazy val rowType: RowType = loadTable("test_tbl").rowType()

  lazy val builder = new PredicateBuilder(rowType)

  lazy val converter: SparkV2FilterConverter = SparkV2FilterConverter(rowType)

  test("V2Filter: all types") {
    var filter = "string_col = 'hello'"
    var actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(0, BinaryString.fromString("hello"))))
    checkAnswer(sql(s"SELECT string_col from test_tbl WHERE $filter"), Seq(Row("hello")))
    assert(scanFilesCount(filter) == 1)

    filter = "byte_col = 1"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(1, 1.toByte)))
    checkAnswer(sql(s"SELECT byte_col from test_tbl WHERE $filter"), Seq(Row(1.toByte)))
    assert(scanFilesCount(filter) == 1)

    filter = "short_col = 1"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(2, 1.toShort)))
    checkAnswer(sql(s"SELECT short_col from test_tbl WHERE $filter"), Seq(Row(1.toShort)))
    assert(scanFilesCount(filter) == 1)

    filter = "int_col = 1"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(3, 1)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter"), Seq(Row(1)))
    assert(scanFilesCount(filter) == 1)

    filter = "long_col = 1"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(4, 1L)))
    checkAnswer(sql(s"SELECT long_col from test_tbl WHERE $filter"), Seq(Row(1L)))
    assert(scanFilesCount(filter) == 1)

    filter = "float_col = 1.0"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(5, 1.0f)))
    checkAnswer(sql(s"SELECT float_col from test_tbl WHERE $filter"), Seq(Row(1.0f)))
    assert(scanFilesCount(filter) == 1)

    filter = "double_col = 1.0"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(6, 1.0d)))
    checkAnswer(sql(s"SELECT double_col from test_tbl WHERE $filter"), Seq(Row(1.0d)))
    assert(scanFilesCount(filter) == 1)

    filter = "decimal_col = 12.12345"
    actual = converter.convert(v2Filter(filter)).get
    assert(
      actual.equals(
        builder.equal(7, Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5))))
    checkAnswer(
      sql(s"SELECT decimal_col from test_tbl WHERE $filter"),
      Seq(Row(new java.math.BigDecimal("12.12345"))))
    assert(scanFilesCount(filter) == 1)

    filter = "boolean_col = true"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(8, true)))
    checkAnswer(sql(s"SELECT boolean_col from test_tbl WHERE $filter"), Seq(Row(true), Row(true)))
    assert(scanFilesCount(filter) == 2)

    filter = "date_col = date('2025-01-15')"
    actual = converter.convert(v2Filter(filter)).get
    val localDate = LocalDate.parse("2025-01-15")
    val epochDay = localDate.toEpochDay.toInt
    assert(actual.equals(builder.equal(9, epochDay)))
    checkAnswer(
      sql(s"SELECT date_col from test_tbl WHERE $filter"),
      sql("SELECT date('2025-01-15')"))
    assert(scanFilesCount(filter) == 1)

    filter = "binary_col = binary('b1')"
    assert(converter.convert(v2Filter(filter)).isEmpty)

    checkAnswer(sql(s"SELECT binary_col from test_tbl WHERE $filter"), sql("SELECT binary('b1')"))
    assert(scanFilesCount(filter) == 4)
  }

  test("V2Filter: timestamp and timestamp_ntz") {
    withTimeZone("Asia/Shanghai") {
      withTable("ts_tbl", "ts_ntz_tbl") {
        sql("CREATE TABLE ts_tbl (ts_col TIMESTAMP) USING paimon")
        sql("INSERT INTO ts_tbl VALUES (timestamp'2025-01-15 00:00:00.123')")
        sql("INSERT INTO ts_tbl VALUES (timestamp'2025-01-16 00:00:00.123')")

        val filter1 = "ts_col = timestamp'2025-01-15 00:00:00.123'"
        val rowType1 = loadTable("ts_tbl").rowType()
        val converter1 = SparkV2FilterConverter(rowType1)
        val actual1 = converter1.convert(v2Filter(filter1, "ts_tbl")).get
        if (treatPaimonTimestampTypeAsSparkTimestampType()) {
          assert(actual1.equals(new PredicateBuilder(rowType1)
            .equal(0, Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-15T00:00:00.123")))))
        } else {
          assert(actual1.equals(new PredicateBuilder(rowType1)
            .equal(0, Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-14T16:00:00.123")))))
        }
        checkAnswer(
          sql(s"SELECT ts_col from ts_tbl WHERE $filter1"),
          sql("SELECT timestamp'2025-01-15 00:00:00.123'"))
        assert(scanFilesCount(filter1, "ts_tbl") == 1)

        // Spark support TIMESTAMP_NTZ since Spark 3.4
        if (gteqSpark3_4) {
          sql("CREATE TABLE ts_ntz_tbl (ts_ntz_col TIMESTAMP_NTZ) USING paimon")
          sql("INSERT INTO ts_ntz_tbl VALUES (timestamp_ntz'2025-01-15 00:00:00.123')")
          sql("INSERT INTO ts_ntz_tbl VALUES (timestamp_ntz'2025-01-16 00:00:00.123')")
          val filter2 = "ts_ntz_col = timestamp_ntz'2025-01-15 00:00:00.123'"
          val rowType2 = loadTable("ts_ntz_tbl").rowType()
          val converter2 = SparkV2FilterConverter(rowType2)
          val actual2 = converter2.convert(v2Filter(filter2, "ts_ntz_tbl")).get
          assert(actual2.equals(new PredicateBuilder(rowType2)
            .equal(0, Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-15T00:00:00.123")))))
          checkAnswer(
            sql(s"SELECT ts_ntz_col from ts_ntz_tbl WHERE $filter2"),
            sql("SELECT timestamp_ntz'2025-01-15 00:00:00.123'"))
          assert(scanFilesCount(filter2, "ts_ntz_tbl") == 1)
        }
      }
    }
  }

  test("V2Filter: EqualTo") {
    val filter = "int_col = 1"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(3, 1)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(1)))
    assert(scanFilesCount(filter) == 1)
  }

  test("V2Filter: EqualNullSafe") {
    var filter = "int_col <=> 1"
    var actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.equal(3, 1)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(1)))
    assert(scanFilesCount(filter) == 1)

    filter = "int_col <=> null"
    actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.isNull(3)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(null)))
    assert(scanFilesCount(filter) == 1)
  }

  test("V2Filter: GreaterThan") {
    val filter = "int_col > 2"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.greaterThan(3, 2)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(3)))
    assert(scanFilesCount(filter) == 1)
  }

  test("V2Filter: GreaterThanOrEqual") {
    val filter = "int_col >= 2"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.greaterOrEqual(3, 2)))
    checkAnswer(
      sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"),
      Seq(Row(2), Row(3)))
    assert(scanFilesCount(filter) == 2)
  }

  test("V2Filter: LessThan") {
    val filter = "int_col < 2"
    val actual = converter.convert(v2Filter("int_col < 2")).get
    assert(actual.equals(builder.lessThan(3, 2)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(1)))
    assert(scanFilesCount(filter) == 1)
  }

  test("V2Filter: LessThanOrEqual") {
    val filter = "int_col <= 2"
    val actual = converter.convert(v2Filter("int_col <= 2")).get
    assert(actual.equals(builder.lessOrEqual(3, 2)))
    checkAnswer(
      sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col "),
      Seq(Row(1), Row(2)))
    assert(scanFilesCount(filter) == 2)
  }

  test("V2Filter: In") {
    val filter = "int_col IN (1, 2)"
    val actual = converter.convert(v2Filter("int_col IN (1, 2)")).get
    assert(actual.equals(builder.in(3, List(1, 2).map(_.asInstanceOf[AnyRef]).asJava)))
    checkAnswer(
      sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"),
      Seq(Row(1), Row(2)))
    assert(scanFilesCount(filter) == 2)
  }

  test("V2Filter: IsNull") {
    val filter = "int_col IS NULL"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.isNull(3)))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(null)))
    assert(scanFilesCount(filter) == 1)
  }

  test("V2Filter: IsNotNull") {
    val filter = "int_col IS NOT NULL"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.isNotNull(3)))
    checkAnswer(
      sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"),
      Seq(Row(1), Row(2), Row(3)))
    assert(scanFilesCount(filter) == 3)
  }

  test("V2Filter: And") {
    val filter = "int_col > 1 AND int_col < 3"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(PredicateBuilder.and(builder.greaterThan(3, 1), builder.lessThan(3, 3))))
    checkAnswer(sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"), Seq(Row(2)))
    assert(scanFilesCount(filter) == 1)
  }

  test("V2Filter: Or") {
    val filter = "int_col > 2 OR int_col IS NULL"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(PredicateBuilder.or(builder.greaterThan(3, 2), builder.isNull(3))))
    checkAnswer(
      sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"),
      Seq(Row(null), Row(3)))
    assert(scanFilesCount(filter) == 2)
  }

  test("V2Filter: Not") {
    val filter = "NOT (int_col > 2)"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.greaterThan(3, 2).negate().get()))
    checkAnswer(
      sql(s"SELECT int_col from test_tbl WHERE $filter ORDER BY int_col"),
      Seq(Row(1), Row(2)))
    assert(scanFilesCount(filter) == 2)
  }

  test("V2Filter: StartWith") {
    val filter = "string_col LIKE 'h%'"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.startsWith(0, BinaryString.fromString("h"))))
    checkAnswer(
      sql(s"SELECT string_col from test_tbl WHERE $filter ORDER BY string_col"),
      Seq(Row("hello"), Row("hi")))
    assert(scanFilesCount(filter) == 2)
  }

  test("V2Filter: EndWith") {
    val filter = "string_col LIKE '%o'"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.endsWith(0, BinaryString.fromString("o"))))
    checkAnswer(
      sql(s"SELECT string_col from test_tbl WHERE $filter ORDER BY string_col"),
      Seq(Row("hello")))
    // EndWith does not have file skipping effect now.
    assert(scanFilesCount(filter) == 4)
  }

  test("V2Filter: Contains") {
    val filter = "string_col LIKE '%e%'"
    val actual = converter.convert(v2Filter(filter)).get
    assert(actual.equals(builder.contains(0, BinaryString.fromString("e"))))
    checkAnswer(
      sql(s"SELECT string_col from test_tbl WHERE $filter ORDER BY string_col"),
      Seq(Row("hello")))
    // Contains does not have file skipping effect now.
    assert(scanFilesCount(filter) == 4)
  }

  private def v2Filter(str: String, tableName: String = "test_tbl"): Predicate = {
    val condition = sql(s"SELECT * FROM $tableName WHERE $str").queryExecution.optimizedPlan
      .collectFirst { case f: Filter => f }
      .get
      .condition
    translateFilterV2(condition).get
  }

  private def scanFilesCount(str: String, tableName: String = "test_tbl"): Int = {
    getPaimonScan(s"SELECT * FROM $tableName WHERE $str").inputPartitions
      .flatMap(_.splits)
      .map(_.asInstanceOf[DataSplit].dataFiles().size())
      .sum
  }
}
