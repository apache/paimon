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
import org.apache.paimon.types.RowType

import org.apache.spark.SparkConf
import org.apache.spark.sql.PaimonUtils.translateFilterV2
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
          | binary BINARY
          |) USING paimon
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
    var actual = converter.convert(v2Filter("string_col = 'hello'"))
    assert(actual.equals(builder.equal(0, BinaryString.fromString("hello"))))

    actual = converter.convert(v2Filter("byte_col = 1"))
    assert(actual.equals(builder.equal(1, 1.toByte)))

    actual = converter.convert(v2Filter("short_col = 1"))
    assert(actual.equals(builder.equal(2, 1.toShort)))

    actual = converter.convert(v2Filter("int_col = 1"))
    assert(actual.equals(builder.equal(3, 1)))

    actual = converter.convert(v2Filter("long_col = 1"))
    assert(actual.equals(builder.equal(4, 1L)))

    actual = converter.convert(v2Filter("float_col = 1.0"))
    assert(actual.equals(builder.equal(5, 1.0f)))

    actual = converter.convert(v2Filter("double_col = 1.0"))
    assert(actual.equals(builder.equal(6, 1.0d)))

    actual = converter.convert(v2Filter("decimal_col = 12.12345"))
    assert(
      actual.equals(
        builder.equal(7, Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5))))

    actual = converter.convert(v2Filter("boolean_col = true"))
    assert(actual.equals(builder.equal(8, true)))

    actual = converter.convert(v2Filter("date_col = cast('2025-01-15' as date)"))
    val localDate = LocalDate.parse("2025-01-15")
    val epochDay = localDate.toEpochDay.toInt
    assert(actual.equals(builder.equal(9, epochDay)))

    intercept[UnsupportedOperationException] {
      actual = converter.convert(v2Filter("binary = binary('b1')"))
    }
  }

  test("V2Filter: timestamp and timestamp_ntz") {
    withTimeZone("Asia/Shanghai") {
      withTable("ts_tbl", "ts_ntz_tbl") {
        sql("CREATE TABLE ts_tbl (ts_col TIMESTAMP) USING paimon")
        val rowType1 = loadTable("ts_tbl").rowType()
        val converter1 = SparkV2FilterConverter(rowType1)
        val actual1 =
          converter1.convert(v2Filter("ts_col = timestamp'2025-01-15 00:00:00.123'", "ts_tbl"))
        assert(
          actual1.equals(new PredicateBuilder(rowType1)
            .equal(0, Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-14T16:00:00.123")))))

        // Spark support TIMESTAMP_NTZ since Spark 3.4
        if (gteqSpark3_4) {
          sql("CREATE TABLE ts_ntz_tbl (ts_ntz_col TIMESTAMP_NTZ) USING paimon")
          val rowType2 = loadTable("ts_ntz_tbl").rowType()
          val converter2 = SparkV2FilterConverter(rowType2)
          val actual2 = converter2.convert(
            v2Filter("ts_ntz_col = timestamp_ntz'2025-01-15 00:00:00.123'", "ts_ntz_tbl"))
          assert(actual2.equals(new PredicateBuilder(rowType2)
            .equal(0, Timestamp.fromLocalDateTime(LocalDateTime.parse("2025-01-15T00:00:00.123")))))
        }
      }
    }
  }

  test("V2Filter: EqualTo") {
    val actual = converter.convert(v2Filter("int_col = 1"))
    assert(actual.equals(builder.equal(3, 1)))
  }

  test("V2Filter: EqualNullSafe") {
    var actual = converter.convert(v2Filter("int_col <=> 1"))
    assert(actual.equals(builder.equal(3, 1)))

    actual = converter.convert(v2Filter("int_col <=> null"))
    assert(actual.equals(builder.isNull(3)))
  }

  test("V2Filter: GreaterThan") {
    val actual = converter.convert(v2Filter("int_col > 1"))
    assert(actual.equals(builder.greaterThan(3, 1)))
  }

  test("V2Filter: GreaterThanOrEqual") {
    val actual = converter.convert(v2Filter("int_col >= 1"))
    assert(actual.equals(builder.greaterOrEqual(3, 1)))
  }

  test("V2Filter: LessThan") {
    val actual = converter.convert(v2Filter("int_col < 1"))
    assert(actual.equals(builder.lessThan(3, 1)))
  }

  test("V2Filter: LessThanOrEqual") {
    val actual = converter.convert(v2Filter("int_col <= 1"))
    assert(actual.equals(builder.lessOrEqual(3, 1)))
  }

  test("V2Filter: In") {
    val actual = converter.convert(v2Filter("int_col IN (1, 2, 3)"))
    assert(actual.equals(builder.in(3, List(1, 2, 3).map(_.asInstanceOf[AnyRef]).asJava)))
  }

  test("V2Filter: IsNull") {
    val actual = converter.convert(v2Filter("int_col IS NULL"))
    assert(actual.equals(builder.isNull(3)))
  }

  test("V2Filter: IsNotNull") {
    val actual = converter.convert(v2Filter("int_col IS NOT NULL"))
    assert(actual.equals(builder.isNotNull(3)))
  }

  test("V2Filter: And") {
    val actual = converter.convert(v2Filter("int_col > 1 AND int_col < 10"))
    assert(actual.equals(PredicateBuilder.and(builder.greaterThan(3, 1), builder.lessThan(3, 10))))
  }

  test("V2Filter: Or") {
    val actual = converter.convert(v2Filter("int_col > 1 OR int_col < 10"))
    assert(actual.equals(PredicateBuilder.or(builder.greaterThan(3, 1), builder.lessThan(3, 10))))
  }

  test("V2Filter: Not") {
    val actual = converter.convert(v2Filter("NOT (int_col > 1)"))
    assert(actual.equals(builder.greaterThan(3, 1).negate().get()))
  }

  test("V2Filter: StartWith") {
    val actual = converter.convert(v2Filter("string_col LIKE 'h%'"))
    assert(actual.equals(builder.startsWith(0, BinaryString.fromString("h"))))
  }

  test("V2Filter: EndWith") {
    val actual = converter.convert(v2Filter("string_col LIKE '%o'"))
    assert(actual.equals(builder.endsWith(0, BinaryString.fromString("o"))))
  }

  test("V2Filter: Contains") {
    val actual = converter.convert(v2Filter("string_col LIKE '%e%'"))
    assert(actual.equals(builder.contains(0, BinaryString.fromString("e"))))
  }

  private def v2Filter(str: String, tableName: String = "test_tbl"): Predicate = {
    val condition = sql(s"SELECT * FROM $tableName WHERE $str").queryExecution.optimizedPlan
      .collectFirst { case f: Filter => f }
      .get
      .condition
    translateFilterV2(condition).get
  }
}
