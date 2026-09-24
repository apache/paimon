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

import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.predicate.FieldTransform
import org.apache.paimon.spark.{PaimonSparkTestBase, SparkTypeUtils}
import org.apache.paimon.spark.util.SparkExpressionConverter
import org.apache.paimon.types.{DataTypes => PaimonTypes, RowType}

import org.apache.spark.sql.catalyst.expressions.{Cast => CatalystCast, Literal => CatalystLiteral}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.connector.expressions.{Cast, Expressions}
import org.apache.spark.sql.types._

import java.util.{Objects, TimeZone}

/** SQL casts must remain in Spark unless the storage transform has equivalent semantics. */
class CastPredicatePushDownTest extends PaimonSparkTestBase {

  private val integralTypes = Seq(ByteType, ShortType, IntegerType, LongType)

  test("safe CAST conversion agrees with Catalyst for boundary values and nulls") {
    val values: Seq[(DataType, Seq[Any])] = Seq(
      ByteType -> Seq(Byte.MinValue, 0.toByte, Byte.MaxValue, null),
      ShortType -> Seq(Short.MinValue, 0.toShort, Short.MaxValue, null),
      IntegerType -> Seq(Int.MinValue, -1, 0, Int.MaxValue, null),
      LongType -> Seq(Long.MinValue, -1L, 0L, 16777217L, Long.MaxValue, null),
      BooleanType -> Seq(true, false, null)
    )
    for ((source, inputs) <- values; ansi <- Seq(false, true)) {
      val targets = source match {
        case BooleanType => Seq(BooleanType, StringType)
        case _ =>
          integralTypes
            .drop(integralTypes.indexOf(source)) ++ Seq(StringType)
      }
      withSparkSQLConf("spark.sql.ansi.enabled" -> ansi.toString) {
        val rowType = RowType.of(Array(SparkTypeUtils.toPaimonType(source)), Array("p"))
        for (target <- targets) {
          val transform = SparkExpressionConverter.toPaimonTransform(
            new Cast(Expressions.column("p"), target),
            rowType)
          assert(transform.nonEmpty, s"$source -> $target")
          for (value <- inputs) {
            val result =
              CatalystCast(CatalystLiteral.create(value, source), target, Some("UTC")).eval()
            val expected =
              if (result == null) null
              else if (target == StringType) BinaryString.fromString(result.toString)
              else result.asInstanceOf[AnyRef]
            val actual = transform.get.transform(GenericRow.of(value.asInstanceOf[AnyRef]))
            assert(Objects.equals(actual, expected), s"$source -> $target: $value, ANSI=$ansi")
          }
        }
      }
    }
  }

  test("unsafe CAST conversions are declined instead of using storage casts") {
    val unsafe: Seq[(DataType, DataType)] = Seq(
      IntegerType -> ByteType,
      LongType -> IntegerType,
      DoubleType -> FloatType,
      FloatType -> IntegerType,
      DoubleType -> LongType,
      StringType -> IntegerType,
      StringType -> LongType,
      StringType -> DoubleType,
      StringType -> BooleanType,
      StringType -> DateType,
      StringType -> TimestampType,
      TimestampType -> StringType,
      TimestampType -> DateType,
      DateType -> TimestampType,
      DateType -> StringType,
      IntegerType -> TimestampType,
      FloatType -> StringType,
      DoubleType -> StringType,
      DecimalType(10, 2) -> DecimalType(5, 1),
      DecimalType(10, 2) -> LongType,
      LongType -> DecimalType(5, 0)
    )
    for ((source, target) <- unsafe) {
      val rowType = RowType.of(Array(SparkTypeUtils.toPaimonType(source)), Array("p"))
      assert(
        SparkExpressionConverter
          .toPaimonTransform(new Cast(Expressions.column("p"), target), rowType)
          .isEmpty,
        s"$source -> $target")
    }
  }

  test("sub-microsecond timestamp CAST must not be treated as a field identity") {
    if (gteqSpark3_4) {
      for (precision <- 7 to 9; legacy <- Seq(false, true)) {
        withSparkSQLConf("spark.paimon.legacy-timestamp-mapping.enabled" -> legacy.toString) {
          for (
            source <- Seq(
              PaimonTypes.TIMESTAMP(precision),
              PaimonTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision))
          ) {
            val rowType = RowType.of(Array[org.apache.paimon.types.DataType](source), Array("p"))
            val exposed = SparkTypeUtils.fromPaimonType(source)
            assert(
              SparkExpressionConverter
                .toPaimonTransform(new Cast(Expressions.column("p"), exposed), rowType)
                .isEmpty)
          }
        }
      }
    }
  }

  test("floating-result CAST predicates stay in Spark because signed zero comparisons differ") {
    for (
      source <- integralTypes ++ Seq(FloatType, DoubleType); target <- Seq(FloatType, DoubleType)
    ) {
      val rowType = RowType.of(Array(SparkTypeUtils.toPaimonType(source)), Array("p"))
      assert(
        SparkExpressionConverter
          .toPaimonTransform(new Cast(Expressions.column("p"), target), rowType)
          .isEmpty,
        s"$source -> $target")
    }
    val sparkEquality = org.apache.spark.sql.catalyst.expressions.EqualTo(
      CatalystCast(CatalystLiteral.create(-0.0f, FloatType), DoubleType, Some("UTC")),
      CatalystLiteral.create(0.0d, DoubleType))
    assert(sparkEquality.eval() == true)
  }

  test("storage TIME exposed as Spark INT must not use clock-format casts") {
    val rowType =
      RowType.of(Array[org.apache.paimon.types.DataType](PaimonTypes.TIME(3)), Array("p"))
    assert(
      SparkExpressionConverter
        .toPaimonTransform(new Cast(Expressions.column("p"), IntegerType), rowType)
        .get
        .isInstanceOf[FieldTransform])
    assert(
      SparkExpressionConverter
        .toPaimonTransform(new Cast(Expressions.column("p"), StringType), rowType)
        .isEmpty)
  }

  test("timestamp identity is based on the Spark exposed type") {
    if (gteqSpark3_4) {
      for (legacy <- Seq(false, true)) {
        withSparkSQLConf("spark.paimon.legacy-timestamp-mapping.enabled" -> legacy.toString) {
          val rowType = RowType.of(
            Array[org.apache.paimon.types.DataType](PaimonTypes.TIMESTAMP(6)),
            Array("p"))
          val exposed = SparkTypeUtils.fromPaimonType(rowType.getTypeAt(0))
          assert(
            SparkExpressionConverter
              .toPaimonTransform(new Cast(Expressions.column("p"), exposed), rowType)
              .get
              .isInstanceOf[FieldTransform])
          val different = if (legacy) TimestampNTZType else TimestampType
          assert(
            SparkExpressionConverter
              .toPaimonTransform(new Cast(Expressions.column("p"), different), rowType)
              .isEmpty)
        }
      }
    }
  }

  for (format <- Seq("parquet", "orc"); partitioned <- Seq(false, true); ansi <- Seq(false, true)) {
    val context = s"$format, partitioned=$partitioned, ANSI=$ansi"

    test(s"CAST preserves NULL and safe results: $context") {
      withInput("INT", "(1, NULL), (2, -1), (3, 0), (4, 128)", format, partitioned, ansi) {
        for (
          predicate <- Seq(
            "CAST(p AS BIGINT) IS NULL",
            "CAST(p AS STRING) IS NULL",
            "CAST(p AS STRING) = '128'",
            "NOT (CAST(p AS STRING) <=> '128')",
            "CAST(p AS BIGINT) > 0")
        ) {
          checkAgainstSpark(predicate)
        }
      }
    }

    test(s"CAST overflow and decimal narrowing follow Spark: $context") {
      withInput("INT", "(1, 128)", format, partitioned, ansi) {
        if (ansi) {
          assertSameError("CAST(p AS TINYINT) = -128", "CAST_OVERFLOW")
        } else {
          checkAgainstSpark("CAST(p AS TINYINT) = -128")
        }
      }
      withInput("DECIMAL(6,2)", "(1, 9999.99), (2, NULL)", format, partitioned, ansi) {
        if (ansi) {
          val reference = intercept[Exception](
            sql("SELECT id FROM cast_reference WHERE CAST(p AS DECIMAL(3,1)) IS NULL").collect())
          val actual = intercept[Exception](
            sql("SELECT id FROM cast_subject WHERE CAST(p AS DECIMAL(3,1)) IS NULL").collect())
          assert(allMessages(reference).contains("NUMERIC_VALUE_OUT_OF_RANGE"))
          assert(allMessages(actual).contains("NUMERIC_VALUE_OUT_OF_RANGE"))
        } else {
          checkAgainstSpark("CAST(p AS DECIMAL(3,1)) IS NULL")
        }
      }
    }

    test(s"parsed string CAST remains in Spark: $context") {
      withInput("STRING", "(1, '1970'), (2, '2024-01-02'), (3, NULL)", format, partitioned, ansi) {
        checkAgainstSpark("CAST(p AS DATE) = DATE '1970-01-01'")
        checkAgainstSpark("CAST(p AS DATE) IS NULL")
        assert(
          sql(
            "SELECT id FROM cast_subject WHERE CAST(p AS DATE) = DATE '1970-01-01'").queryExecution.optimizedPlan
            .exists(_.isInstanceOf[Filter]))
      }
      withInput("STRING", "(1, 'bad'), (2, '123'), (3, NULL)", format, partitioned, ansi) {
        if (ansi) assertSameError("CAST(p AS INT) IS NULL", "CAST_INVALID_INPUT")
        else checkAgainstSpark("CAST(p AS INT) IS NULL")
      }
    }
  }

  test("timestamp CAST uses Spark formatting and session time zone") {
    val previous = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
      for (zone <- Seq("UTC", "Asia/Shanghai"); partitioned <- Seq(false, true)) {
        withSparkSQLConf("spark.sql.session.timeZone" -> zone) {
          withInput(
            "TIMESTAMP",
            "(1, TIMESTAMP '2024-01-01 00:30:00'), (2, NULL)",
            "parquet",
            partitioned,
            true) {
            checkAgainstSpark("CAST(p AS STRING) = '2024-01-01 00:30:00'")
            checkAgainstSpark("CAST(p AS STRING) IS NULL")
          }
        }
      }
    } finally TimeZone.setDefault(previous)
  }

  test("safe partition CAST retains pruning and unsafe CAST retains residual") {
    withInput("INT", "(1, 1), (2, 2), (3, 3), (4, NULL)", "parquet", true, true) {
      val all = getPaimonScan("SELECT * FROM cast_subject").inputSplits.length
      val query = "SELECT * FROM cast_subject WHERE CAST(p AS STRING) = '2'"
      val scan = getPaimonScan(query)
      assert(!sql(query).queryExecution.optimizedPlan.exists(_.isInstanceOf[Filter]))
      assert(scan.inputSplits.nonEmpty && scan.inputSplits.length < all)
      checkAgainstSpark("CAST(p AS STRING) = '2'")
    }
    withInput("STRING", "(1, '1970'), (2, '2024-01-02')", "parquet", true, true) {
      val query = "SELECT * FROM cast_subject WHERE CAST(p AS DATE) = DATE '1970-01-01'"
      assert(sql(query).queryExecution.optimizedPlan.exists(_.isInstanceOf[Filter]))
      checkAgainstSpark("CAST(p AS DATE) = DATE '1970-01-01'")
    }
  }

  private def withInput(
      source: String,
      values: String,
      format: String,
      partitioned: Boolean,
      ansi: Boolean)(body: => Unit): Unit = {
    withSparkSQLConf("spark.sql.ansi.enabled" -> ansi.toString) {
      withTable("cast_subject") {
        val input = sql(s"SELECT id, CAST(p AS $source) AS p FROM VALUES $values AS input(id,p)")
        input.createOrReplaceTempView("cast_reference")
        try {
          val partition = if (partitioned) "PARTITIONED BY (p)" else ""
          sql(
            s"CREATE TABLE cast_subject (id INT, p $source) USING paimon $partition TBLPROPERTIES ('bucket'='-1','file.format'='$format')")
          sql("INSERT INTO cast_subject SELECT * FROM cast_reference")
          body
        } finally spark.catalog.dropTempView("cast_reference")
      }
    }
  }

  private def checkAgainstSpark(predicate: String): Unit = {
    val expected = sql(s"SELECT id, p FROM cast_reference WHERE $predicate").collect().toSeq
    checkAnswer(sql(s"SELECT id, p FROM cast_subject WHERE $predicate"), expected)
  }

  private def allMessages(error: Throwable): String = {
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString("\n")
  }

  private def assertSameError(predicate: String, errorClass: String): Unit = {
    val expected =
      intercept[Exception](sql(s"SELECT id FROM cast_reference WHERE $predicate").collect())
    val actual =
      intercept[Exception](sql(s"SELECT id FROM cast_subject WHERE $predicate").collect())
    assert(allMessages(expected).contains(errorClass))
    assert(allMessages(actual).contains(errorClass))
  }
}
