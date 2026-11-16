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

package org.apache.paimon.spark.predicate

import org.apache.paimon.data.{BinaryRow, BinaryString, Decimal => PaimonDecimal, GenericArray, GenericRow, InternalArray, InternalRow, Timestamp => PaimonTimestamp}
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.format.SimpleStatsCollector
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.stats.SimpleStatsConverter
import org.apache.paimon.types.{DataType => PaimonType, DataTypes, RowType}
import org.apache.paimon.utils.InstantiationUtil

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Coalesce, Contains, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, ScalaUDF, StartsWith, Upper}
import org.apache.spark.sql.catalyst.filter.SparkCatalystPartitionPredicate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.util.concurrent.{Callable, ConcurrentHashMap, CyclicBarrier, Executors, TimeUnit}

import scala.util.control.NonFatal

/** No false-negative pruning: actual matches or evaluation errors must survive statistics. */
class SparkCatalystPartitionStatsTest extends PaimonSparkTestBase {

  private val partitionType = RowType.of(
    Array[PaimonType](DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()),
    Array("dt", "region", "n"))
  private val dt = AttributeReference("dt", StringType)()
  private val region = AttributeReference("region", StringType)()
  private val n = AttributeReference("n", IntegerType)()

  private def cast(ansi: Boolean = false): Expression =
    Compatibility.cast(dt, IntegerType, ansiEnabled = ansi)

  private def row(value: String, area: String = "a", number: Int = 0): GenericRow = {
    GenericRow.of(
      if (value == null) null else BinaryString.fromString(value),
      if (area == null) null else BinaryString.fromString(area),
      Int.box(number))
  }

  private case class Sample(
      count: Long,
      min: InternalRow,
      max: InternalRow,
      nulls: InternalArray,
      partitions: Seq[BinaryRow]) {
    def mayMatch(predicate: SparkCatalystPartitionPredicate): Boolean =
      predicate.test(count, min, max, nulls)
  }

  private def sample(rows: Seq[GenericRow], rowType: RowType = partitionType): Sample = {
    val collector = new SimpleStatsCollector(rowType)
    rows.foreach(collector.collect)
    val stats = new SimpleStatsConverter(rowType).toBinaryAllMode(collector.extract())
    val serializer = new InternalRowSerializer(rowType)
    Sample(
      rows.size,
      stats.minValues(),
      stats.maxValues(),
      stats.nullCounts(),
      rows.map(r => serializer.toBinaryRow(r).copy()))
  }

  private def mayMatch(expression: Expression, rows: GenericRow*): Boolean =
    sample(rows).mayMatch(SparkCatalystPartitionPredicate(expression, partitionType))

  test("native leaf statistics handle non-singleton intervals") {
    val data = Seq(row("10", number = 10), row("20", number = 20))
    Seq[Expression](
      EqualTo(n, Literal(30)),
      GreaterThan(n, Literal(20)),
      GreaterThanOrEqual(n, Literal(21)),
      LessThan(n, Literal(10)),
      LessThanOrEqual(n, Literal(9)),
      GreaterThan(Literal(10), n),
      In(n, Seq(Literal(1), Literal(30))),
      IsNull(n),
      Not(GreaterThanOrEqual(n, Literal(10))),
      Not(In(n, Seq(Literal(10), Literal(20), Literal.create(null, IntegerType))))
    )
      .foreach(e => assert(!mayMatch(e, data: _*), e.toString))
    assert(mayMatch(EqualTo(n, Literal(15)), data: _*)) // range overlap is only a possibility
    assert(mayMatch(EqualTo(dt, Literal("15")), data: _*))
    assert(!mayMatch(EqualTo(dt, Literal("30")), data: _*))
    assert(!mayMatch(StartsWith(dt, Literal("z")), data: _*))
    // No substring inference from just the endpoints.
    assert(mayMatch(Contains(dt, Literal("x")), data: _*))
  }

  test("all comparison operators, IN and NOT retain original CAST semantics on finite domains") {
    Seq[Expression](
      EqualTo(cast(), Literal(9)),
      GreaterThan(cast(), Literal(10)),
      GreaterThanOrEqual(cast(), Literal(11)),
      LessThan(cast(), Literal(10)),
      LessThanOrEqual(cast(), Literal(9)),
      Not(EqualTo(cast(), Literal(10))),
      In(cast(), Seq(Literal(8), Literal(9))),
      Not(In(cast(), Seq(Literal(10), Literal.create(null, IntegerType))))
    )
      .foreach(e => assert(!mayMatch(e, row("10"), row(null)), e.toString))
    Seq("010", "+10", " 10 ").foreach {
      value => assert(mayMatch(EqualTo(cast(), Literal(10)), row(value)))
    }
  }

  test("foldable Spark functions are not limited to casts or single referenced columns") {
    assert(!mayMatch(EqualTo(Upper(dt), Literal("B")), row("a")))
    assert(mayMatch(EqualTo(Upper(dt), Literal("A")), row("a")))
    val coalesced = Coalesce(Seq(dt, region, Literal("fallback")))
    assert(!mayMatch(EqualTo(coalesced, Literal("z")), row("a", null), row(null, "b")))
    assert(mayMatch(EqualTo(coalesced, Literal("fallback")), row(null, null)))
    assert(!mayMatch(EqualTo(cast(), Literal(11)), row("10", "a"), row("10", "z")))
  }

  test("NULL combinations are enumerated, not replaced by a single min/max row") {
    val data = Seq(row("a", null), row(null, "b"))
    assert(mayMatch(And(IsNotNull(dt), IsNull(region)), data: _*))
    assert(mayMatch(And(IsNull(dt), IsNotNull(region)), data: _*))
    // The Cartesian superset also contains (a,b) and (NULL,NULL). No correlation is assumed.
    assert(mayMatch(And(IsNotNull(dt), IsNotNull(region)), data: _*))
    assert(mayMatch(EqualNullSafe(dt, region), data: _*))
    assert(!mayMatch(EqualTo(dt, region), data: _*))
    assert(!mayMatch(Not(EqualTo(dt, Literal("a"))), data: _*))
    assert(mayMatch(Not(EqualNullSafe(dt, Literal("a"))), data: _*))
    assert(mayMatch(IsNull(cast()), row("invalid"), row(null)))
    assert(!mayMatch(IsNotNull(cast()), row("invalid"), row(null)))
  }

  test("AND OR NOT use conservative logic, never boolean negation of may-match") {
    val data = Seq(row("10", number = 1), row("10", number = 3))
    val impossible = GreaterThan(cast(), Literal(50))
    val possible = EqualTo(n, Literal(2))
    assert(!mayMatch(And(impossible, possible), data: _*))
    assert(mayMatch(Or(impossible, possible), data: _*))
    assert(mayMatch(Not(possible), data: _*))
    assert(mayMatch(Not(Not(possible)), data: _*))
    assert(!mayMatch(Not(Or(EqualTo(cast(), Literal(10)), possible)), data: _*))
    assert(mayMatch(Not(And(impossible, possible)), data: _*))
  }

  test("string intervals never become numeric intervals, even with digit-only endpoints") {
    val data = Seq(row("10"), row("100"), row("20"))
    assert(mayMatch(EqualTo(cast(), Literal(100)), data: _*))
    assert(mayMatch(GreaterThanOrEqual(cast(), Literal(50)), data: _*))
    // All endpoint strings have 8 digits; the interior value does not.
    val dates = Seq(row("20261112"), row("202611120"), row("20261114"))
    assert(mayMatch(GreaterThan(cast(), Literal(30000000)), dates: _*))
  }

  test("ANSI failures are not hidden by statistics or an unrelated impossible conjunct") {
    val bad = EqualTo(cast(ansi = true), Literal(10))
    Seq("invalid", "2147483648").foreach {
      value =>
        assert(mayMatch(bad, row(value)))
        assert(mayMatch(And(bad, EqualTo(n, Literal(100))), row(value, number = 1)))
        assert(!mayMatch(EqualTo(cast(), Literal(10)), row(value)))
    }
    // Unknown interval values might throw too; an unrelated child is not permission to hide it.
    assert(
      mayMatch(And(bad, EqualTo(n, Literal(100))), row("10", number = 1), row("20", number = 2)))
  }

  test("missing inconsistent and unsupported statistics conservatively retain manifests") {
    val predicate =
      SparkCatalystPartitionPredicate(GreaterThanOrEqual(cast(), Literal(10)), partitionType)
    val data = sample(Seq(row("1"), row("2")))
    assert(predicate.test(0, data.min, data.max, data.nulls))
    assert(predicate.test(-1, data.min, data.max, data.nulls))
    assert(predicate.test(data.count, null, data.max, data.nulls))
    assert(predicate.test(data.count, data.min, data.max, null))
    assert(predicate.test(data.count, data.max, data.min, data.nulls))
    Seq[AnyRef](null, Long.box(-1), Long.box(3)).foreach {
      count =>
        val counts = new GenericArray(Array[AnyRef](count, Long.box(0), Long.box(0)))
        assert(predicate.test(data.count, data.min, data.max, counts))
    }
    val floats = RowType.of(Array[PaimonType](DataTypes.DOUBLE()), Array("x"))
    val floatPredicate = SparkCatalystPartitionPredicate(
      LessThan(AttributeReference("x", DoubleType)(), Literal(0.0)),
      floats)
    assert(sample(Seq(GenericRow.of(Double.box(Double.NaN))), floats).mayMatch(floatPredicate))
    val binary = RowType.of(Array[PaimonType](DataTypes.BYTES()), Array("x"))
    val binaryPredicate = SparkCatalystPartitionPredicate(
      EqualTo(AttributeReference("x", BinaryType)(), Literal(Array[Byte](1))),
      binary)
    assert(sample(Seq(GenericRow.of(Array[Byte](-1))), binary).mayMatch(binaryPredicate))
  }

  test("finite-domain work limit is a fallback, never a reason to drop a manifest") {
    val names = (0 until 7).map(i => s"p$i").toArray
    val rowType = RowType.of(names.map(_ => DataTypes.STRING(): PaimonType), names)
    val nonNull = GenericRow.of(names.map(_ => BinaryString.fromString("a"): AnyRef): _*)
    val nulls = new GenericRow(names.length)
    val expression = EqualTo(
      Coalesce(names.map(name => AttributeReference(name, StringType)()).toSeq),
      Literal("z"))
    assert(
      sample(Seq(nonNull, nulls), rowType)
        .mayMatch(SparkCatalystPartitionPredicate(expression, rowType)))
  }

  test("short statistics retain manifests even when the referenced field is present") {
    val data = sample(Seq(row("10")))
    val predicate = SparkCatalystPartitionPredicate(EqualTo(cast(), Literal(11)), partitionType)
    assert(!data.mayMatch(predicate))
    for (size <- 0 until partitionType.getFieldCount) {
      val shortType = partitionType.project((0 until size).toArray)
      val shortRow = new GenericRow(size)
      if (size > 0) shortRow.setField(0, BinaryString.fromString("10"))
      if (size > 1) shortRow.setField(1, BinaryString.fromString("a"))
      val shortBinary = new InternalRowSerializer(shortType).toBinaryRow(shortRow).copy()
      Seq[InternalRow](shortRow, shortBinary).foreach {
        short =>
          assert(predicate.test(data.count, short, data.max, data.nulls))
          assert(predicate.test(data.count, data.min, short, data.nulls))
      }
      val shortNulls = new GenericArray(Array.fill[AnyRef](size)(Long.box(0)))
      assert(predicate.test(data.count, data.min, data.max, shortNulls))
    }
  }

  test("query SQLConf is initialized once and shared read-only across worker threads") {
    val observed = ConcurrentHashMap.newKeySet[SQLConf]()
    val expression = ScalaUDF(
      (_: String) => {
        observed.add(SQLConf.get)
        SQLConf.get.sessionLocalTimeZone == "UTC"
      },
      BooleanType,
      Seq(dt))
    var predicate: SparkCatalystPartitionPredicate = null
    withSparkSQLConf(
      "spark.sql.session.timeZone" -> "UTC",
      "spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
      predicate = SparkCatalystPartitionPredicate(expression, partitionType)
    }
    val data = sample(Seq(row("10")))
    val executor = Executors.newFixedThreadPool(4)
    val barrier = new CyclicBarrier(4)
    try {
      val results = (1 to 4).map {
        _ =>
          executor.submit(new Callable[Boolean] {
            override def call(): Boolean = {
              val caller = new SQLConf
              caller.setConfString("spark.sql.session.timeZone", "Asia/Shanghai")
              SQLConf.withExistingConf(caller) {
                barrier.await(30, TimeUnit.SECONDS)
                (1 to 10).forall {
                  _ => data.partitions.forall(predicate.test) && (SQLConf.get eq caller)
                }
              }
            }
          })
      }
      results.foreach(result => assert(result.get(30, TimeUnit.SECONDS)))
      assert(observed.size() == 1)
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  test("timestamp precision, signed zero and binary ordering are not blindly delegated") {
    val timestampType = RowType.of(Array[PaimonType](DataTypes.TIMESTAMP(9)), Array("x"))
    val timestamps = sample(
      Seq(
        GenericRow.of(PaimonTimestamp.fromEpochMillis(1000, 1)),
        GenericRow.of(PaimonTimestamp.fromEpochMillis(2000))),
      timestampType)
    val timestampPredicate = SparkCatalystPartitionPredicate(
      EqualTo(AttributeReference("x", TimestampType)(), Literal.create(1000000L, TimestampType)),
      timestampType)
    // The first partition becomes exactly 1 second in Spark, despite a larger Paimon min.
    assert(timestamps.partitions.exists(timestampPredicate.test))
    assert(timestamps.mayMatch(timestampPredicate))
    val floatType = RowType.of(Array[PaimonType](DataTypes.DOUBLE()), Array("x"))
    val negativeZero =
      sample(Seq(GenericRow.of(Double.box(-0.0)), GenericRow.of(Double.box(0.0))), floatType)
    assert(
      negativeZero.mayMatch(
        SparkCatalystPartitionPredicate(
          GreaterThan(AttributeReference("x", DoubleType)(), Literal(1.0)),
          floatType)))
  }

  test("native reuse agrees with Spark for each enabled statistics type") {
    val decimalType = DecimalType(10, 2)
    val cases: Seq[(PaimonType, DataType, AnyRef, AnyRef, Any)] = Seq(
      (DataTypes.BOOLEAN(), BooleanType, Boolean.box(false), Boolean.box(true), true),
      (DataTypes.TINYINT(), ByteType, Byte.box(-10), Byte.box(10), 20.toByte),
      (DataTypes.SMALLINT(), ShortType, Short.box(-10), Short.box(10), 20.toShort),
      (
        DataTypes.INT(),
        IntegerType,
        Int.box(Int.MinValue),
        Int.box(Int.MaxValue - 1),
        Int.MaxValue),
      (
        DataTypes.BIGINT(),
        LongType,
        Long.box(Long.MinValue),
        Long.box(Long.MaxValue - 1),
        Long.MaxValue),
      (DataTypes.DATE(), DateType, Int.box(-10), Int.box(10), 20),
      (
        DataTypes.DECIMAL(10, 2),
        decimalType,
        PaimonDecimal.fromBigDecimal(new java.math.BigDecimal("-1.25"), 10, 2),
        PaimonDecimal.fromBigDecimal(new java.math.BigDecimal("1.25"), 10, 2),
        Decimal(new java.math.BigDecimal("2.25"), 10, 2)),
      (
        DataTypes.STRING(),
        StringType,
        BinaryString.fromString("海信"),
        BinaryString.fromString("\uD83D\uDE00"),
        org.apache.spark.unsafe.types.UTF8String.fromString("\uFFFF"))
    )
    cases.foreach {
      case (paimonType, sparkType, lower, upper, value) =>
        val rowType = RowType.of(Array(paimonType), Array("x"))
        val data =
          sample(Seq(GenericRow.of(lower), GenericRow.of(upper), GenericRow.of(null)), rowType)
        val attribute = AttributeReference("x", sparkType)()
        val literal = Literal.create(value, sparkType)
        Seq[Expression](
          EqualTo(attribute, literal),
          LessThan(attribute, literal),
          GreaterThanOrEqual(attribute, literal),
          Not(EqualTo(attribute, literal)),
          IsNull(attribute))
          .foreach {
            expression =>
              val predicate = SparkCatalystPartitionPredicate(expression, rowType)
              assert(
                !data.partitions.exists(predicate.test) || data.mayMatch(predicate),
                expression.toString)
          }
    }
  }

  test("malformed UTF-8 literals are not changed by native conversion") {
    val rowType = RowType.of(Array[PaimonType](DataTypes.STRING()), Array("x"))
    val data = sample(
      Seq(0xfd, 0xfe, 0xff).map(b => GenericRow.of(BinaryString.fromBytes(Array(b.toByte)))),
      rowType)
    val literal = Literal.create(
      org.apache.spark.unsafe.types.UTF8String.fromBytes(Array(0xfe.toByte)),
      StringType)
    val predicate = SparkCatalystPartitionPredicate(
      EqualTo(AttributeReference("x", StringType)(), literal),
      rowType)
    assert(data.partitions.exists(predicate.test))
    assert(data.mayMatch(predicate))
  }

  test("serialized statistics evaluator preserves compound predicate results") {
    val original = SparkCatalystPartitionPredicate(
      Or(GreaterThan(cast(), Literal(50)), LessThan(n, Literal(0))),
      partitionType)
    val data = sample(Seq(row("10", number = 1), row(null, number = 2)))
    assert(!data.mayMatch(original))
    val restored = InstantiationUtil.deserializeObject[SparkCatalystPartitionPredicate](
      InstantiationUtil.serializeObject(original),
      getClass.getClassLoader)
    assert(!data.mayMatch(restored))
  }

  if (gteqSpark3_5) {
    test("statistics retain query SQLConf when lazy evaluation happens under another context") {
      val key = "spark.sql.legacy.nullInEmptyListBehavior"
      val data = sample(Seq(row(null)))
      var predicate: SparkCatalystPartitionPredicate = null
      withSparkSQLConf(key -> "false") {
        predicate = SparkCatalystPartitionPredicate(Not(In(dt, Seq.empty)), partitionType)
        // With modern semantics, NULL NOT IN an empty set is true.
        assert(data.partitions.forall(predicate.test))
      }
      withSparkSQLConf(key -> "true") {
        assert(data.mayMatch(predicate))
        assert(SQLConf.get.getConfString(key) == "true")
      }
    }

    test("query SQLConf survives serialization and cold evaluation on worker threads") {
      val key = "spark.sql.legacy.nullInEmptyListBehavior"
      val data = sample(Seq(row(null)))
      val queries = for {
        legacy <- Seq(false, true)
        mode <- Seq("CODEGEN_ONLY", "NO_CODEGEN")
      } yield {
        var predicate: SparkCatalystPartitionPredicate = null
        withSparkSQLConf(key -> legacy.toString, "spark.sql.codegen.factoryMode" -> mode) {
          predicate = SparkCatalystPartitionPredicate(Not(In(dt, Seq.empty)), partitionType)
        }
        val restored = InstantiationUtil.deserializeObject[SparkCatalystPartitionPredicate](
          InstantiationUtil.serializeObject(predicate),
          getClass.getClassLoader)
        (legacy, restored)
      }
      val executor = Executors.newFixedThreadPool(2)
      try {
        val results = queries.map {
          case (legacy, predicate) =>
            executor.submit(new Callable[Boolean] {
              override def call(): Boolean = {
                val caller = new SQLConf
                caller.setConfString(key, (!legacy).toString)
                SQLConf.withExistingConf(caller) {
                  val expected = !legacy
                  (1 to 5).forall {
                    _ =>
                      data.mayMatch(predicate) == expected &&
                      data.partitions.forall(predicate.test) == expected &&
                      SQLConf.get.getConfString(key) == (!legacy).toString
                  }
                }
              }
            })
        }
        results.foreach(result => assert(result.get(30, TimeUnit.SECONDS)))
      } finally {
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
      }
    }
  }

  test("exhaustive bounded partitions never prune an actual match or Spark evaluation error") {
    val values = Seq[String](
      null,
      "",
      "-2",
      "0",
      "00",
      "+0",
      " 0 ",
      "1",
      "1.5",
      "2",
      "10",
      "100",
      "20",
      "invalid",
      "2147483648")
    val samples = for {
      a <- values.indices
      b <- a until values.size
      c <- b until values.size
    } yield sample(Seq(
      row(values(a), if (a % 2 == 0) null else "a", a),
      row(values(b), "a", b),
      row(values(c), "a", c)))
    val comparisons: Seq[Expression => Expression] = Seq(
      e => EqualTo(e, Literal(1)),
      e => EqualNullSafe(e, Literal(1)),
      e => GreaterThan(e, Literal(1)),
      e => GreaterThanOrEqual(e, Literal(1)),
      e => LessThan(e, Literal(1)),
      e => LessThanOrEqual(e, Literal(1)),
      e => In(e, Seq(Literal(0), Literal(1), Literal.create(null, IntegerType))),
      e => IsNull(e),
      e => IsNotNull(e)
    )
    val expressions = comparisons.flatMap {
      comparison =>
        val plain = comparison(n)
        Seq(plain, Not(plain)) ++ Seq(false, true).flatMap {
          ansi =>
            val converted = comparison(cast(ansi))
            Seq(
              converted,
              Not(converted),
              And(converted, EqualTo(region, Literal("a"))),
              Or(converted, IsNull(region)))
        }
    }
    var checked = 0
    var pruned = 0
    expressions.foreach {
      expression =>
        val predicate = SparkCatalystPartitionPredicate(expression, partitionType)
        samples.foreach {
          data =>
            val mustKeep = data.partitions.exists {
              partition =>
                try predicate.test(partition)
                catch { case NonFatal(_) => true }
            }
            val keep = data.mayMatch(predicate)
            assert(
              !mustKeep || keep,
              s"Unsafe pruning: $expression, min=${data.min}, max=${data.max}")
            if (!keep) pruned += 1
            checked += 1
        }
    }
    assert(checked > 50000)
    assert(pruned > 100)
    info(s"No-false-negative checks: $checked; safely pruned cases: $pruned")
  }

}
