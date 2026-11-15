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

import org.apache.paimon.data.{BinaryRow, InternalArray, InternalRow}
import org.apache.paimon.metrics.{Gauge, MetricGroup, MetricGroupImpl, MetricRegistry}
import org.apache.paimon.operation.metrics.ScanMetrics
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.Compatibility

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Literal, Not}
import org.apache.spark.sql.catalyst.filter.SparkCatalystPartitionPredicate
import org.apache.spark.sql.types.{IntegerType, StringType}

import java.util.{Collections, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Exercise the Catalyst predicate through the public Paimon scan API. This does not install a
 * Catalyst-filter pushdown rule in Spark's SQL scan builder.
 */
class SparkCatalystPartitionScanTest extends PaimonSparkTestBase {

  private val dt = AttributeReference("dt", StringType)()
  private def cast(ansi: Boolean = false): Expression =
    Compatibility.cast(dt, IntegerType, ansiEnabled = ansi)

  private case class ScanResult(ids: Seq[Int], manifests: Long)

  private def scan(
      tableName: String,
      expression: Expression,
      useStatistics: Boolean = true): ScanResult = {
    val table = loadTable(tableName)
    val predicate =
      SparkCatalystPartitionPredicate(expression, table.rowType().project(table.partitionKeys()))
    // Baseline: the same exact partition evaluation with the former always-true statistics path.
    val filter =
      if (useStatistics) predicate
      else
        new PartitionPredicate {
          override def test(partition: BinaryRow): Boolean = predicate.test(partition)
          override def test(
              rowCount: Long,
              min: InternalRow,
              max: InternalRow,
              nullCounts: InternalArray): Boolean = true
        }
    val group = new MetricGroupImpl(ScanMetrics.GROUP_NAME, Collections.emptyMap[String, String]())
    val registry = new MetricRegistry {
      override def createMetricGroup(name: String, variables: JMap[String, String]): MetricGroup =
        group
    }
    val builder = table.newReadBuilder().withPartitionFilter(filter)
    val plan = builder.newScan().withMetricRegistry(registry).plan()
    val read = builder.newRead()
    val ids = ArrayBuffer.empty[Int]
    plan.splits().asScala.foreach {
      split =>
        val reader = read.createReader(split)
        try {
          reader.forEachRemaining((row: InternalRow) => { ids += row.getInt(0); () })
        } finally {
          reader.close()
        }
    }
    val manifests = group.getMetrics
      .get(ScanMetrics.LAST_SCANNED_MANIFESTS)
      .asInstanceOf[Gauge[Long]]
      .getValue
    ScanResult(ids.sorted.toSeq, manifests)
  }

  for (format <- Seq("parquet", "orc"); ansi <- Seq(false, true)) {
    test(s"CAST comparison and compound statistics prune real manifests: $format, ANSI=$ansi") {
      withSparkSQLConf("spark.sql.ansi.enabled" -> ansi.toString) {
        withTable("stats_dates") {
          sql(s"""CREATE TABLE stats_dates (id INT, dt STRING) USING paimon PARTITIONED BY (dt)
                 |TBLPROPERTIES ('bucket'='-1', 'write-only'='true',
                 |'manifest.merge-min-count'='100', 'file.format'='$format')""".stripMargin)
          Seq("20261112", "20261113", "20261114", "020261113").zipWithIndex.foreach {
            case (value, id) => sql(s"INSERT INTO stats_dates VALUES ($id,'$value')")
          }
          val converted = cast(ansi)
          Seq(
            (EqualTo(converted, Literal(20261113)), Seq(1, 3)),
            (GreaterThanOrEqual(converted, Literal(20261114)), Seq(2)),
            (LessThan(converted, Literal(20261113)), Seq(0)),
            (In(converted, Seq(Literal(20261112), Literal(20261114))), Seq(0, 2)),
            (Not(EqualTo(converted, Literal(20261113))), Seq(0, 2)),
            (
              And(
                GreaterThanOrEqual(converted, Literal(20261113)),
                LessThanOrEqual(converted, Literal(20261113))),
              Seq(1, 3))
          ).foreach {
            case (expression, expected) =>
              val optimized = scan("stats_dates", expression)
              val baseline = scan("stats_dates", expression, useStatistics = false)
              assert(optimized.ids == expected)
              assert(optimized.ids == baseline.ids)
              assert(optimized.manifests == expected.size)
              assert(baseline.manifests == 4L)
          }
          checkAnswer(
            sql("SELECT id FROM stats_dates WHERE CAST(dt AS INT) = 20261113"),
            Seq(Row(1), Row(3)))
        }
      }
    }
  }

  for (format <- Seq("parquet", "orc"); singleDate <- Seq(false, true)) {
    test(s"statistics pruning after actual manifest merging: $format, singleDate=$singleDate") {
      withTable("merged_dates") {
        sql(s"""CREATE TABLE merged_dates (id INT, dt STRING, region STRING)
               |USING paimon PARTITIONED BY (region, dt)
               |TBLPROPERTIES ('bucket'='-1', 'write-only'='true',
               |'manifest.merge-min-count'='2', 'file.format'='$format')""".stripMargin)
        sql("INSERT INTO merged_dates VALUES (1,'10','a')")
        val secondDate = if (singleDate) "10" else "100"
        sql(s"INSERT INTO merged_dates VALUES (2,'$secondDate','z')")
        def manifestNames: Set[String] =
          sql("SELECT file_name FROM `merged_dates$manifests`").collect().map(_.getString(0)).toSet
        val before = manifestNames
        assert(before.size == 2)
        sql("INSERT INTO merged_dates VALUES (3,'2','b')")
        val after = manifestNames
        assert(after.size == 2)
        assert(before.intersect(after).isEmpty)

        val missing = EqualTo(cast(), Literal(50))
        val matching = EqualTo(cast(), Literal(10))
        val expected = if (singleDate) Seq(1, 2) else Seq(1)
        val optimized = scan("merged_dates", missing)
        val baseline = scan("merged_dates", missing, useStatistics = false)
        assert(optimized.ids.isEmpty)
        assert(optimized.ids == baseline.ids)
        assert(optimized.manifests == (if (singleDate) 0L else 1L))
        assert(baseline.manifests == 2L)
        assert(scan("merged_dates", matching).ids == expected)
        assert(scan("merged_dates", matching, useStatistics = false).ids == expected)
        // Direct field predicates retain interval pruning, unlike CAST over string intervals.
        assert(scan("merged_dates", EqualTo(dt, Literal("50"))).manifests == 0L)
      }
    }
  }

  test("statistics pruning preserves primary-key merge reads") {
    withTable("pk_dates") {
      sql("""CREATE TABLE pk_dates (id INT, value STRING, dt STRING) USING paimon
            |PARTITIONED BY (dt) TBLPROPERTIES ('bucket'='1', 'primary-key'='id,dt',
            |'write-only'='true', 'manifest.merge-min-count'='100')""".stripMargin)
      sql("INSERT INTO pk_dates VALUES (1,'unrelated','20261112')")
      sql("INSERT INTO pk_dates VALUES (2,'old','20261113')")
      sql("INSERT INTO pk_dates VALUES (2,'new','20261113')")
      val expression = EqualTo(cast(), Literal(20261113))
      val optimized = scan("pk_dates", expression)
      assert(optimized.ids == Seq(2))
      assert(optimized.ids == scan("pk_dates", expression, useStatistics = false).ids)
      assert(optimized.manifests == 2L)
      checkAnswer(sql("SELECT id, value FROM pk_dates WHERE dt = '20261113'"), Seq(Row(2, "new")))
    }
  }

  test("statistics pruning preserves overwrite ADD and DELETE entries") {
    withTable("overwrite_dates") {
      sql("""CREATE TABLE overwrite_dates (id INT, dt STRING) USING paimon PARTITIONED BY (dt)
            |TBLPROPERTIES ('bucket'='-1', 'manifest.merge-min-count'='100')""".stripMargin)
      sql("INSERT INTO overwrite_dates VALUES (1,'20261112'), (2,'20261113')")
      sql("INSERT OVERWRITE overwrite_dates PARTITION (dt='20261113') SELECT 3")
      Seq(20261112 -> Seq(1), 20261113 -> Seq(3)).foreach {
        case (date, expected) =>
          val expression = EqualTo(cast(), Literal(date))
          val optimized = scan("overwrite_dates", expression)
          assert(optimized.ids == expected)
          assert(optimized.ids == scan("overwrite_dates", expression, useStatistics = false).ids)
      }
    }
  }
}
