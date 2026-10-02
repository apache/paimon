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

import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

/** The same scan contract is exercised on each Spark 4 minor version. */
abstract class BucketedScanPlanningTestBase
  extends PaimonSparkTestBase
  with AdaptiveSparkPlanHelper {

  private val preserveGrouping = "spark.paimon.scan.preserve-data-grouping"
  private val v2Bucketing = "spark.sql.sources.v2.bucketing.enabled"
  private val autoBucketedScan = "spark.sql.sources.bucketing.autoBucketedScan.enabled"
  private val partialClustering =
    "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled"

  private def createEvents(
      primaryKey: Boolean = false,
      partitions: Int = 8,
      rows: Int = 80,
      multipleKeys: Boolean = false): Unit = {
    val pk = if (primaryKey) ", 'primary-key'='ds,id,seq'" else ""
    val id = if (multipleKeys) "id % 4" else "42"
    sql(s"""CREATE TABLE t_events (id BIGINT, seq BIGINT, payload STRING, ds INT)
           |PARTITIONED BY (ds) TBLPROPERTIES (
           |'bucket'='16', 'bucket-key'='id', 'source.split.target-size'='1 B',
           |'source.split.open-file-cost'='0 B' $pk)""".stripMargin)
    sql(s"""INSERT INTO t_events
           |SELECT CAST($id AS BIGINT), id, 'old', CAST(id % $partitions AS INT)
           |FROM range($rows)""".stripMargin)
  }

  private def createDimension(multipleKeys: Boolean = false): Unit = {
    sql("CREATE TABLE t_dim (id BIGINT) TBLPROPERTIES ('bucket'='16', 'bucket-key'='id')")
    sql(
      if (multipleKeys) "INSERT INTO t_dim SELECT id FROM range(4)"
      else "INSERT INTO t_dim VALUES (42)")
  }

  private def createOrderedTable(): Unit = {
    sql("""CREATE TABLE t_ordered (id BIGINT, payload STRING)
          |TBLPROPERTIES ('primary-key'='id', 'bucket'='4',
          |'source.split.target-size'='128 mb', 'source.split.open-file-cost'='4 mb')
          |""".stripMargin)
    sql("INSERT INTO t_ordered SELECT id, concat('v', cast(id AS STRING)) FROM range(100)")
  }

  private def eventScan(df: DataFrame): BatchScanExec = {
    collect(df.queryExecution.executedPlan) {
      case batch: BatchScanExec
          if batch.scan.isInstanceOf[PaimonScan] && batch.output.exists(_.name == "seq") =>
        batch
    }.head
  }

  private def numShuffles(df: DataFrame): Int = {
    collect(df.queryExecution.executedPlan) { case exchange: ShuffleExchangeLike => exchange }.size
  }

  private def checkLayout(scan: PaimonScan, grouped: Boolean): Unit = {
    assert(scan.inputPartitions.nonEmpty)
    assert(scan.inputPartitions.forall(_.bucketed == grouped))
    assert(scan.outputPartitioning.isInstanceOf[KeyGroupedPartitioning] == grouped)
    if (!grouped) {
      assert(scan.outputPartitioning.isInstanceOf[UnknownPartitioning])
      assert(scan.outputOrdering().isEmpty)
    }
  }

  for ((aqe, force) <- Seq((false, false), (true, false), (true, true)); pk <- Seq(false, true)) {
    test(s"Default scanning retains 45 read tasks with aqe=$aqe force=$force primaryKey=$pk") {
      withTable("t_events") {
        createEvents(primaryKey = pk, partitions = 45, rows = 90)
        withSparkSQLConf(
          preserveGrouping -> "false",
          v2Bucketing -> "true",
          autoBucketedScan -> "true",
          "spark.sql.adaptive.enabled" -> aqe.toString,
          "spark.sql.adaptive.forceApply" -> force.toString
        ) {
          spark.conf.unset(preserveGrouping)
          for (explicit <- Seq(None, Some("true"), Some("false"))) {
            spark.conf.unset(v2Bucketing)
            explicit.foreach(value => spark.conf.set(v2Bucketing, value))
            val df = sql("""SELECT id, seq FROM t_events
                           |WHERE id = 42 AND length(payload) > 0
                           |ORDER BY seq DESC LIMIT 20""".stripMargin)
            checkAnswer(df, (70L until 90L).reverse.map(seq => Row(42L, seq)))
            val batch = eventScan(df)
            val scan = batch.scan.asInstanceOf[PaimonScan]
            checkLayout(scan, grouped = false)
            assert(scan.inputSplits.length == 45)
            assert(batch.inputRDD.getNumPartitions == 45)
          }
        }
      }
    }
  }

  test("Table, session and read options select the scan layout") {
    withTable("t_events") {
      createEvents()
      sql("ALTER TABLE t_events SET TBLPROPERTIES ('scan.preserve-data-grouping'='true')")
      withSparkSQLConf(v2Bucketing -> "true", preserveGrouping -> "false") {
        spark.conf.unset(preserveGrouping)
        val tableDefault = sql("SELECT id, seq FROM t_events")
        tableDefault.collect()
        checkLayout(eventScan(tableDefault).scan.asInstanceOf[PaimonScan], grouped = true)
        assert(eventScan(tableDefault).inputRDD.getNumPartitions == 1)

        spark.conf.set(preserveGrouping, "false")
        val sessionOverride = sql("SELECT id, seq FROM t_events")
        sessionOverride.collect()
        checkLayout(eventScan(sessionOverride).scan.asInstanceOf[PaimonScan], grouped = false)
        assert(eventScan(sessionOverride).inputRDD.getNumPartitions == 8)

        val readOverride = spark.read
          .option("scan.preserve-data-grouping", "true")
          .table("t_events")
          .select("id", "seq")
        readOverride.collect()
        checkLayout(eventScan(readOverride).scan.asInstanceOf[PaimonScan], grouped = true)
        assert(eventScan(readOverride).inputRDD.getNumPartitions == 1)
      }
    }
  }

  test("A scan fixes its layout before physical planning and ignores later configuration changes") {
    withTable("t_ordered") {
      createOrderedTable()
      for (preserve <- Seq(false, true); v2 <- Seq(false, true)) {
        withSparkSQLConf(preserveGrouping -> preserve.toString, v2Bucketing -> v2.toString) {
          val df = sql("SELECT id FROM t_ordered")
          val scan = df.queryExecution.optimizedPlan.collectFirst {
            case relation: DataSourceV2ScanRelation if relation.scan.isInstanceOf[PaimonScan] =>
              relation.scan.asInstanceOf[PaimonScan]
          }.get
          withSparkSQLConf(
            preserveGrouping -> (!preserve).toString,
            v2Bucketing -> (!v2).toString) {
            checkLayout(scan, preserve && v2)
            assert(scan.outputOrdering().nonEmpty == (preserve && v2))
            checkLayout(scan.copy(), preserve && v2)
            assert(scan.copy().outputOrdering().nonEmpty == (preserve && v2))
          }
        }
      }
    }
  }

  test("Scans with different layouts cannot be considered equal for query reuse") {
    withTable("t_ordered") {
      createOrderedTable()
      withSparkSQLConf(preserveGrouping -> "true", v2Bucketing -> "false") {
        def plannedScan(): PaimonScan = {
          sql("SELECT id FROM t_ordered").queryExecution.optimizedPlan.collectFirst {
            case relation: DataSourceV2ScanRelation if relation.scan.isInstanceOf[PaimonScan] =>
              relation.scan.asInstanceOf[PaimonScan]
          }.get
        }
        val ungrouped = plannedScan()
        spark.conf.set(v2Bucketing, "true")
        val grouped = plannedScan()
        checkLayout(ungrouped, grouped = false)
        checkLayout(grouped, grouped = true)
        assert(ungrouped != grouped)
      }
    }
  }

  for (aqe <- Seq(false, true)) {
    test(s"TopN and SORT BY stay correct in both scan modes with aqe=$aqe") {
      withTable("t_ordered") {
        createOrderedTable()
        withSparkSQLConf(
          v2Bucketing -> "true",
          "spark.sql.adaptive.enabled" -> aqe.toString,
          "spark.sql.adaptive.forceApply" -> aqe.toString,
          "spark.sql.files.minPartitionNum" -> "1") {
          for (preserve <- Seq(false, true); auto <- Seq(false, true)) {
            withSparkSQLConf(
              preserveGrouping -> preserve.toString,
              autoBucketedScan -> auto.toString) {
              val topN = sql("SELECT id FROM t_ordered ORDER BY id LIMIT 5")
              assert(topN.collect().map(_.getLong(0)).toSeq == (0L until 5L))
              val sorted = sql("SELECT id FROM t_ordered SORT BY id")
              val parts = sorted.rdd
                .mapPartitions(rows => Iterator(rows.map(_.getLong(0)).toVector))
                .collect()
              assert(parts.flatten.sorted.toSeq == (0L until 100L))
              assert(parts.forall(p => p == p.sorted))
              assert(collect(sorted.queryExecution.executedPlan) {
                case sort: SortExec => sort
              }.isEmpty == preserve)
            }
          }
        }
      }
    }
  }

  for (aqe <- Seq(false, true); preserve <- Seq(false, true)) {
    test(
      s"Generate and Sample retain aggregation and join semantics with aqe=$aqe grouped=$preserve") {
      withTable("t_events", "t_dim") {
        createEvents()
        createDimension()
        withSparkSQLConf(
          preserveGrouping -> preserve.toString,
          v2Bucketing -> "true",
          autoBucketedScan -> "true",
          partialClustering -> "false",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.shuffle.partitions" -> "4",
          "spark.sql.adaptive.enabled" -> aqe.toString,
          "spark.sql.adaptive.forceApply" -> aqe.toString
        ) {
          val generated = sql("""SELECT id, count(*) FROM t_events
                                |LATERAL VIEW explode(array(1, 2)) v AS x GROUP BY id
                                |""".stripMargin)
          checkAnswer(generated, Seq(Row(42L, 160L)))
          assert((numShuffles(generated) == 0) == preserve)
          val sampled =
            sql("SELECT id, count(*) FROM t_events TABLESAMPLE (100 PERCENT) GROUP BY id")
          checkAnswer(sampled, Seq(Row(42L, 80L)))
          assert((numShuffles(sampled) == 0) == preserve)
          val joined = sql("""SELECT /*+ MERGE(e, d) */ e.id, e.seq, e.x
                             |FROM (SELECT id, seq, explode(array(1, 2)) AS x FROM t_events) e
                             |JOIN t_dim d ON e.id = d.id""".stripMargin)
          checkAnswer(joined, for (seq <- 0L until 80L; x <- 1 to 2) yield Row(42L, seq, x))
          assert((numShuffles(joined) == 0) == preserve)
        }
      }
    }
  }

  for (aqe <- Seq(false, true); partial <- Seq(false, true)) {
    test(s"Explicit grouping retains SPJ read units with aqe=$aqe partial=$partial") {
      withTable("t_events", "t_dim") {
        createEvents()
        createDimension()
        withSparkSQLConf(
          preserveGrouping -> "true",
          v2Bucketing -> "true",
          autoBucketedScan -> "true",
          partialClustering -> partial.toString,
          "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
          "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.adaptive.enabled" -> aqe.toString
        ) {
          val df = sql("""SELECT /*+ MERGE(e, d) */ e.id, e.seq
                         |FROM t_events e JOIN t_dim d ON e.id = d.id""".stripMargin)
          checkAnswer(df, (0L until 80L).map(seq => Row(42L, seq)))
          val batch = eventScan(df)
          assert(batch.scan.asInstanceOf[PaimonScan].inputPartitions.size == 8)
          assert(batch.inputRDD.getNumPartitions == (if (partial) 8 else 1))
          assert(numShuffles(df) == 0)
        }
      }
    }
  }

  test("Partially clustered joins retain rows across multiple bucket keys") {
    withTable("t_events", "t_dim") {
      createEvents(multipleKeys = true)
      createDimension(multipleKeys = true)
      withSparkSQLConf(
        preserveGrouping -> "true",
        v2Bucketing -> "true",
        partialClustering -> "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.adaptive.enabled" -> "false"
      ) {
        val df = sql("""SELECT /*+ MERGE(e, d) */ e.id, e.seq
                       |FROM t_events e JOIN t_dim d ON e.id = d.id""".stripMargin)
        checkAnswer(df, (0L until 80L).map(seq => Row(seq % 4, seq)))
        assert(eventScan(df).inputRDD.getNumPartitions > 1)
        assert(numShuffles(df) == 0)
      }
    }
  }

  test("Both scan modes preserve primary-key updates and deletes") {
    withTable("t_events", "t_dim") {
      createEvents(primaryKey = true)
      createDimension()
      sql(
        "INSERT INTO t_events SELECT CAST(42 AS BIGINT), id, 'new', CAST(id % 8 AS INT) FROM range(8)")
      sql("DELETE FROM t_events WHERE seq = 3")
      val expected =
        (0L until 80L).filter(_ != 3L).map(seq => Row(42L, seq, if (seq < 8L) "new" else "old"))
      for (preserve <- Seq(false, true)) {
        withSparkSQLConf(
          preserveGrouping -> preserve.toString,
          v2Bucketing -> "true",
          partialClustering -> "true",
          "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
          "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
          "spark.sql.autoBroadcastJoinThreshold" -> "-1",
          "spark.sql.adaptive.enabled" -> "false"
        ) {
          val df = sql("""SELECT /*+ MERGE(e, d) */ e.id, e.seq, e.payload
                         |FROM t_events e JOIN t_dim d ON e.id = d.id""".stripMargin)
          checkAnswer(df, expected)
          checkLayout(eventScan(df).scan.asInstanceOf[PaimonScan], preserve)
          assert((numShuffles(df) == 0) == preserve)
        }
      }
    }
  }
}
