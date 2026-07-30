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

import org.apache.paimon.catalog.{Catalog, CatalogLoader, DelegateCatalog, Identifier}
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase, PostponeMergeInputScan}
import org.apache.paimon.spark.PaimonMetrics._
import org.apache.paimon.spark.commands.PaimonSparkWriter
import org.apache.paimon.spark.execution.PostponeMergeOnReadExec
import org.apache.paimon.spark.procedure.SparkPostponeCompactProcedure
import org.apache.paimon.table.{BucketMode, CatalogEnvironment, FileStoreTableFactory}

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}

import scala.collection.JavaConverters._

class PostponeBucketTableTest extends PaimonSparkTestBase {

  test("Postpone bucket table: cap inferred row-count buckets before Int conversion") {
    assert(
      PaimonSparkWriter.computeBucketNumByRowCount(
        Integer.MAX_VALUE.toLong + 1L,
        targetRowNumPerBucket = 1L,
        maxNumBuckets = 2048) == 2048)
  }

  test("Postpone bucket table: write with different bucket number") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |PARTITIONED BY (pt)
            |""".stripMargin)

      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |CAST((1 + FLOOR(RAND() * 4)) AS STRING) AS pt -- pt in [1, 4]
            |FROM range (0, 1000)
            |""".stripMargin)

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
      checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row(499500)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
        Seq(Row(0))
      )

      sql("ALTER TABLE t SET TBLPROPERTIES ('postpone.target-size-per-bucket' = '8 kb')")

      // Write to existing partition, the bucket number should not change
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(6) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |'3' AS pt
            |FROM range (100, 800)
            |""".stripMargin)
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{3}' ORDER BY bucket"),
        Seq(Row(0))
      )

      // Write to new partition, the bucket number should change
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(6) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |'5' AS pt
            |FROM range (100, 800)
            |""".stripMargin)
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{5}' ORDER BY bucket"),
        Seq(Row(0), Row(1), Row(2))
      )
    }
  }

  test("Postpone bucket table: infer bucket number from incoming row count") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt INT
            |) PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true',
            |  'postpone.target-row-num-per-bucket' = '200'
            |)
            |""".stripMargin)

      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(20) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |CASE WHEN id < 100 THEN 0 ELSE 1 END AS pt
            |FROM range (0, 550)
            |""".stripMargin)

      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{0}' ORDER BY bucket"),
        Seq(Row(0))
      )
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{1}' ORDER BY bucket"),
        Seq(Row(0), Row(1), Row(2))
      )

      // Existing partitions keep their bucket number even when the new data volume changes.
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(20) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |0 AS pt
            |FROM range (1000, 2000)
            |""".stripMargin)
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{0}' ORDER BY bucket"),
        Seq(Row(0))
      )

      // Postpone rows are included when a partition gets real buckets for the first time.
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("""
              |INSERT INTO t SELECT
              |id AS k,
              |CAST(id AS STRING) AS v,
              |2 AS pt
              |FROM range (2000, 2150)
              |""".stripMargin)
      }
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(20) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |2 AS pt
            |FROM range (3000, 3100)
            |""".stripMargin)
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{2}' ORDER BY bucket"),
        Seq(Row(-2), Row(0), Row(1))
      )
    }
  }

  test("Postpone bucket table: overwrite excludes existing postpone rows from inference") {
    Seq(
      (
        "static",
        """
          |INSERT OVERWRITE t SELECT
          |id AS k,
          |CAST(id AS STRING) AS v,
          |0 AS pt
          |FROM range (1000, 1100)
          |""".stripMargin),
      (
        "static",
        """
          |INSERT OVERWRITE t PARTITION (pt = 0) SELECT
          |id AS k,
          |CAST(id AS STRING) AS v
          |FROM range (1000, 1100)
          |""".stripMargin),
      (
        "dynamic",
        """
          |INSERT OVERWRITE t SELECT
          |id AS k,
          |CAST(id AS STRING) AS v,
          |0 AS pt
          |FROM range (1000, 1100)
          |""".stripMargin)
    ).foreach {
      case (partitionOverwriteMode, overwriteSql) =>
        withTable("t") {
          sql("""
                |CREATE TABLE t (
                |  k INT,
                |  v STRING,
                |  pt INT
                |) PARTITIONED BY (pt)
                |TBLPROPERTIES (
                |  'primary-key' = 'k, pt',
                |  'bucket' = '-2',
                |  'postpone.batch-write-fixed-bucket' = 'false',
                |  'postpone.target-row-num-per-bucket' = '100'
                |)
                |""".stripMargin)

          sql("""
                |INSERT INTO t SELECT
                |id AS k,
                |CAST(id AS STRING) AS v,
                |0 AS pt
                |FROM range (0, 1000)
                |""".stripMargin)

          withSparkSQLConf(
            "spark.paimon.postpone.batch-write-fixed-bucket" -> "true",
            "spark.sql.sources.partitionOverwriteMode" -> partitionOverwriteMode) {
            sql(overwriteSql)
          }

          checkAnswer(
            sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{0}'"),
            Seq(Row(0))
          )
        }
    }
  }

  test("Postpone bucket table: infer bucket number from serialized data size") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt INT
            |) PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true',
            |  'postpone.target-size-per-bucket' = '32 kb'
            |)
            |""".stripMargin)

      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(20) */
            |id AS k,
            |CASE WHEN id < 100 THEN repeat('x', 100) ELSE repeat('x', 1000) END AS v,
            |CASE WHEN id < 100 THEN 0 ELSE 1 END AS pt
            |FROM range (0, 200)
            |""".stripMargin)

      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{0}' ORDER BY bucket"),
        Seq(Row(0))
      )
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{1}' ORDER BY bucket"),
        Seq(Row(0), Row(1), Row(2), Row(3))
      )

      // Estimate existing postpone data with the average serialized size of incoming rows.
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("""
              |INSERT INTO t SELECT
              |id AS k,
              |repeat('x', 1000) AS v,
              |2 AS pt
              |FROM range (1000, 1100)
              |""".stripMargin)
      }
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(20) */
            |id AS k,
            |repeat('x', 1000) AS v,
            |2 AS pt
            |FROM range (2000, 2100)
            |""".stripMargin)
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{2}' ORDER BY bucket"),
        Seq(Row(-2), Row(0), Row(1), Row(2), Row(3), Row(4), Row(5), Row(6))
      )
    }
  }

  test("Postpone bucket table: write fix bucket then write postpone bucket") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)

      // write fix bucket
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v
            |FROM range (0, 1000)
            |""".stripMargin)

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
      checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row(499500)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
        Seq(Row(0))
      )

      // write postpone bucket
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("""
              |INSERT INTO t SELECT /*+ REPARTITION(6) */
              |id AS k,
              |CAST(id AS STRING) AS v
              |FROM range (0, 1000)
              |""".stripMargin)
        checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
        checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row(499500)))
        checkAnswer(
          sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
          Seq(Row(-2), Row(0))
        )
      }
    }
  }

  test("Postpone bucket table: Spark merge on read") {
    withTable("t", "normal_t", "postpone_only_t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true',
            |  'deletion-vectors.enabled' = 'true',
            |  'deletion-vectors.merge-on-read' = 'true',
            |  'postpone.default-bucket-num' = '1',
            |  'source.split.target-size' = '1 B'
            |)
            |""".stripMargin)

      // Force multiple real splits in bucket 0.
      sql("INSERT INTO t VALUES (1, 'base-1')")
      sql("INSERT INTO t VALUES (2, 'base-2')")
      val baseSnapshotId = loadTable("t").latestSnapshot().get().id()

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        checkAnswer(sql("SELECT * FROM t ORDER BY k"), Seq(Row(1, "base-1"), Row(2, "base-2")))
        val plan = sql("SELECT * FROM t").queryExecution.executedPlan.toString()
        assert(plan.contains("PaimonPostponeMergeScan test.t"), plan)

        val aggregate = sql("SELECT count(*) FROM t")
        checkAnswer(aggregate, Seq(Row(2L)))
        val aggregatePlan = aggregate.queryExecution.executedPlan.toString()
        assert(aggregatePlan.contains("HashAggregate"), aggregatePlan)
      }

      sql("CREATE TABLE normal_t (k INT, v STRING) USING paimon")
      sql("INSERT INTO normal_t VALUES (10, 'normal')")

      sql("""
            |CREATE TABLE postpone_only_t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false',
            |  'postpone.default-bucket-num' = '4'
            |)
            |""".stripMargin)
      sql("INSERT INTO postpone_only_t VALUES (4, 'only-postpone')")

      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        // Conflicting records share one writer order.
        sql("INSERT INTO t VALUES (1, 'new-1'), (1, 'newest-1'), (3, 'new-3')")
        sql("DELETE FROM t WHERE k = 2")
      }

      // MOR disabled: postpone records remain hidden.
      checkAnswer(sql("SELECT * FROM t ORDER BY k"), Seq(Row(1, "base-1"), Row(2, "base-2")))
      checkAnswer(sql("SELECT * FROM postpone_only_t"), Seq.empty)

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        // The option must not affect ordinary tables.
        checkAnswer(sql("SELECT * FROM normal_t"), Seq(Row(10, "normal")))
        val postponeOnly = sql("SELECT * FROM postpone_only_t")
        assert(postponeOnly.queryExecution.optimizedPlan.stats.rowCount.contains(BigInt(1)))
        val postponeScan = postponeOnly.queryExecution.optimizedPlan.collectFirst {
          case relation: DataSourceV2ScanRelation if relation.scan.isInstanceOf[PaimonScan] =>
            relation.scan.asInstanceOf[PaimonScan]
        }.get
        assert(postponeScan.planPostponeMerge(spark.sparkContext.defaultParallelism).isDefined)
        assert(postponeScan.filterAttributes().isEmpty)
        assert(
          intercept[UnsupportedOperationException](postponeScan.toBatch).getMessage
            .contains("PostponeMergeOnReadExec"))
        checkAnswer(postponeOnly, Seq(Row(4, "only-postpone")))
        checkAnswer(sql("SELECT * FROM t ORDER BY k"), Seq(Row(1, "newest-1"), Row(3, "new-3")))
        checkAnswer(sql("SELECT v FROM t ORDER BY v"), Seq(Row("new-3"), Row("newest-1")))
        checkAnswer(sql("SELECT * FROM t WHERE v = 'base-1'"), Seq.empty)
        checkAnswer(sql("SELECT * FROM t WHERE v = 'new-3'"), Seq(Row(3, "new-3")))
        checkAnswer(sql("SELECT k FROM t WHERE k = 3"), Seq(Row(3)))
        checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(2L)))
        checkAnswer(sql("SELECT count(*), max(k) FROM t"), Seq(Row(2, 3)))
        checkAnswer(sql("SELECT v FROM t ORDER BY k LIMIT 1"), Seq(Row("newest-1")))
        if (gteqSpark3_3) {
          checkAnswer(
            sql(s"SELECT * FROM t VERSION AS OF $baseSnapshotId ORDER BY k"),
            Seq(Row(1, "base-1"), Row(2, "base-2")))
        }

        val query = sql("SELECT * FROM t")
        val mergeScan = query.queryExecution.optimizedPlan.collectFirst {
          case relation: DataSourceV2ScanRelation if relation.scan.isInstanceOf[PaimonScan] =>
            relation.scan.asInstanceOf[PaimonScan]
        }.get
        val realSplits = mergeScan
          .planPostponeMerge(spark.sparkContext.defaultParallelism)
          .get
          .corePlan
          .realSplits()
        assert(realSplits.size() > 1, realSplits)

        val plan = query.queryExecution.executedPlan.toString()
        assert(plan.contains("PaimonPostponeMergeScan test.t"), plan)
        assert(plan.contains("Paimon Postpone Scan"), plan)
        assert(plan.contains("Exchange hashpartitioning"), plan)
        assert(!plan.contains("PostponeArrivalOrder"), plan)
        assert(plan.contains("Sort [__paimon_postpone_partition"), plan)
        assert(plan.contains("__paimon_postpone_writer_local_order"), plan)
      }

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> (1L << 30).toString,
        "spark.sql.files.maxPartitionBytes" -> (1L << 30).toString,
        "spark.sql.shuffle.partitions" -> "8"
      ) {
        val query = sql("SELECT * FROM postpone_only_t")
        checkAnswer(query, Seq(Row(4, "only-postpone")))
        assert(query.rdd.getNumPartitions == 1, query.queryExecution.executedPlan)
      }

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> (1L << 30).toString,
        "spark.sql.files.maxPartitionBytes" -> "1",
        "spark.sql.shuffle.partitions" -> "8"
      ) {
        val query = sql("SELECT * FROM postpone_only_t")
        checkAnswer(query, Seq(Row(4, "only-postpone")))
        assert(query.rdd.getNumPartitions == 6, query.queryExecution.executedPlan)
      }

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.paimon.scan.mode" -> "compacted-full") {
        val compactedFull = sql("SELECT * FROM t ORDER BY k")
        checkAnswer(compactedFull, Seq(Row(1, "base-1"), Row(2, "base-2")))
        assert(
          !compactedFull.queryExecution.executedPlan.toString
            .contains("PaimonPostponeMergeScan test.t"))
      }
    }
  }

  test("Postpone bucket table: Spark merge on read metrics") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true',
            |  'postpone.default-bucket-num' = '1',
            |  'source.split.target-size' = '1 B'
            |)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'base-1')")
      sql("INSERT INTO t VALUES (2, 'base-2')")
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("INSERT INTO t VALUES (1, 'new-1'), (1, 'newest-1'), (3, 'new-3')")
      }

      val table = loadTable("t")
      val postponeSplits = table
        .newSnapshotReader()
        .withBucket(BucketMode.POSTPONE_BUCKET)
        .read()
        .dataSplits()
      val expectedPostponeFiles = postponeSplits.asScala
        .map(_.dataFiles().size().toLong)
        .sum
      val realSplits = table
        .newSnapshotReader()
        .onlyReadRealBuckets()
        .read()
        .dataSplits()
      val expectedRealFiles = realSplits.asScala
        .map(_.dataFiles().size().toLong)
        .sum
      val expectedRealCarriers = realSplits.asScala
        .groupBy(split => (split.partition(), split.bucket()))
        .size
        .toLong

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.sql.adaptive.enabled" -> "false") {
        val query = sql("SELECT * FROM t WHERE k >= 1")
        assert(
          query.collect().sortBy(_.getInt(0)).toSeq ==
            Seq(Row(1, "newest-1"), Row(2, "base-2"), Row(3, "new-3")))

        val executedPlan = query.queryExecution.executedPlan
        val inputScan = executedPlan.collectFirst {
          case scan: BatchScanExec if scan.scan.isInstanceOf[PostponeMergeInputScan] => scan
        }.get
        val merge = executedPlan.collectFirst { case exec: PostponeMergeOnReadExec => exec }.get

        assert(
          inputScan.scan.description() ==
            "Paimon Postpone Scan: read postpone files and route records by target bucket")
        assert(merge.nodeName == "PaimonPostponeMergeScan test.t")
        assert(merge.simpleString(100).contains("PaimonScan"))
        assert(merge.simpleString(100).contains("DataFilters"))

        val inputMetrics = inputScan.metrics
        assert(inputMetrics(RESULTED_POSTPONE_FILES).value == expectedPostponeFiles)
        assert(inputMetrics(NUM_POSTPONE_RECORDS).value == 3L)
        assert(!inputMetrics.contains(NUM_SPLITS))
        assert(inputMetrics(PARTITION_SIZE).value > 0L)
        assert(inputMetrics(READ_BATCH_TIME).value >= 0L)
        assert(inputMetrics("numOutputRows").value == expectedRealCarriers + 3L)
        assert(!inputMetrics.contains(RESULTED_TABLE_FILES))
        assert(!inputMetrics.contains(PLANNING_DURATION))

        val mergeMetrics = merge.metrics
        assert(mergeMetrics(RESULTED_TABLE_FILES).value == expectedRealFiles)
        assert(mergeMetrics(SCANNED_SNAPSHOT_ID).value == 3L)
        assert(mergeMetrics(SCANNED_MANIFESTS).value > 0L)
        assert(mergeMetrics(SKIPPED_TABLE_FILES).value >= 0L)
        assert(mergeMetrics(PLANNING_DURATION).value >= 0L)
        assert(mergeMetrics(NUM_SPLITS).value == expectedRealCarriers)
        assert(mergeMetrics(PARTITION_SIZE).value > 0L)
        assert(mergeMetrics(READ_BATCH_TIME).value >= 0L)
        assert(mergeMetrics("numOutputRows").value == 3L)
        assert(!mergeMetrics.contains(RESULTED_POSTPONE_FILES))
        assert(!mergeMetrics.contains(NUM_POSTPONE_RECORDS))
      }
    }
  }

  test("Postpone bucket table: Spark combines postpone and deletion-vector merge on read") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false',
            |  'deletion-vectors.enabled' = 'true',
            |  'deletion-vectors.merge-on-read' = 'true'
            |)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'base-1'), (2, 'base-2'), (3, 'base-3'), (4, 'base-4')")
      sql("CALL sys.compact(table => 't')")
      sql("INSERT INTO t VALUES (1, 'real-1'), (5, 'real-5')")
      sql("CALL sys.compact(table => 't')")
      assert(deletionVectorCardinality("t") > 0)

      sql("INSERT INTO t VALUES (2, 'postpone-2'), (3, 'postpone-3'), (6, 'postpone-6')")
      sql("DELETE FROM t WHERE k = 4")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY k"),
        Seq(
          Row(1, "real-1"),
          Row(2, "base-2"),
          Row(3, "base-3"),
          Row(4, "base-4"),
          Row(5, "real-5")))

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.paimon.deletion-vectors.merge-on-read" -> "true") {
        val query = sql("SELECT * FROM t ORDER BY k")
        checkAnswer(
          query,
          Seq(
            Row(1, "real-1"),
            Row(2, "postpone-2"),
            Row(3, "postpone-3"),
            Row(5, "real-5"),
            Row(6, "postpone-6")))
        checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(5L)))
        assert(
          query.queryExecution.executedPlan.toString.contains("PaimonPostponeMergeScan test.t"))
      }
    }
  }

  test("Postpone bucket table: Spark merge on read respects full scan protection") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt INT
            |) TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false'
            |)
            |PARTITIONED BY (pt)
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2)")

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.paimon.read.allow.fullScan" -> "false") {
        assert(
          intercept[Exception](sql("SELECT * FROM t").collect()).getMessage
            .contains("Full scan is not supported."))
        assert(
          intercept[Exception](sql("SELECT * FROM t WHERE v IS NOT NULL").collect()).getMessage
            .contains("Full scan is not supported."))
        checkAnswer(sql("SELECT * FROM t WHERE pt = 1"), Seq(Row(1, "a", 1)))
      }
    }
  }

  test("Postpone bucket table: Spark merge on read custom bucket key") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k1 INT,
            |  k2 INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k1, k2',
            |  'bucket-key' = 'k1',
            |  'bucket' = '-2',
            |  'postpone.default-bucket-num' = '3',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')")
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("INSERT INTO t VALUES (1, 1, 'new-a'), (1, 2, 'new-b'), (3, 1, 'd')")
      }

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY k1, k2"),
          Seq(Row(1, 1, "new-a"), Row(1, 2, "new-b"), Row(2, 1, "c"), Row(3, 1, "d")))
        checkAnswer(sql("SELECT v FROM t WHERE k1 = 1 AND k2 = 2"), Seq(Row("new-b")))
      }
    }
  }

  test("Postpone bucket table: Spark merge on read sequence field") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  seq INT
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'sequence.field' = 'seq',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'base-1', 10), (2, 'base-2', 10)")
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("INSERT INTO t VALUES (1, 'older', 5), (2, 'newer', 20)")
      }

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY k"),
          Seq(Row(1, "base-1", 10), Row(2, "newer", 20)))
        // Core must retain seq after Spark projects v.
        checkAnswer(sql("SELECT v FROM t ORDER BY v"), Seq(Row("base-1"), Row("newer")))
      }
    }
  }

  test("Postpone bucket table: Spark merge on read keeps partitions isolated") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |PARTITIONED BY (pt)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'base-a', 'a'), (1, 'base-b', 'b')")
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("INSERT INTO t VALUES (1, 'new-a', 'a')")
      }

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY pt"),
          Seq(Row(1, "new-a", "a"), Row(1, "base-b", "b")))
        checkAnswer(sql("SELECT * FROM t WHERE pt = 'a'"), Seq(Row(1, "new-a", "a")))
        checkAnswer(sql("SELECT * FROM t WHERE pt = 'b'"), Seq(Row(1, "base-b", "b")))

        val mergePlan = sql("SELECT * FROM t WHERE pt = 'a'").queryExecution.executedPlan.toString
        val ordinaryPlan =
          sql("SELECT * FROM t WHERE pt = 'b'").queryExecution.executedPlan.toString
        assert(mergePlan.contains("PaimonPostponeMergeScan test.t"), mergePlan)
        assert(ordinaryPlan.contains("PaimonPostponeMergeScan test.t"), ordinaryPlan)
      }
    }
  }

  test("Postpone bucket table: Spark merge on read pins the selected snapshot") {
    withTable("real_t", "empty_t") {
      sql("""
            |CREATE TABLE real_t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)
      sql("INSERT INTO real_t VALUES (1, 'base')")

      sql("""
            |CREATE TABLE empty_t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        val pinnedReal = sql("SELECT * FROM real_t ORDER BY k")
        val pinnedRealScan = pinnedReal.queryExecution.optimizedPlan.collectFirst {
          case relation: DataSourceV2ScanRelation if relation.scan.isInstanceOf[PaimonScan] =>
            relation.scan.asInstanceOf[PaimonScan]
        }.get
        assert(
          pinnedRealScan
            .planPostponeMerge(spark.sparkContext.defaultParallelism)
            .isDefined)

        withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
          sql("INSERT INTO real_t VALUES (1, 'new'), (2, 'added')")
        }
        checkAnswer(pinnedReal, Seq(Row(1, "base")))
        checkAnswer(sql("SELECT * FROM real_t ORDER BY k"), Seq(Row(1, "new"), Row(2, "added")))

        val pinnedEmpty = sql("SELECT * FROM empty_t")
        val pinnedEmptyScan = pinnedEmpty.queryExecution.optimizedPlan.collectFirst {
          case relation: DataSourceV2ScanRelation if relation.scan.isInstanceOf[PaimonScan] =>
            relation.scan.asInstanceOf[PaimonScan]
        }.get
        assert(
          pinnedEmptyScan
            .planPostponeMerge(spark.sparkContext.defaultParallelism)
            .isEmpty)

        sql("INSERT INTO empty_t VALUES (1, 'committed-later')")
        checkAnswer(pinnedEmpty, Seq.empty)
        checkAnswer(sql("SELECT * FROM empty_t"), Seq(Row(1, "committed-later")))
      }
    }
  }

  test("Postpone bucket table: Spark merge on read merge engines") {
    withTable("partial_t", "aggregation_t") {
      sql("""
            |CREATE TABLE partial_t (
            |  k INT,
            |  v1 STRING,
            |  v2 STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'merge-engine' = 'partial-update',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)
      sql("INSERT INTO partial_t VALUES (1, 'a', CAST(NULL AS STRING))")

      sql("""
            |CREATE TABLE aggregation_t (
            |  k INT,
            |  total BIGINT
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'merge-engine' = 'aggregation',
            |  'fields.total.aggregate-function' = 'sum',
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)
      sql("INSERT INTO aggregation_t VALUES (1, 10L)")

      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("INSERT INTO partial_t VALUES (1, CAST(NULL AS STRING), 'b')")
        sql("INSERT INTO aggregation_t VALUES (1, 3L)")
      }

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        checkAnswer(sql("SELECT * FROM partial_t"), Seq(Row(1, "a", "b")))
        checkAnswer(sql("SELECT * FROM aggregation_t"), Seq(Row(1, 13L)))
        checkAnswer(sql("SELECT v1 FROM partial_t"), Seq(Row("a")))
        checkAnswer(sql("SELECT total FROM aggregation_t"), Seq(Row(13L)))
        checkAnswer(sql("SELECT * FROM partial_t WHERE v2 = 'b'"), Seq(Row(1, "a", "b")))
        checkAnswer(sql("SELECT * FROM aggregation_t WHERE total = 13"), Seq(Row(1, 13L)))
      }
    }
  }

  test("Postpone bucket table: write postpone bucket then write fix bucket") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false'
            |)
            |""".stripMargin)

      // write postpone bucket
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v
            |FROM range (0, 1000)
            |""".stripMargin)

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(0)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
        Seq(Row(-2))
      )

      // write fix bucket
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "true") {
        sql("""
              |INSERT INTO t SELECT /*+ REPARTITION(6) */
              |id AS k,
              |CAST(id AS STRING) AS v
              |FROM range (0, 1000)
              |""".stripMargin)
        checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
        checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row(499500)))
        checkAnswer(
          sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
          Seq(Row(-2), Row(0))
        )
      }

      // overwrite fix bucket
      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "true") {
        sql("""
              |INSERT OVERWRITE t SELECT /*+ REPARTITION(8) */
              |id AS k,
              |CAST(id AS STRING) AS v
              |FROM range (0, 500)
              |""".stripMargin)
        checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(500)))
        checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row(124750)))
        checkAnswer(
          sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
          Seq(Row(0))
        )
      }
    }
  }

  test("Postpone bucket table: write postpone bucket then compact") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false'
            |)
            |""".stripMargin)

      // write postpone bucket
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v
            |FROM range (0, 1000)
            |""".stripMargin)

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(0)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
        Seq(Row(-2))
      )

      sql("SET spark.default.parallelism = 2")
      // compact
      sql("CALL sys.compact(table => 't')")

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
      checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row((0 until 1000).sum)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
        Seq(Row(0), Row(1))
      )
    }
  }

  test("Postpone compaction resolves blob descriptor source outside Spark tasks") {
    withTable("source", "t") {
      sql("CREATE TABLE source (k INT, v STRING) TBLPROPERTIES ('primary-key' = 'k')")
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false',
            |  'blob-descriptor.source-table' = 'test.source'
            |)
            |""".stripMargin)
      sql("INSERT INTO t SELECT id, CAST(id AS STRING) FROM range(0, 20)")

      val table = loadTable("t")
      val environment = table.catalogEnvironment
      val driverOnlyEnvironment = new CatalogEnvironment(
        environment.identifier,
        environment.uuid,
        new PostponeBucketTableTest.SourceTableDriverOnlyCatalogLoader(environment.catalogLoader),
        environment.lockFactory,
        environment.lockContext,
        environment.catalogContext,
        environment.supportsVersionManagement,
        false
      )
      val driverOnlyTable = FileStoreTableFactory.create(
        table.fileIO,
        table.location,
        table.schema,
        driverOnlyEnvironment)

      SparkPostponeCompactProcedure(driverOnlyTable, spark, null, createRelationV2("t")).execute()

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(20)))
    }
  }

  test("Postpone partition bucket table: write postpone bucket then compact") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt INT
            |) PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.default-bucket-num' = '3',
            |  'changelog-producer' = 'lookup',
            |  'snapshot.num-retained.min' = '5',
            |  'snapshot.num-retained.max' = '5',
            |  'postpone.batch-write-fixed-bucket' = 'false'
            |)
            |""".stripMargin)

      // write postpone bucket
      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |id % 2 AS pt
            |FROM range (0, 1000)
            |""".stripMargin)

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(0)))
      checkAnswer(sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"), Seq(Row(-2)))

      // compact
      sql("SET spark.default.parallelism = 2")
      sql("CALL sys.compact(table => 't')")

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1000)))
      checkAnswer(sql("SELECT sum(k) FROM t"), Seq(Row((0 until 1000).sum)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"),
        Seq(Row(0), Row(1), Row(2))
      )

      val table = loadTable("t")
      val files = table.fileIO.listStatus(new Path(table.location, "pt=0/bucket-0"))
      assert(files.count(_.getPath.getName.startsWith("changelog-")) > 0)

      for (i <- 2000 until 2020) {
        spark.sql(s"INSERT INTO t (k, v, pt) VALUES ($i, '$i', ${i % 2})")
      }

      // Verify that snapshots are not automatically expired before compaction
      checkAnswer(sql("SELECT count(1) FROM `t$snapshots`"), Seq(Row(22)))

      sql("CALL sys.compact(table => 't')")
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` where partition = '{0}' ORDER BY bucket"),
        Seq(Row(0), Row(1), Row(2))
      )
      checkAnswer(sql("SELECT count(1) FROM `t$snapshots`"), Seq(Row(5)))
    }
  }

  test("Postpone partition bucket table: compact with target row num per bucket") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING,
            |  pt INT
            |) PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'k, pt',
            |  'bucket' = '-2',
            |  'postpone.target-row-num-per-bucket' = '200',
            |  'postpone.batch-write-fixed-bucket' = 'false'
            |)
            |""".stripMargin)

      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v,
            |CASE WHEN id < 100 THEN 0 ELSE 1 END AS pt
            |FROM range (0, 550)
            |""".stripMargin)

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(0)))
      checkAnswer(sql("SELECT distinct(bucket) FROM `t$buckets` ORDER BY bucket"), Seq(Row(-2)))

      sql("CALL sys.compact(table => 't')")

      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(550)))
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{0}' ORDER BY bucket"),
        Seq(Row(0))
      )
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets` WHERE partition = '{1}' ORDER BY bucket"),
        Seq(Row(0), Row(1), Row(2))
      )
    }
  }

  test("Postpone bucket table: skip clustering in writing phase") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  k INT,
            |  v STRING
            |) TBLPROPERTIES (
            |  'primary-key' = 'k',
            |  'bucket' = '-2',
            |  'postpone.batch-write-fixed-bucket' = 'false',
            |  'clustering.columns' = 'k',
            |  'clustering.strategy' = 'order'
            |)
            |""".stripMargin)

      val before = System.currentTimeMillis()

      sql("""
            |INSERT INTO t SELECT /*+ REPARTITION(4) */
            |id AS k,
            |CAST(id AS STRING) AS v
            |FROM range (0, 100)
            |""".stripMargin)

      // Verify no Sort operator in the plan (clustering is skipped)
      val executions = spark.sharedState.statusStore.executionsList()
      val hasSort = executions.exists {
        e =>
          e.submissionTime > before &&
          e.physicalPlanDescription != null &&
          e.physicalPlanDescription.toLowerCase.contains("sort")
      }
      assert(!hasSort, "Postpone table should skip clustering (no sort in plan)")

      // Verify data was written to postpone directory (bucket=-2)
      checkAnswer(
        sql("SELECT distinct(bucket) FROM `t$buckets`"),
        Seq(Row(-2))
      )
    }
  }

  private def deletionVectorCardinality(tableName: String): Long = {
    val table = loadTable(tableName)
    table
      .store()
      .newIndexFileHandler()
      .scan(table.latestSnapshot().get(), DELETION_VECTORS_INDEX)
      .asScala
      .flatMap(entry => Option(entry.indexFile().dvRanges()).toSeq)
      .flatMap(_.values().asScala)
      .flatMap(meta => Option(meta.cardinality()).map(_.longValue()))
      .sum
  }
}

object PostponeBucketTableTest {

  private class SourceTableDriverOnlyCatalogLoader(delegate: CatalogLoader) extends CatalogLoader {

    override def load(): Catalog = {
      new DelegateCatalog(delegate.load()) {
        override def catalogLoader(): CatalogLoader = delegate

        override def getTable(identifier: Identifier) = {
          if (TaskContext.get() != null && identifier.getTableName == "source") {
            throw new IllegalStateException("Source table must not be loaded in a Spark task")
          }
          super.getTable(identifier)
        }
      }
    }
  }
}
