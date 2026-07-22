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
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.{PaimonSparkTestBase, PostponeMergeOnReadScan}
import org.apache.paimon.spark.procedure.SparkPostponeCompactProcedure
import org.apache.paimon.table.{CatalogEnvironment, FileStoreTableFactory}

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

class PostponeBucketTableTest extends PaimonSparkTestBase {

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
        Seq(Row(0), Row(1), Row(2), Row(3))
      )

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
        Seq(Row(0), Row(1), Row(2), Row(3))
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
        Seq(Row(0), Row(1), Row(2), Row(3), Row(4), Row(5))
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
        Seq(Row(0), Row(1), Row(2), Row(3))
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
          Seq(Row(-2), Row(0), Row(1), Row(2), Row(3))
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
            |  'postpone.batch-write-fixed-bucket' = 'true'
            |)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'base-1'), (2, 'base-2')")
      val baseSnapshotId = loadTable("t").latestSnapshot().get().id()

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        checkAnswer(sql("SELECT * FROM t ORDER BY k"), Seq(Row(1, "base-1"), Row(2, "base-2")))
        val plan = sql("SELECT * FROM t").queryExecution.executedPlan.toString()
        assert(!plan.contains("PostponeMergeOnRead"), plan)

        val aggregate = sql("SELECT count(*) FROM t")
        checkAnswer(aggregate, Seq(Row(2L)))
        assert(
          aggregate.queryExecution.executedPlan.collect {
            case _: BaseAggregateExec => true
          }.isEmpty,
          aggregate.queryExecution.executedPlan)
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
            |  'postpone.batch-write-fixed-bucket' = 'false'
            |)
            |""".stripMargin)
      sql("INSERT INTO postpone_only_t VALUES (4, 'only-postpone')")

      withSparkSQLConf("spark.paimon.postpone.batch-write-fixed-bucket" -> "false") {
        sql("INSERT INTO t VALUES (1, 'new-1'), (3, 'new-3')")
        sql("INSERT INTO t VALUES (1, 'newest-1')")
        sql("DELETE FROM t WHERE k = 2")
      }

      // Default reads keep the existing delayed-visibility behavior.
      checkAnswer(sql("SELECT * FROM t ORDER BY k"), Seq(Row(1, "base-1"), Row(2, "base-2")))
      checkAnswer(sql("SELECT * FROM postpone_only_t"), Seq.empty)

      withSparkSQLConf("spark.paimon.postpone.merge-on-read" -> "true") {
        // The session option must not affect non-postpone tables.
        checkAnswer(sql("SELECT * FROM normal_t"), Seq(Row(10, "normal")))
        val postponeOnly = sql("SELECT * FROM postpone_only_t")
        assert(postponeOnly.queryExecution.optimizedPlan.stats.rowCount.contains(BigInt(1)))
        val postponeScan = postponeOnly.queryExecution.optimizedPlan.collectFirst {
          case relation: DataSourceV2ScanRelation
              if relation.scan.isInstanceOf[PostponeMergeOnReadScan] =>
            relation.scan.asInstanceOf[PostponeMergeOnReadScan]
        }.get
        assert(postponeScan.filterAttributes().isEmpty)
        checkAnswer(postponeOnly, Seq(Row(4, "only-postpone")))
        checkAnswer(sql("SELECT * FROM t ORDER BY k"), Seq(Row(1, "newest-1"), Row(3, "new-3")))
        checkAnswer(sql("SELECT v FROM t ORDER BY v"), Seq(Row("new-3"), Row("newest-1")))
        checkAnswer(sql("SELECT * FROM t WHERE v = 'base-1'"), Seq.empty)
        checkAnswer(sql("SELECT * FROM t WHERE v = 'new-3'"), Seq(Row(3, "new-3")))
        checkAnswer(sql("SELECT k FROM t WHERE k = 3"), Seq(Row(3)))
        checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(2L)))
        checkAnswer(sql("SELECT count(*), max(k) FROM t"), Seq(Row(2, 3)))
        checkAnswer(sql("SELECT v FROM t ORDER BY k LIMIT 1"), Seq(Row("newest-1")))
        checkAnswer(
          sql(s"SELECT * FROM t VERSION AS OF $baseSnapshotId ORDER BY k"),
          Seq(Row(1, "base-1"), Row(2, "base-2")))

        val plan = sql("SELECT * FROM t").queryExecution.executedPlan.toString()
        assert(plan.contains("PostponeMergeOnRead"), plan)
        assert(plan.contains("PaimonPostponeMergeInput"), plan)
        assert(plan.contains("Exchange hashpartitioning"), plan)
        assert(plan.contains("Sort [__paimon_postpone_partition"), plan)
      }

      withSparkSQLConf(
        "spark.paimon.postpone.merge-on-read" -> "true",
        "spark.paimon.scan.mode" -> "compacted-full") {
        val compactedFull = sql("SELECT * FROM t ORDER BY k")
        checkAnswer(compactedFull, Seq(Row(1, "base-1"), Row(2, "base-2")))
        assert(!compactedFull.queryExecution.executedPlan.toString.contains("PostponeMergeOnRead"))
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
        // Spark only requests v; Core must retain seq until the merge is complete.
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
        assert(mergePlan.contains("PostponeMergeOnRead"), mergePlan)
        assert(!ordinaryPlan.contains("PostponeMergeOnRead"), ordinaryPlan)
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
          Seq(Row(-2), Row(0), Row(1), Row(2), Row(3), Row(4), Row(5))
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
          Seq(Row(0), Row(1), Row(2), Row(3), Row(4), Row(5))
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
