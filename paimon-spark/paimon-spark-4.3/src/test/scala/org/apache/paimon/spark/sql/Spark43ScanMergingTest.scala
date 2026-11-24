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

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier, TableCapability, TableCatalog}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** On Spark 4.3, scalar subqueries over Paimon tables are merged by Spark's `MergeSubplans`. */
class Spark43ScanMergingTest extends PaimonSparkTestBase {

  /** Scan nodes in the executed plan; a reused subquery is a leaf and is not counted. */
  private def scanNodes(sqlText: String): Seq[SparkPlan] = {
    var nodes: Seq[SparkPlan] = Seq.empty
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      nodes = sql(sqlText).queryExecution.executedPlan.collectWithSubqueries {
        case s: BatchScanExec => s
        case s: FileSourceScanExec => s
      }
    }
    nodes
  }

  private def scanCount(sqlText: String): Int = scanNodes(sqlText).size

  private def capabilities(tableName: String): java.util.Set[TableCapability] =
    spark.sessionState.catalogManager.currentCatalog
      .asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(Array(dbName0), tableName))
      .capabilities()

  test("data tables declare SCAN_MERGING and system tables do not") {
    withTable("T") {
      sql("CREATE TABLE T (a INT)")
      assert(capabilities("T").contains(TableCapability.SCAN_MERGING))
      Seq("T$snapshots", "T$ro", "T$audit_log").foreach {
        systemTable => assert(!capabilities(systemTable).contains(TableCapability.SCAN_MERGING))
      }
    }
  }

  test("tables with query auth do not declare SCAN_MERGING") {
    withTable("T", "A") {
      sql("CREATE TABLE T (a INT)")
      withSparkSQLConf("spark.paimon.query-auth.enabled" -> "true") {
        assert(!capabilities("T").contains(TableCapability.SCAN_MERGING))
      }
      sql("CREATE TABLE A (a INT) TBLPROPERTIES ('query-auth.enabled' = 'true')")
      assert(!capabilities("A").contains(TableCapability.SCAN_MERGING))
    }
  }

  test("merge Paimon subqueries that read different columns") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b DOUBLE, c STRING)")
      sql("INSERT INTO T VALUES (1, 11.1, 'x1'), (2, 22.2, 'x2'), (3, 33.3, 'x3')")
      val query = "SELECT (SELECT SUM(a) FROM T), (SELECT AVG(b) FROM T)"
      checkAnswer(sql(query), Row(6L, 22.2))
      assert(scanCount(query) == 1)
    }
  }

  test("merge Paimon subqueries when only one has a data filter") {
    withTable("T") {
      sql("CREATE TABLE T (x BIGINT, y BIGINT)")
      // The x ranges of the two writes do not overlap, so a scan that pushed `x > 6` would skip
      // the files of the first one.
      sql("INSERT INTO T SELECT id, id * 10 FROM range(5)")
      sql("INSERT INTO T SELECT id, id * 10 FROM range(5, 10)")
      val query = "SELECT (SELECT sum(x) FROM T), (SELECT sum(y) FROM T WHERE x > 6)"
      checkAnswer(sql(query), Row(45L, 240L))
      assert(scanCount(query) == 1)
    }
  }

  test("do not merge Paimon subqueries when only one has a partition filter") {
    withTable("T") {
      sql("CREATE TABLE T (x BIGINT, p INT) PARTITIONED BY (p)")
      sql("INSERT INTO T SELECT id, CAST(id % 2 AS INT) FROM range(10)")
      val query = "SELECT (SELECT sum(x) FROM T), (SELECT sum(x) FROM T WHERE p = 1)"
      checkAnswer(sql(query), Row(45L, 25L))
      assert(scanCount(query) == 2)
    }
  }

  test("merged scans of a primary-key table see updates and deletes") {
    withTable("T") {
      sql(
        "CREATE TABLE T (id INT, x BIGINT, y BIGINT) " +
          "TBLPROPERTIES ('primary-key' = 'id', 'deletion-vectors.enabled' = 'true')")
      sql("INSERT INTO T SELECT CAST(id AS INT), id, id * 10 FROM range(10)")
      sql("INSERT INTO T VALUES (1, 100, 1000)")
      sql("DELETE FROM T WHERE id = 2")
      val query =
        "SELECT (SELECT sum(x) FROM T WHERE id > 0), (SELECT sum(y) FROM T WHERE id > 0)"
      checkAnswer(sql(query), Row(142L, 1420L))
      assert(scanCount(query) == 1)
    }
  }

  test("merged scans keep the bucket filter") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, x BIGINT) TBLPROPERTIES ('bucket' = '4', 'bucket-key' = 'id')")
      sql("INSERT INTO T SELECT CAST(id AS INT), id FROM range(20)")
      val bucketSum = sql("SELECT sum(id) FROM T WHERE __paimon_bucket = 1").head().getLong(0)
      assert(bucketSum > 0 && bucketSum < 190)
      val query = "SELECT (SELECT sum(id) FROM T WHERE __paimon_bucket = 1), " +
        "(SELECT sum(x) FROM T WHERE __paimon_bucket = 1)"
      checkAnswer(sql(query), Row(bucketSum, bucketSum))
      val scans = scanNodes(query)
      assert(scans.size == 1)
      // The filter is also evaluated above the scan, so the rows would not show it was dropped.
      val scan = scans.head.asInstanceOf[BatchScanExec].scan.asInstanceOf[PaimonScan]
      assert(scan.pushedBucketFilters.nonEmpty)
    }
  }

  test("merge non-Paimon subqueries that have different filters") {
    withTempDir {
      dir =>
        spark
          .range(10)
          .selectExpr("id AS x", "id * 10 AS y")
          .write
          .mode("overwrite")
          .parquet(dir.getCanonicalPath)
        withTempView("pq") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("pq")
          val query = "SELECT (SELECT count(*) FROM pq), (SELECT sum(x) FROM pq), " +
            "(SELECT sum(y) FROM pq WHERE x > 3)"
          checkAnswer(sql(query), Row(10L, 45L, 390L))
          assert(scanCount(query) == 1)
        }
    }
  }

  test("do not merge CSV reads whose rows depend on the columns read") {
    withTempDir {
      dir =>
        Files.write(
          new File(dir, "data.csv").toPath,
          "0,0\n1,10\n2,BAD\n3,30\n4,40".getBytes(StandardCharsets.UTF_8))
        withTempView("csv") {
          spark.read
            .schema("a LONG, b LONG")
            .option("mode", "DROPMALFORMED")
            .csv(dir.getCanonicalPath)
            .createOrReplaceTempView("csv")
          // A read of `a` alone does not parse `b`, so it keeps the row whose `b` is malformed.
          checkAnswer(
            sql("SELECT (SELECT sum(a) FROM csv), (SELECT sum(b) FROM csv)"),
            Row(10L, 80L))
        }
    }
  }

  test("scalar subqueries over a non-deterministic CTE under a limit") {
    withTable("T") {
      sql("CREATE TABLE T (c1 STRING, c2 STRING)")
      sql("INSERT INTO T VALUES ('a', 'b'), ('c', 'd')")
      val df = sql("""
                     |WITH cte AS (
                     |  SELECT c1, c2, monotonically_increasing_id() AS id FROM T
                     |),
                     |agg1 AS (
                     |  SELECT c1, count(*) / (SELECT count(c1) FROM cte) AS r
                     |  FROM cte WHERE c1 IS NOT NULL GROUP BY c1
                     |),
                     |agg2 AS (
                     |  SELECT c2, count(*) / (SELECT count(c2) FROM cte) AS r
                     |  FROM cte WHERE c2 IS NOT NULL GROUP BY c2
                     |)
                     |SELECT b.c1, a1.r, a2.r FROM cte b
                     |LEFT JOIN agg1 a1 ON b.c1 = a1.c1
                     |LEFT JOIN agg2 a2 ON b.c2 = a2.c2
                     |""".stripMargin)
      // `Dataset.show()` also puts a limit above the plan.
      checkAnswer(df.limit(10), Seq(Row("a", 0.5, 0.5), Row("c", 0.5, 0.5)))
    }
  }

  test("a scalar subquery nested in another") {
    withTable("T") {
      sql("CREATE TABLE T (a INT, b INT)")
      sql("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)")
      checkAnswer(sql("SELECT (SELECT sum((SELECT sum(a) FROM T)) FROM T)"), Row(18L))
    }
  }

  test("subqueries that read different fields of one struct") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, s STRUCT<a: INT, b: INT>)")
      sql(
        "INSERT INTO T VALUES (1, named_struct('a', 1, 'b', 100)), " +
          "(2, named_struct('a', 2, 'b', 200))")
      checkAnswer(sql("SELECT (SELECT sum(s.a) FROM T), (SELECT max(s.b) FROM T)"), Row(3L, 200))
    }
  }
}
