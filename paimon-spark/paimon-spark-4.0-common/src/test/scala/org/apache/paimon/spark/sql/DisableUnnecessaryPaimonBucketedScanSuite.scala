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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.execution.adaptive.DisableUnnecessaryPaimonBucketedScan

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class DisableUnnecessaryPaimonBucketedScanSuite
  extends PaimonSparkTestBase
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    // Make non-shuffle query work with stage preparation rule
    super.sparkConf
      .set("spark.sql.adaptive.forceApply", "true")
  }

  private def checkDisableBucketedScan(
      query: String,
      expectedNumScanWithAutoScanEnabled: Int,
      expectedNumScanWithAutoScanDisabled: Int): Unit = {

    def checkNumBucketedScan(df: DataFrame, expectedNumBucketedScan: Int): Unit = {
      val plan = df.queryExecution.executedPlan
      val bucketedScan = collect(plan) {
        case p if DisableUnnecessaryPaimonBucketedScan.extractPaimonBucketedScan(p).isDefined => p
      }
      assert(bucketedScan.length == expectedNumBucketedScan, query)
    }

    withSQLConf("spark.sql.sources.v2.bucketing.enabled" -> "true") {
      withSQLConf("spark.sql.sources.bucketing.autoBucketedScan.enabled" -> "true") {
        val df = sql(query)
        val result = df.collect()
        checkNumBucketedScan(df, expectedNumScanWithAutoScanEnabled)

        withSQLConf("spark.sql.sources.bucketing.autoBucketedScan.enabled" -> "false") {
          val expected = sql(query)
          checkAnswer(expected, result)
          checkNumBucketedScan(expected, expectedNumScanWithAutoScanDisabled)
        }
      }
    }
  }

  private def initializeTable(): Unit = {
    spark.sql(
      "CREATE TABLE t1 (i INT, j INT, k STRING) TBLPROPERTIES ('primary-key' = 'i', 'bucket'='10')")
    spark.sql(
      "INSERT INTO t1 VALUES (1, 1, 'x1'), (2, 2, 'x3'), (3, 3, 'x3'), (4, 4, 'x4'), (5, 5, 'x5')")
    spark.sql(
      "CREATE TABLE t2 (i INT, j INT, k STRING) TBLPROPERTIES ('primary-key' = 'i', 'bucket'='10')")
    spark.sql(
      "INSERT INTO t2 VALUES (1, 1, 'x1'), (2, 2, 'x3'), (3, 3, 'x3'), (4, 4, 'x4'), (5, 5, 'x5')")
    spark.sql(
      "CREATE TABLE t3 (i INT, j INT, k STRING) TBLPROPERTIES ('primary-key' = 'i', 'bucket'='2')")
    spark.sql(
      "INSERT INTO t3 VALUES (1, 1, 'x1'), (2, 2, 'x3'), (3, 3, 'x3'), (4, 4, 'x4'), (5, 5, 'x5')")
  }

  test("Disable unnecessary bucketed table scan - basic test") {
    assume(gteqSpark3_3)

    withTable("t1", "t2", "t3") {
      initializeTable()

      Seq(
        // Read bucketed table
        ("SELECT * FROM t1", 0, 1),
        ("SELECT i FROM t1", 0, 1),
        ("SELECT j FROM t1", 0, 0),
        // Filter on bucketed column
        ("SELECT * FROM t1 WHERE i = 1", 0, 1),
        // Filter on non-bucketed column
        ("SELECT * FROM t1 WHERE j = 1", 0, 1),
        // Join with same buckets
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 2, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.i", 2, 2),
        // Join with different buckets
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i", 0, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t3 ON t1.i = t3.i", 0, 2),
        // Join on non-bucketed column
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.j", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.j", 0, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.i = t2.j", 0, 2),
        ("SELECT /*+ broadcast(t1)*/ * FROM t1 JOIN t2 ON t1.j = t2.j", 0, 2),
        ("SELECT /*+ shuffle_hash(t1)*/ * FROM t1 JOIN t2 ON t1.j = t2.j", 0, 2),
        ("SELECT /*+ merge(t1)*/ * FROM t1 JOIN t2 ON t1.j = t2.j", 0, 2),
        // Aggregate on bucketed column
        ("SELECT SUM(i) FROM t1 GROUP BY i", 1, 1),
        // Aggregate on non-bucketed column
        ("SELECT SUM(i) FROM t1 GROUP BY j", 0, 1),
        ("SELECT j, SUM(i), COUNT(j) FROM t1 GROUP BY j", 0, 1)
      ).foreach {
        case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
          checkDisableBucketedScan(query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  test("Disable unnecessary bucketed table scan - multiple joins test") {
    assume(gteqSpark3_3)

    withTable("t1", "t2", "t3") {
      initializeTable()

      Seq(
        // Multiple joins on bucketed columns
        (
          """
         SELECT /*+ broadcast(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
         ON t1.i = t2.i AND t2.i = t3.i
         """.stripMargin,
          0,
          3),
        (
          """
         SELECT /*+ broadcast(t1) merge(t3)*/ * FROM t1 JOIN t2 JOIN t3
         ON t1.i = t2.i AND t2.i = t3.i
         """.stripMargin,
          0,
          3),
        (
          """
         SELECT /*+ merge(t1) broadcast(t3)*/ * FROM t1 JOIN t2 JOIN t3
         ON t1.i = t2.i AND t2.i = t3.i
         """.stripMargin,
          2,
          3),
        (
          """
         SELECT /*+ merge(t1, t3)*/ * FROM t1 LEFT JOIN t2 LEFT JOIN t3
         ON t1.i = t2.i AND t2.i = t3.i
         """.stripMargin,
          0,
          3),
        // Multiple joins on non-bucketed columns
        (
          """
         SELECT /*+ broadcast(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
         ON t1.i = t2.j AND t2.j = t3.i
         """.stripMargin,
          0,
          3),
        (
          """
         SELECT /*+ merge(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
         ON t1.i = t2.j AND t2.j = t3.i
         """.stripMargin,
          0,
          3),
        (
          """
         SELECT /*+ merge(t1, t3)*/ * FROM t1 JOIN t2 JOIN t3
         ON t1.j = t2.j AND t2.j = t3.j
         """.stripMargin,
          0,
          3)
      ).foreach {
        case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
          checkDisableBucketedScan(query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  test("Disable unnecessary bucketed table scan - other operators test") {
    assume(gteqSpark3_3)

    withTable("t1", "t2", "t3") {
      initializeTable()

      Seq(
        // Operator with interesting partition not in sub-plan
        (
          """
         SELECT t1.i FROM t1
         UNION ALL
         (SELECT t2.i FROM t2 GROUP BY t2.i)
         """.stripMargin,
          1,
          2),
        // Non-allowed operator in sub-plan
        (
          """
         SELECT COUNT(*)
         FROM (SELECT t1.i FROM t1 UNION ALL SELECT t2.i FROM t2)
         GROUP BY i
         """.stripMargin,
          0,
          2),
        // Multiple [[Exchange]] in sub-plan
        (
          """
         SELECT j, SUM(i), COUNT(*) FROM t1 GROUP BY j
         DISTRIBUTE BY j
         """.stripMargin,
          0,
          1),
        (
          """
         SELECT j, COUNT(*)
         FROM (SELECT i, j FROM t1 DISTRIBUTE BY i, j)
         GROUP BY j
         """.stripMargin,
          0,
          1),
        // No bucketed table scan in plan
        (
          """
         SELECT j, COUNT(*)
         FROM (SELECT t1.j FROM t1 JOIN t3 ON t1.j = t3.j)
         GROUP BY j
         """.stripMargin,
          0,
          0)
      ).foreach {
        case (query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled) =>
          checkDisableBucketedScan(query, numScanWithAutoScanEnabled, numScanWithAutoScanDisabled)
      }
    }
  }

  test("Aggregates with no groupby over tables having 1 BUCKET, return multiple rows") {
    assume(gteqSpark3_3)

    withTable("t1") {
      sql("""
            |CREATE TABLE t1 (`id` BIGINT, `event_date` DATE)
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket'='1')
            |""".stripMargin)
      sql("""
            |INSERT INTO TABLE t1 VALUES(1.23, cast("2021-07-07" as date))
            |""".stripMargin)
      sql("""
            |INSERT INTO TABLE t1 VALUES(2.28, cast("2021-08-08" as date))
            |""".stripMargin)
      val df = spark.sql("select sum(id) from t1 where id is not null")
      assert(df.count() == 1)
      checkDisableBucketedScan(
        query = "SELECT SUM(id) FROM t1 WHERE id is not null",
        expectedNumScanWithAutoScanEnabled = 1,
        expectedNumScanWithAutoScanDisabled = 1)
    }
  }
}
