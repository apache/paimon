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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

class BucketedTableQueryTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {
  private def checkAnswerAndShuffleSorts(
      query: String,
      numShuffles: Int,
      numSorts: Int,
      extraConf: Option[Array[(String, String)]] = None): Unit = {
    var expectedResult: Array[Row] = null
    // avoid config default value change in future, so specify it manually
    withSparkSQLConf(
      "spark.sql.sources.v2.bucketing.enabled" -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      expectedResult = spark.sql(query).collect()
    }
    val conf = extraConf.getOrElse(Array.empty).toSeq
    withSparkSQLConf(
      Array(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1") ++ conf: _*
    ) {
      val df = spark.sql(query)
      checkAnswer(df, expectedResult.toSeq)
      assert(collect(df.queryExecution.executedPlan) {
        case shuffle: ShuffleExchangeLike => shuffle
      }.size == numShuffles)
      if (gteqSpark3_4) {
        assert(collect(df.queryExecution.executedPlan) {
          case sort: SortExec => sort
        }.size == numSorts)
      }
    }
  }

  private def checkInputPartition(query: String, number: Int): Unit = {
    // disable include partition
    withSparkSQLConf(
      "spark.sql.sources.v2.bucketing.enabled" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.requireAllClusterKeysForCoPartition" -> "false"
    ) {
      val df = spark.sql(query)

      assert(collect(df.queryExecution.executedPlan) {
        case source: BatchScanExec =>
          source.inputPartitions.length
      }.sum == number)
    }
  }

  test("Query on a bucketed table - join - positive case") {
    assume(gteqSpark3_3)

    withTable("t1", "t2", "t3", "t4", "t5", "t6") {
      spark.sql(
        "CREATE TABLE t1 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")

      // all matched
      spark.sql(
        "CREATE TABLE t2 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t2 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t2 on t1.id = t2.id", 0, 0)

      // different primary-key name but does not matter
      spark.sql(
        "CREATE TABLE t3 (id2 INT, c STRING) TBLPROPERTIES ('primary-key' = 'id2', 'bucket'='10')")
      spark.sql("INSERT INTO t3 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t3 on t1.id = t3.id2", 0, 0)

      // one primary-key table and one bucketed table
      spark.sql(
        "CREATE TABLE t4 (id INT, c STRING) TBLPROPERTIES ('bucket-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t4 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t4 on t1.id = t4.id", 0, 1)

      // one primary-key table and
      // one primary-key table with two primary keys and one bucket column
      spark.sql(
        "CREATE TABLE t5 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id,c', 'bucket-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t5 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t5 on t1.id = t5.id", 0, 0)

      // one primary-key table and
      // one primary-key table with two primary keys and one primary key is the partition column
      spark.sql(
        "CREATE TABLE t6 (id INT, data STRING, year STRING) PARTITIONED BY (year) TBLPROPERTIES ('primary-key' = 'id,year', 'bucket'='10')")
      spark.sql(
        "INSERT INTO t6 VALUES (1, 'x1', '2020'), (2, 'x3', '2020'), (3, 'x3', '2021'), (4, 'x4', '2021'), (5, 'x5', '2021')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t6 on t1.id = t6.id", 0, 0)
    }
  }

  test("Query on a bucketed table - join - negative case") {
    assume(gteqSpark3_3)

    withTable("t1", "t2", "t3", "t4", "t5", "t6", "t7") {
      spark.sql(
        "CREATE TABLE t1 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")

      // dynamic bucket number
      spark.sql("CREATE TABLE t2 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id')")
      spark.sql("INSERT INTO t2 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t2 on t1.id = t2.id", 2, 2)

      // different bucket number
      spark.sql(
        "CREATE TABLE t3 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='2')")
      spark.sql("INSERT INTO t3 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t3 on t1.id = t3.id", 2, 2)

      // different primary-key data type
      spark.sql(
        "CREATE TABLE t4 (id STRING, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t4 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t4 on t1.id = t4.id", 2, 2)

      // different input partition number
      spark.sql(
        "CREATE TABLE t5 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t5 VALUES (1, 'x1')")
      if (gteqSpark4_0) {
        checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t5 on t1.id = t5.id", 0, 0)
      } else {
        checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t5 on t1.id = t5.id", 2, 2)
      }

      // one more bucket keys
      spark.sql(
        "CREATE TABLE t6 (id1 INT, id2 INT, c STRING) TBLPROPERTIES ('bucket-key' = 'id1,id2', 'bucket'='10')")
      spark.sql(
        "INSERT INTO t6 VALUES (1, 1, 'x1'), (2, 2, 'x3'), (3, 3, 'x3'), (4, 4, 'x4'), (5, 5, 'x5')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t6 on t1.id = t6.id1", 2, 2)

      // primary-key table with three primary keys and one primary key is the partition column
      spark.sql(
        "CREATE TABLE t7 (id1 INT, id2 STRING, year STRING) PARTITIONED BY (year) TBLPROPERTIES ('primary-key' = 'id1,id2,year', 'bucket'='10')")
      spark.sql(
        "INSERT INTO t7 VALUES (1, 'x1', '2020'), (2, 'x3', '2020'), (3, 'x3', '2021'), (4, 'x4', '2021'), (5, 'x5', '2021')")
      checkAnswerAndShuffleSorts("SELECT * FROM t1 JOIN t7 on t1.id = t7.id1", 2, 2)
    }
  }

  test("Query on a bucketed table - other operators") {
    assume(gteqSpark3_3)

    withTable("t1") {
      spark.sql(
        "CREATE TABLE t1 (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='10')")
      spark.sql("INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")

      checkAnswerAndShuffleSorts("SELECT id, count(*) FROM t1 GROUP BY id", 0, 0)
      checkAnswerAndShuffleSorts("SELECT id, max(c) FROM t1 GROUP BY id", 0, 0)
      checkAnswerAndShuffleSorts("SELECT c, count(*) FROM t1 GROUP BY c", 1, 0)
      checkAnswerAndShuffleSorts("SELECT c, max(c) FROM t1 GROUP BY c", 1, 2)
      checkAnswerAndShuffleSorts("select max(c) OVER (PARTITION BY id ORDER BY c) from t1", 0, 1)
      // TODO: it is a Spark issue for `WindowExec` which would required partition-by + and order-by
      //   without do distinct..
      checkAnswerAndShuffleSorts("select max(c) OVER (PARTITION BY id ORDER BY id) from t1", 0, 1)
      checkAnswerAndShuffleSorts("select sum(id) OVER (PARTITION BY c ORDER BY id) from t1", 1, 1)

      withSparkSQLConf("spark.sql.requireAllClusterKeysForDistribution" -> "false") {
        checkAnswerAndShuffleSorts("SELECT id, c, count(*) FROM t1 GROUP BY id, c", 0, 0)
      }
      withSparkSQLConf("spark.sql.requireAllClusterKeysForDistribution" -> "true") {
        checkAnswerAndShuffleSorts("SELECT id, c, count(*) FROM t1 GROUP BY id, c", 1, 0)
      }
    }
  }

  test("Report scan output ordering - rawConvertible") {
    assume(gteqSpark3_3)

    withTable("t") {
      spark.sql(
        "CREATE TABLE t (id INT, c STRING) TBLPROPERTIES ('primary-key' = 'id', 'bucket'='2', 'deletion-vectors.enabled'='true')")

      // one file case
      spark.sql(s"INSERT INTO t VALUES (1, 'x1'), (2, 'x3')")
      checkAnswerAndShuffleSorts("SELECT id, max(c) FROM t GROUP BY id", 0, 0)

      // generate some files
      (1.to(20)).foreach {
        i => spark.sql(s"INSERT INTO t VALUES ($i, 'x1'), ($i, 'x3'), ($i, 'x3')")
      }
      checkAnswerAndShuffleSorts("SELECT id, max(c) FROM t GROUP BY id", 0, 1)
    }
  }

  test("Query on partitioned bucket table: partition include in bucket") {
    withTable("t1", "t2", "t3", "t4") {
      spark.sql(
        "CREATE TABLE t1 (id INT, dt STRING) PARTITIONED BY (dt) TBLPROPERTIES ('bucket'='1', 'bucket-key' = 'id')")
      spark.sql("INSERT INTO t1 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")

      spark.sql(
        "CREATE TABLE t2 (id INT, dt STRING) PARTITIONED BY (dt) TBLPROPERTIES ('bucket'='1', 'bucket-key' = 'id')")
      spark.sql("INSERT INTO t2 VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')")
      val query1 = "SELECT * FROM t1 JOIN t2 on t1.id = t2.id and t1.dt = t2.dt"
      checkAnswerAndShuffleSorts(
        query1,
        0,
        2,
        Some(Array("spark.sql.requireAllClusterKeysForCoPartition" -> "false")))
      checkInputPartition(query1, 8)

      // partial partition
      spark.sql(
        "CREATE TABLE t3 (id INT, dt STRING, app STRING, content STRING ) PARTITIONED BY (dt, app) TBLPROPERTIES ('bucket'='1', 'bucket-key' = 'id')")
      spark.sql("""INSERT INTO t3 VALUES (1, '20250316', 'x1', 'a'),
                  | (2, '20250316', 'x3', 'b'),
                  | (3, '20250317', 'x3', 'c'),
                  | (4, '20250317', 'x4', 'd'),
                  | (5, '20250317', 'x5', 'e')""".stripMargin)

      spark.sql(
        "CREATE TABLE t4 (id INT, dt STRING, app STRING, content STRING) PARTITIONED BY (dt, app) TBLPROPERTIES ('bucket'='1', 'bucket-key' = 'id')")
      spark.sql("""INSERT INTO t4 VALUES (1, '20250316', 'x1', 'a'),
                  | (2, '20250316', 'x3', 'b'),
                  | (3, '20250317', 'x3', 'c'),
                  | (4, '20250317', 'x4', 'd'),
                  | (5, '20250317', 'x5', 'e')""".stripMargin)

      // prune the dt column
      val query2 = "SELECT t3.content FROM t3 JOIN t4 on t3.id = t4.id and t3.app = t4.app"
      checkAnswerAndShuffleSorts(
        query2,
        0,
        2,
        Some(Array("spark.sql.requireAllClusterKeysForCoPartition" -> "false")))
      checkInputPartition(query2, 8)

    }
  }
}
