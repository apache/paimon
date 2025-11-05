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
}
