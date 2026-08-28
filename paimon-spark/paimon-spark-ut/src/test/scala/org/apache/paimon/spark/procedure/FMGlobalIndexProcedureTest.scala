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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamTest

import scala.collection.JavaConverters._

/** End-to-end test for building and querying a partitioned exact FM global index. */
class FMGlobalIndexProcedureTest extends PaimonSparkTestBase with StreamTest {

  test("create and query exact fm global index") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, 'abcdef'),
                  |  (1, 'abcXXbcdXXcdeXXdef'),
                  |  (2, NULL),
                  |  (3, 'tail-abcdef-tail'),
                  |  (4, 'zzabzz'),
                  |  (5, 'ab'),
                  |  (6, '你好世界')
                  |""".stripMargin)

      val output = spark
        .sql(
          "CALL sys.create_global_index(" +
            "table => 'test.T', index_column => 'content', index_type => 'fmindex', " +
            "options => 'fm-index.partition-row-count=2')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val entries = loadTable("T")
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .map(_.indexFile())
        .filter(_.indexType() == "fmindex")
      assert(entries.nonEmpty)
      assert(entries.map(_.rowCount()).sum == 7L)

      checkAnswer(
        spark.table("T").where(col("content").contains("abcdef")).select("id"),
        Seq(Row(0), Row(3)))
      checkAnswer(
        spark.table("T").where(col("content").contains("ab")).select("id"),
        Seq(Row(0), Row(1), Row(3), Row(4), Row(5)))
      checkAnswer(spark.table("T").where(col("content").contains("好")).select("id"), Seq(Row(6)))
      checkAnswer(
        spark
          .table("T")
          .where(col("content").contains("abc") && col("content").contains("def"))
          .select("id"),
        Seq(Row(0), Row(1), Row(3)))
    }
  }

  test("create fm global index after deletion-vector deletes") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'deletion-vectors.enabled' = 'true')
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, 'keep-prefix'),
                  |  (1, 'deleted-middle'),
                  |  (2, 'needle-middle'),
                  |  (3, NULL),
                  |  (4, 'keep-tail'),
                  |  (5, 'needle-tail'),
                  |  (6, 'deleted-tail')
                  |""".stripMargin)
      spark.sql("DELETE FROM T WHERE id IN (1, 6)")

      val output = spark
        .sql(
          "CALL sys.create_global_index(" +
            "table => 'test.T', index_column => 'content', index_type => 'fmindex', " +
            "options => 'fm-index.partition-row-count=2')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val entries = loadTable("T")
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .map(_.indexFile())
        .filter(_.indexType() == "fmindex")
      assert(entries.nonEmpty)
      // The index retains the stable physical row-id range, including rows hidden by DVs.
      assert(entries.map(_.rowCount()).sum == 7L)

      checkAnswer(
        spark.table("T").where(col("content").contains("needle")).select("id"),
        Seq(Row(2), Row(5)))
      checkAnswer(
        spark.table("T").where(col("content").contains("deleted")).select("id"),
        Seq.empty)
    }
  }
}
