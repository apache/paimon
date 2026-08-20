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

import org.apache.paimon.data.BinaryString
import org.apache.paimon.globalindex.IndexedSplit
import org.apache.paimon.predicate.{ArrayContains, ArrayContainsAll, ArraysOverlap, LeafPredicate, PredicateBuilder}
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/** End-to-end Spark SQL tests for source-backed primary-key sorted indexes. */
class PrimaryKeySortedIndexTest extends PaimonSparkTestBase {

  test("Spark array predicates use multivalue index") {
    assume(gteqSpark3_3)

    withTable("t") {
      spark.sql("""
                  |CREATE TABLE t (id INT, tags ARRAY<STRING>, probe STRING)
                  |TBLPROPERTIES (
                  |  'primary-key' = 'id',
                  |  'bucket' = '1',
                  |  'deletion-vectors.enabled' = 'true',
                  |  'pk-multivalue.index.columns' = 'tags'
                  |)
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO t VALUES
                  |  (1, array('red', 'blue'), 'red'),
                  |  (2, array('green'), 'red'),
                  |  (3, array('red'), 'blue')
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO t VALUES
                  |  (4, array('yellow'), 'yellow'),
                  |  (5, array(CAST(NULL AS STRING)), 'red'),
                  |  (6, CAST(NULL AS ARRAY<STRING>), 'red')
                  |""".stripMargin)
      spark.sql("CALL sys.compact(table => 't')")

      val sourceIndexes = loadTable("t").store.newIndexFileHandler.scanEntries.asScala
        .map(_.indexFile)
        .filter(meta => meta.globalIndexMeta != null && meta.globalIndexMeta.sourceMeta != null)
      assert(sourceIndexes.map(_.indexType).toSet == Set("multivalue"))

      val predicateBuilder = new PredicateBuilder(loadTable("t").rowType())
      val red = BinaryString.fromString("red")
      val blue = BinaryString.fromString("blue")
      val green = BinaryString.fromString("green")

      val containsQuery = "SELECT id FROM t WHERE array_contains(tags, 'red')"
      val containsScan = getPaimonScan(containsQuery)
      assert(containsScan.pushedDataFilters.contains(predicateBuilder.arrayContains(1, red)))
      assert(containsScan.inputSplits.exists(_.isInstanceOf[IndexedSplit]))
      checkAnswer(spark.sql(containsQuery), Seq(Row(1), Row(3)))

      val overlapsQuery =
        "SELECT id FROM t WHERE arrays_overlap(tags, array('blue', 'green'))"
      val overlapsScan = getPaimonScan(overlapsQuery)
      assert(
        overlapsScan.pushedDataFilters.contains(
          predicateBuilder.arraysOverlap(1, Seq(blue, green).asJava)))
      assert(overlapsScan.inputSplits.exists(_.isInstanceOf[IndexedSplit]))
      checkAnswer(spark.sql(overlapsQuery), Seq(Row(1), Row(2)))

      val dynamicQuery = "SELECT id FROM t WHERE array_contains(tags, probe)"
      val dynamicScan = getPaimonScan(dynamicQuery)
      assert(!dynamicScan.pushedDataFilters.exists {
        case leaf: LeafPredicate =>
          leaf.function() == ArrayContains.INSTANCE ||
          leaf.function() == ArraysOverlap.INSTANCE ||
          leaf.function() == ArrayContainsAll.INSTANCE
        case _ => false
      })
      assert(!dynamicScan.inputSplits.exists(_.isInstanceOf[IndexedSplit]))
      checkAnswer(spark.sql(dynamicQuery), Seq(Row(1), Row(4)))
    }
  }

  test("postpone bucket builds and applies sorted indexes during compact") {
    withTable("t") {
      spark.sql("""
                  |CREATE TABLE t (id INT, score INT, category STRING)
                  |TBLPROPERTIES (
                  |  'primary-key' = 'id',
                  |  'bucket' = '-2',
                  |  'postpone.batch-write-fixed-bucket' = 'false',
                  |  'compaction.force-up-level-0' = 'true',
                  |  'compaction.force-rewrite-all-files' = 'true',
                  |  'deletion-vectors.enabled' = 'true',
                  |  'pk-btree.index.columns' = 'score',
                  |  'pk-bitmap.index.columns' = 'category',
                  |  'fields.score.pk-btree.index.options' =
                  |    '{"block-size":"4 kb"}',
                  |  'fields.category.pk-bitmap.index.options' =
                  |    '{"dictionary-block-size":"8 kb"}'
                  |)
                  |""".stripMargin)
      spark.sql("INSERT INTO t VALUES (1, 10, 'red'), (2, 20, 'blue'), (3, 30, 'red')")

      assert(spark.sql("SELECT * FROM t").collect().isEmpty)
      assert(spark.sql("SELECT bucket FROM `t$buckets`").collect().exists(_.getInt(0) == -2))

      // A single L0 run can be upgraded without a rewrite, so force an eligible compact output.
      spark.sql("CALL sys.compact(table => 't')")

      assert(!spark.sql("SELECT bucket FROM `t$buckets`").collect().exists(_.getInt(0) == -2))
      assert(spark.sql("SELECT file_path FROM `t$files` WHERE level = 0").collect().isEmpty)
      val sourceIndexes = loadTable("t").store.newIndexFileHandler.scanEntries.asScala
        .map(_.indexFile)
        .filter(meta => meta.globalIndexMeta != null && meta.globalIndexMeta.sourceMeta != null)
      assert(sourceIndexes.map(_.indexType).toSet == Set("btree", "bitmap"))

      val indexedQuery = "SELECT * FROM t WHERE score >= 20 AND category = 'red'"
      val indexedScan = getPaimonScan(indexedQuery)
      assert(indexedScan.inputSplits.exists(_.isInstanceOf[IndexedSplit]))
      checkAnswer(spark.sql(indexedQuery), Seq(Row(3, 30, "red")))
    }
  }

  test("mixed sorted indexes with deletion vectors") {
    withTable("t") {
      spark.sql("""
                  |CREATE TABLE t (id INT, score INT, category STRING, note STRING)
                  |TBLPROPERTIES (
                  |  'primary-key' = 'id',
                  |  'bucket' = '1',
                  |  'deletion-vectors.enabled' = 'true',
                  |  'pk-btree.index.columns' = 'score',
                  |  'pk-bitmap.index.columns' = 'category',
                  |  'fields.score.pk-btree.index.options' =
                  |    '{"block-size":"4 kb"}',
                  |  'fields.category.pk-bitmap.index.options' =
                  |    '{"dictionary-block-size":"8 kb"}'
                  |)
                  |""".stripMargin)
      spark.sql("INSERT INTO t VALUES (1, 10, 'red', 'keep'), (2, 20, 'blue', 'drop')")
      spark.sql("INSERT INTO t VALUES (3, 30, 'red', 'keep'), (4, 40, 'green', 'keep')")
      spark.sql("CALL sys.compact(table => 't')")

      val sourceIndexes = loadTable("t").store.newIndexFileHandler.scanEntries.asScala
        .map(_.indexFile)
        .filter(meta => meta.globalIndexMeta != null && meta.globalIndexMeta.sourceMeta != null)
      assert(sourceIndexes.map(_.indexType).toSet == Set("btree", "bitmap"))

      spark.sql("UPDATE t SET score = 25, category = 'red', note = 'keep' WHERE id = 2")
      spark.sql("DELETE FROM t WHERE id = 3")
      spark.sql("INSERT INTO t VALUES (5, 35, 'yellow', 'keep')")

      val indexedQuery = "SELECT * FROM t WHERE score >= 20 AND score < 40"
      val indexedScan = getPaimonScan(indexedQuery)
      assert(indexedScan.pushedDataFilters.nonEmpty)
      assert(indexedScan.inputSplits.exists(_.isInstanceOf[IndexedSplit]))
      assert(indexedScan.inputSplits.exists(_.isInstanceOf[DataSplit]))

      checkAnswer(spark.sql("SELECT * FROM t WHERE score = 10"), Seq(Row(1, 10, "red", "keep")))
      checkAnswer(
        spark.sql(indexedQuery),
        Seq(Row(2, 25, "red", "keep"), Row(5, 35, "yellow", "keep")))
      checkAnswer(
        spark.sql("SELECT * FROM t WHERE score >= 10 AND category = 'red'"),
        Seq(Row(1, 10, "red", "keep"), Row(2, 25, "red", "keep")))
      checkAnswer(
        spark.sql("SELECT * FROM t WHERE score = 40 OR category = 'red'"),
        Seq(Row(1, 10, "red", "keep"), Row(2, 25, "red", "keep"), Row(4, 40, "green", "keep")))
      checkAnswer(
        spark.sql("SELECT * FROM t WHERE note = 'keep'"),
        Seq(
          Row(1, 10, "red", "keep"),
          Row(2, 25, "red", "keep"),
          Row(4, 40, "green", "keep"),
          Row(5, 35, "yellow", "keep"))
      )
    }
  }
}
