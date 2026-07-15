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

import scala.collection.JavaConverters._

/** Tests for full-text search read/write operations using test-only brute-force full-text index. */
class FullTextSearchTest extends PaimonSparkTestBase {

  private val indexType = "test-fulltext"

  // ========== Index Creation Tests ==========

  test("create full-text index - basic") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(i => s"($i, 'document number $i about paimon lake format')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == indexType)

      assert(indexEntries.nonEmpty)
      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 100L)
    }
  }

  // ========== Index Read/Search Tests ==========

  test("primary-key full-text search uses physical splits and exposes scores") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING)
                  |TBLPROPERTIES (
                  |  'primary-key' = 'id',
                  |  'bucket' = '1',
                  |  'deletion-vectors.enabled' = 'true',
                  |  'pk-full-text.index.columns' = 'content')
                  |""".stripMargin)

      spark.sql("INSERT INTO T VALUES (0, 'lake format')")
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (1, 'paimon full text search'),
                  |  (2, 'apache paimon storage')
                  |""".stripMargin)
      spark.sql("CALL sys.compact(table => 'T')")

      val compactedFiles = spark.sql("SELECT level FROM `T$files`").collect()
      assert(compactedFiles.exists(_.getInt(0) > 0))
      val payloads = loadTable("T")
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "full-text")
      assert(payloads.nonEmpty)

      val rows = spark
        .sql("""
               |SELECT id, __paimon_search_score
               |FROM full_text_search(
               |  'T',
               |  'content',
               |  '{"match":{"column":"content","terms":"paimon"}}',
               |  10)
               |ORDER BY id
               |""".stripMargin)
        .collect()

      assert(rows.map(_.getInt(0)).toSeq == Seq(1, 2))
      assert(rows.forall(row => !row.isNullAt(1) && row.getFloat(1) > 0.0f))
    }
  }

  test("full-text search - basic search") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(i => s"($i, 'document number $i about paimon lake format')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()

      val result = spark
        .sql("""
               |SELECT * FROM full_text_search('T', 'content', '{"match":{"column":"content","terms":"paimon"}}', 5)
               |""".stripMargin)
        .collect()
      assert(result.length == 5)
    }
  }

  test("full-text search - top-k with different k values") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 200)
        .map(i => s"($i, 'document number $i about paimon lake format')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()

      // Test with k=1
      var result = spark
        .sql("""
               |SELECT * FROM full_text_search('T', 'content', '{"match":{"column":"content","terms":"paimon"}}', 1)
               |""".stripMargin)
        .collect()
      assert(result.length == 1)

      // Test with k=10
      result = spark
        .sql("""
               |SELECT * FROM full_text_search('T', 'content', '{"match":{"column":"content","terms":"paimon"}}', 10)
               |""".stripMargin)
        .collect()
      assert(result.length == 10)
    }
  }

  test("full-text search - multi-term query operators") {
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
                  |  (0, 'Apache Paimon lake format'),
                  |  (1, 'Paimon supports full-text search'),
                  |  (2, 'full-text search in Apache Paimon'),
                  |  (3, 'vector similarity search'),
                  |  (4, 'streaming batch processing')
                  |""".stripMargin)

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()

      val defaultOrResult = spark
        .sql("""
               |SELECT id FROM full_text_search('T', 'content', '{"match":{"column":"content","terms":"Paimon search"}}', 5)
               |ORDER BY id
               |""".stripMargin)
        .collect()

      assert(defaultOrResult.map(_.getInt(0)).toSeq == Seq(0, 1, 2, 3))

      val explicitAndResult = spark
        .sql("""
               |SELECT id FROM full_text_search('T', 'content', '{"match":{"column":"content","terms":"Paimon search","operator":"And"}}', 5)
               |ORDER BY id
               |""".stripMargin)
        .collect()

      assert(explicitAndResult.map(_.getInt(0)).toSeq == Seq(1, 2))
    }
  }

  test("full-text search - structured JSON DSL") {
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
                  |  (0, 'Apache Paimon lake format'),
                  |  (1, 'Paimon supports full-text search'),
                  |  (2, 'full-text search in Apache Paimon'),
                  |  (3, 'vector similarity search')
                  |""".stripMargin)

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()

      val phraseQuery = """{"phrase":{"column":"content","terms":"full-text search"}}"""
      val phraseResult = spark
        .sql(s"""
                |SELECT id FROM full_text_search('T', 'content', '$phraseQuery', 10)
                |ORDER BY id
                |""".stripMargin)
        .collect()

      assert(phraseResult.map(_.getInt(0)).toSeq == Seq(1, 2))

      val booleanQuery =
        """{"boolean":{"queries":[["Must",{"match":{"column":"content","terms":"Paimon"}}],["Must",{"match":{"column":"content","terms":"search"}}],["MustNot",{"match":{"column":"content","terms":"vector"}}]]}}"""
      val booleanResult = spark
        .sql(s"""
                |SELECT id FROM full_text_search('T', 'content', '$booleanQuery', 10)
                |ORDER BY id
                |""".stripMargin)
        .collect()

      assert(booleanResult.map(_.getInt(0)).toSeq == Seq(1, 2))
    }
  }

  // ========== Integration Tests ==========

  test("end-to-end: write, index, search cycle") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, title STRING, content STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 1000)
        .map(i => s"($i, 'title_$i', 'document number $i about paimon lake format')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val indexResult = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()
        .head
      assert(indexResult.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == indexType)
      assert(indexEntries.nonEmpty)

      val searchResult = spark
        .sql("""
               |SELECT id, title FROM full_text_search('T', 'content', '{"match":{"column":"content","terms":"paimon"}}', 10)
               |""".stripMargin)
        .collect()

      assert(searchResult.length == 10)
    }
  }
}
