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
               |SELECT * FROM full_text_search('T', 'content', 'paimon', 5)
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
               |SELECT * FROM full_text_search('T', 'content', 'paimon', 1)
               |""".stripMargin)
        .collect()
      assert(result.length == 1)

      // Test with k=10
      result = spark
        .sql("""
               |SELECT * FROM full_text_search('T', 'content', 'paimon', 10)
               |""".stripMargin)
        .collect()
      assert(result.length == 10)
    }
  }

  test("full-text search - multi-term query") {
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

      // Query "Paimon search" - rows 1 and 2 match both terms
      val result = spark
        .sql("""
               |SELECT id FROM full_text_search('T', 'content', 'Paimon search', 2)
               |ORDER BY id
               |""".stripMargin)
        .collect()

      assert(result.length == 2)
      val ids = result.map(_.getInt(0)).toSet
      assert(ids.contains(1))
      assert(ids.contains(2))
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
               |SELECT id, title FROM full_text_search('T', 'content', 'paimon', 10)
               |""".stripMargin)
        .collect()

      assert(searchResult.length == 10)
    }
  }
}
