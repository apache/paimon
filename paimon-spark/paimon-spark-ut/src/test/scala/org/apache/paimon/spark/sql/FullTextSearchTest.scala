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

import org.apache.paimon.index.DataEvolutionIndexSourceMeta
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
      val scanSnapshotId = loadTable("T").snapshotManager().latestSnapshot().id()

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
      assert(
        indexEntries.forall(
          entry =>
            DataEvolutionIndexSourceMeta.fromIndexFile(entry.indexFile()).scanSnapshotId() ==
              scanSnapshotId))
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

  // ========== Row filter tests ==========

  private def createRankedTable(extraProps: String = ""): Unit = {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, category STRING, content STRING)
                 |TBLPROPERTIES (
                 |  'bucket' = '-1',
                 |  'global-index.row-count-per-shard' = '10000',
                 |  'row-tracking.enabled' = 'true',
                 |  'data-evolution.enabled' = 'true'$extraProps)
                 |""".stripMargin)
    // Rows 0-2 match both terms of "paimon lake" (score 1.0); rows 3-5 only "paimon" (0.5).
    spark.sql("""
                |INSERT INTO T VALUES
                |  (0, 'a', 'paimon lake alpha'),
                |  (1, 'a', 'paimon lake beta'),
                |  (2, 'b', 'paimon lake gamma'),
                |  (3, 'b', 'paimon delta'),
                |  (4, 'c', 'paimon epsilon'),
                |  (5, 'c', 'paimon zeta')
                |""".stripMargin)
    spark
      .sql(
        s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
      .collect()
  }

  private val rankedQuery = """{"match":{"column":"content","terms":"paimon lake"}}"""

  test("full-text search - WHERE filter is applied before top-k") {
    withTable("T") {
      createRankedTable()
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'btree')")
        .collect()

      val unfiltered = spark
        .sql(s"SELECT id FROM full_text_search('T', 'content', '$rankedQuery', 2)")
        .collect()
        .map(_.getInt(0))
        .toSet
      assert(unfiltered.subsetOf(Set(0, 1, 2)))

      val filtered = spark
        .sql(s"""
                |SELECT id, __paimon_search_score
                |FROM full_text_search('T', 'content', '$rankedQuery', 2)
                |WHERE id >= 3
                |""".stripMargin)
        .collect()
      assert(filtered.length == 2)
      assert(filtered.map(_.getInt(0)).toSet.subsetOf(Set(3, 4, 5)))
      assert(filtered.forall(_.getFloat(1) == 0.5f))

      // Equivalent to ranking the filtered subset without the index.
      val expected = spark
        .sql("SELECT id FROM T WHERE id >= 3 AND content LIKE '%paimon%' ORDER BY id LIMIT 3")
        .collect()
        .map(_.getInt(0))
        .toSet
      assert(filtered.map(_.getInt(0)).toSet.subsetOf(expected))
    }
  }

  test("full-text search - WHERE filter on string column with bitmap index") {
    withTable("T") {
      createRankedTable()
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'category', index_type => 'bitmap')")
        .collect()

      val result = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 10)
                |WHERE category IN ('b', 'c')
                |ORDER BY id
                |""".stripMargin)
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(result == Seq(2, 3, 4, 5))

      val none = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 10)
                |WHERE category = 'z'
                |""".stripMargin)
        .collect()
      assert(none.isEmpty)
    }
  }

  test("full-text search - fast mode excludes rows whose filter column has no index") {
    withTable("T") {
      createRankedTable()

      val result = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 10)
                |WHERE id >= 3
                |""".stripMargin)
        .collect()
      assert(result.isEmpty)
    }
  }

  test("full-text search - full mode scans rows whose filter column has no index") {
    withTable("T") {
      createRankedTable(",\n  'scalar-index.search-mode' = 'full'")

      val result = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 2)
                |WHERE id >= 3
                |""".stripMargin)
        .collect()
        .map(_.getInt(0))
        .toSet
      assert(result.size == 2)
      assert(result.subsetOf(Set(3, 4, 5)))
    }
  }

  test("full-text search - WHERE filter combined with partition filter") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING, pt INT)
                  |PARTITIONED BY (pt)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, 'paimon lake', 1),
                  |  (1, 'paimon lake', 1),
                  |  (2, 'paimon lake', 2),
                  |  (3, 'paimon lake', 2)
                  |""".stripMargin)
      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'content', index_type => '$indexType')")
        .collect()
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'btree')")
        .collect()

      val result = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 10)
                |WHERE pt = 2 AND id >= 3
                |""".stripMargin)
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(result == Seq(3))
    }
  }

  test("full-text search - WHERE filter with deletion vectors") {
    withTable("T") {
      createRankedTable(",\n  'deletion-vectors.enabled' = 'true'")
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'btree')")
        .collect()
      spark.sql("DELETE FROM T WHERE id = 3")

      val result = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 10)
                |WHERE id >= 3
                |ORDER BY id
                |""".stripMargin)
        .collect()
        .map(_.getInt(0))
        .toSeq
      assert(result == Seq(4, 5))
    }
  }

  test("full-text search - predicate that cannot be pushed down is applied after the search") {
    withTable("T") {
      createRankedTable()
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'btree')")
        .collect()

      // `id % 2 = 1` is not a pushable predicate, so Spark evaluates it after the top-k: the
      // result is a subset of the unfiltered top-k, never rows outside it, and may be short.
      val unfiltered = spark
        .sql(s"SELECT id FROM full_text_search('T', 'content', '$rankedQuery', 3)")
        .collect()
        .map(_.getInt(0))
        .toSet
      val filtered = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 3)
                |WHERE id % 2 = 1
                |""".stripMargin)
        .collect()
        .map(_.getInt(0))
        .toSet
      assert(filtered.subsetOf(unfiltered))
      assert(filtered.forall(_ % 2 == 1))

      // Mixed: the pushable half narrows the candidates before top-k, the rest post-filters.
      val mixed = spark
        .sql(s"""
                |SELECT id
                |FROM full_text_search('T', 'content', '$rankedQuery', 3)
                |WHERE id >= 3 AND id % 2 = 1
                |""".stripMargin)
        .collect()
        .map(_.getInt(0))
        .toSet
      assert(mixed.subsetOf(Set(3, 5)))
    }
  }
}
