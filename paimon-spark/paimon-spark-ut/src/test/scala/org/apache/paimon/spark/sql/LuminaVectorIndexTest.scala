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

/** Tests for Lumina vector index read/write operations. */
class LuminaVectorIndexTest extends PaimonSparkTestBase {

  private val indexType = "lumina"
  private val legacyIndexType = "lumina-vector-ann"
  private val defaultOptions = "lumina.index.dimension=3"

  // ========== Index Creation Tests ==========

  test("create lumina vector index - basic") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
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

  test("create lumina vector index after adding the index column") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)
      spark.sql("INSERT INTO T VALUES (0), (1), (2), (3), (4)")

      spark.sql("ALTER TABLE T ADD COLUMNS (v ARRAY<FLOAT>)")
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (5, array(5.0f, 6.0f, 7.0f)),
                  |  (6, array(6.0f, 7.0f, 8.0f)),
                  |  (7, array(7.0f, 8.0f, 9.0f)),
                  |  (8, array(8.0f, 9.0f, 10.0f)),
                  |  (9, array(9.0f, 10.0f, 11.0f))
                  |""".stripMargin)

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val indexEntries =
        loadTable("T").store().newIndexFileHandler().scan(indexType).asScala
      assert(indexEntries.size == 1)
      val indexFile = indexEntries.head.indexFile()
      assert(indexFile.rowCount() == 10L)
      assert(indexFile.globalIndexMeta().rowRangeStart() == 0L)
      assert(indexFile.globalIndexMeta().rowRangeEnd() == 9L)

      val searchResult = spark
        .sql("""
               |SELECT id FROM vector_search(
               |  'T', 'v', array(5.0f, 6.0f, 7.0f), 5,
               |  map('refine_factor', '5'))
               |""".stripMargin)
        .collect()
      assert(searchResult.map(_.getInt(0)).toSet == (5 to 9).toSet)
    }
  }

  test("create lumina vector index - legacy index type") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 10)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$legacyIndexType', options => '$defaultOptions')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == legacyIndexType)

      assert(indexEntries.nonEmpty)
      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 10L)

      // End-to-end read: vector_search must resolve the legacy identifier through
      // LegacyLuminaVectorGlobalIndexerFactory and return results.
      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
               |""".stripMargin)
        .collect()
      assert(result.length == 5)
    }
  }

  test("table_indexes system table - global index metadata") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()

      // Query table_indexes system table
      val indexRows = spark
        .sql("""
               |SELECT index_type, row_count, row_range_start, row_range_end,
               |       index_field_id, index_field_name
               |FROM `T$table_indexes`
               |WHERE index_type = 'lumina'
               |""".stripMargin)
        .collect()

      assert(indexRows.nonEmpty)
      val row = indexRows.head
      assert(row.getAs[String]("index_type") == "lumina")
      assert(row.getAs[Long]("row_count") == 100L)
      assert(row.getAs[Long]("row_range_start") == 0L)
      assert(row.getAs[Long]("row_range_end") == 99L)
      assert(row.getAs[String]("index_field_name") == "v")

      // Verify max row id matches snapshot next_row_id - 1
      val nextRowId = spark
        .sql("SELECT next_row_id FROM `T$snapshots` ORDER BY snapshot_id DESC LIMIT 1")
        .collect()
        .head
        .getAs[Long]("next_row_id")
      assert(row.getAs[Long]("row_range_end") == nextRowId - 1)
    }
  }

  test("create lumina vector index - with partitioned table") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>, pt STRING)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |  PARTITIONED BY (pt)
                  |""".stripMargin)

      var values = (0 until 500)
        .map(
          i =>
            s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)), 'p0')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      values = (0 until 300)
        .map(
          i =>
            s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)), 'p1')")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
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
      assert(totalRowCount == 800L)
    }
  }

  // ========== Index Write Tests ==========

  test("write vectors - large dataset") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val df = spark
        .range(0, 10000)
        .selectExpr(
          "cast(id as int) as id",
          "array(cast(id as float), cast(id + 1 as float), cast(id + 2 as float)) as v")
      df.write.insertInto("T")

      val output = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
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

      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 10000L)
    }
  }

  // ========== Index Read/Search Tests ==========

  test("read vectors - basic search") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()

      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
               |""".stripMargin)
        .collect()
      assert(result.length == 5)
    }
  }

  test("read vectors - top-k search with different k values") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 200)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")
        .collect()

      // Test with k=1
      var result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(100.0f, 101.0f, 102.0f), 1)
               |""".stripMargin)
        .collect()
      assert(result.length == 1)

      // Test with k=10
      result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(100.0f, 101.0f, 102.0f), 10)
               |""".stripMargin)
        .collect()
      assert(result.length == 10)
    }
  }

  test("read vectors - normalized vectors search") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (1 to 100)
        .map {
          i =>
            val v = math.sqrt(3.0 * i * i)
            val normalized = i.toFloat / v.toFloat
            s"($i, array($normalized, $normalized, $normalized))"
        }
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark.sql(
        s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => '$indexType', options => '$defaultOptions')")

      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(0.577f, 0.577f, 0.577f), 10)
               |""".stripMargin)
        .collect()

      assert(result.length == 10)
    }
  }

  // ========== Integration Tests ==========

  test("end-to-end: write, index, read cycle") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING, embedding ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 1000)
        .map(
          i =>
            s"($i, 'item_$i', array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val indexResult = spark
        .sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => 'embedding', index_type => '$indexType', options => '$defaultOptions')")
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
        .sql(
          """
            |SELECT id, name FROM vector_search('T', 'embedding', array(500.0f, 501.0f, 502.0f), 10)
            |""".stripMargin)
        .collect()

      assert(searchResult.length == 10)
    }
  }

  test("incremental build atomically refreshes vectors updated inside an indexed range") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'lumina.distance.metric' = 'l2',
                  |  'global-index.column-update-action' = 'IGNORE')
                  |""".stripMargin)

      val values = (0 until 200)
        .map {
          case i if i < 100 => s"($i, cast(NULL as ARRAY<FLOAT>))"
          case i =>
            s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))"
        }
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val table = loadTable("T")
      val firstDataSnapshotId = table.snapshotManager().latestSnapshot().id()
      spark.sql(
        s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', " +
          s"index_type => '$indexType', options => '$defaultOptions')")

      val oldEntries = table.store().newIndexFileHandler().scan(indexType).asScala
      assert(oldEntries.nonEmpty)
      assert(
        oldEntries.forall(
          entry =>
            DataEvolutionIndexSourceMeta.fromIndexFile(entry.indexFile()).scanSnapshotId() ==
              firstDataSnapshotId))
      val oldFileNames = oldEntries.map(_.indexFile().fileName()).toSet

      spark.sql("""
                  |MERGE INTO T
                  |USING (SELECT 0 AS id,
                  |       array(cast(0 as float), cast(1 as float), cast(2 as float)) AS v) S
                  |ON T.id = S.id
                  |WHEN MATCHED THEN UPDATE SET v = S.v
                  |""".stripMargin)

      val updatedSnapshot = table.snapshotManager().latestSnapshot()
      val stillVisible =
        table.store().newIndexFileHandler().scan(updatedSnapshot, indexType).asScala
      assert(stillVisible.map(_.indexFile().fileName()).toSet == oldFileNames)
      // Retrieve every indexed candidate before exact reranking so ANN recall is deterministic.
      val beforeRefresh = spark
        .sql("""
               |SELECT id FROM vector_search(
               |  'T', 'v', array(0.0f, 1.0f, 2.0f), 1,
               |  map('refine_factor', '200'))
               |""".stripMargin)
        .collect()
      assert(beforeRefresh.head.getInt(0) != 0)

      spark.sql(
        s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', " +
          s"index_type => '$indexType', options => '$defaultOptions')")

      val indexCommitSnapshot = table.snapshotManager().latestSnapshot()
      assert(indexCommitSnapshot.id() == updatedSnapshot.id() + 1)
      val refreshedEntries =
        table.store().newIndexFileHandler().scan(indexCommitSnapshot, indexType).asScala
      assert(refreshedEntries.nonEmpty)
      assert(refreshedEntries.map(_.indexFile().fileName()).toSet.intersect(oldFileNames).isEmpty)
      assert(
        refreshedEntries.forall(
          entry =>
            DataEvolutionIndexSourceMeta.fromIndexFile(entry.indexFile()).scanSnapshotId() ==
              updatedSnapshot.id()))

      val afterRefresh = spark
        .sql("""
               |SELECT id FROM vector_search(
               |  'T', 'v', array(0.0f, 1.0f, 2.0f), 1,
               |  map('refine_factor', '200'))
               |""".stripMargin)
        .collect()
      assert(afterRefresh.head.getInt(0) == 0)
    }
  }
}
