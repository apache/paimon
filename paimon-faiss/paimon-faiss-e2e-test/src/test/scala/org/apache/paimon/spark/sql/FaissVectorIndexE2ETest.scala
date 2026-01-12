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

/** End-to-end tests for FAISS vector index read/write operations on Spark 3.5. */
class FaissVectorIndexE2ETest extends PaimonSparkTestBase {

  // ========== Index Creation Tests ==========

  test("create faiss vector index - basic") {
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

      // Create FAISS vector index
      val output = spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()
        .head
      assert(output.getBoolean(0))

      // Verify index was created
      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "faiss-vector-ann")

      assert(indexEntries.nonEmpty)
      val totalRowCount = indexEntries.map(_.indexFile().rowCount()).sum
      assert(totalRowCount == 100L)
    }
  }

  test("create faiss vector index - with different index types") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 50)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      // Test with HNSW index type
      val output = spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3,vector.index-type=HNSW')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "faiss-vector-ann")

      assert(indexEntries.nonEmpty)
    }
  }

  test("create faiss vector index - with partitioned table") {
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
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "faiss-vector-ann")

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

      // Insert 10000 rows
      val values = (0 until 10000)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      val output = spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()
        .head
      assert(output.getBoolean(0))

      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "faiss-vector-ann")

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
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()

      // Test vector search
      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
               |""".stripMargin)
        .collect()
      assert(result.map(_.getInt(0)).contains(50))
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
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()

      // Test with k=1
      var result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(100.0f, 101.0f, 102.0f), 1)
               |""".stripMargin)
        .collect()
      assert(result.length == 1)
      assert(result.head.getInt(0) == 100)

      // Test with k=10
      result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(100.0f, 101.0f, 102.0f), 10)
               |""".stripMargin)
        .collect()
      assert(result.map(_.getInt(0)).contains(100))
    }
  }

  test("read vectors - multiple concurrent searches") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      val values = (0 until 500)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()

      // Perform multiple searches
      val result1 = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(10.0f, 11.0f, 12.0f), 3)
               |""".stripMargin)
        .collect()
      assert(result1.length == 3)
      assert(result1.map(_.getInt(0)).contains(10))

      val result2 = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(250.0f, 251.0f, 252.0f), 5)
               |""".stripMargin)
        .collect()
      assert(result2.map(_.getInt(0)).contains(250))

      val result3 = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(450.0f, 451.0f, 452.0f), 7)
               |""".stripMargin)
        .collect()
      assert(result3.map(_.getInt(0)).contains(450))
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

      // Insert rows with normalized vectors
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
        "CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")

      // Query for top 10 similar to (1, 1, 1) normalized
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
      // Step 1: Create table
      spark.sql("""
                  |CREATE TABLE T (id INT, name STRING, embedding ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      // Step 2: Write data
      val values = (0 until 1000)
        .map(
          i =>
            s"($i, 'item_$i', array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      // Step 3: Create index
      val indexResult = spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'embedding', index_type => 'faiss-vector-ann', options => 'vector.dim=3')")
        .collect()
        .head
      assert(indexResult.getBoolean(0))

      // Step 4: Verify index creation
      val table = loadTable("T")
      val indexEntries = table
        .store()
        .newIndexFileHandler()
        .scanEntries()
        .asScala
        .filter(_.indexFile().indexType() == "faiss-vector-ann")
      assert(indexEntries.nonEmpty)

      // Step 5: Read using vector search
      val searchResult = spark
        .sql(
          """
            |SELECT id, name FROM vector_search('T', 'embedding', array(500.0f, 501.0f, 502.0f), 10)
            |""".stripMargin)
        .collect()

      assert(searchResult.exists(row => row.getInt(0) == 500 && row.getString(1) == "item_500"))
    }
  }
}
