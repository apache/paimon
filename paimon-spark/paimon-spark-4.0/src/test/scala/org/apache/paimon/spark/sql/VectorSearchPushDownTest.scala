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

import org.apache.paimon.spark.read.PaimonScan

/** Tests for vector search table-valued function with global vector index. */
class VectorSearchPushDownTest extends BaseVectorSearchPushDownTest {
  test("vector search with global index") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      // Insert 100 rows with predictable vectors
      val values = (0 until 100)
        .map(
          i => s"($i, array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      // Create vector index
      val output = spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'lucene-vector-knn', options => 'vector.dim=3')")
        .collect()
        .head
      assert(output.getBoolean(0))

      // Test vector search with table-valued function syntax
      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
               |""".stripMargin)
        .collect()

      // The result should contain 5 rows
      assert(result.length == 5)

      // Vector (50, 51, 52) should be most similar to the row with id=50
      assert(result.map(_.getInt(0)).contains(50))
    }
  }

  test("vector search pushdown is applied in plan") {
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

      // Create vector index
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'lucene-vector-knn', options => 'vector.dim=3')")
        .collect()

      // Check that vector search is pushed down with table function syntax
      val df = spark.sql("""
                           |SELECT * FROM vector_search('T', 'v', array(50.0f, 51.0f, 52.0f), 5)
                           |""".stripMargin)

      // Get the scan from the executed plan (physical plan)
      val executedPlan = df.queryExecution.executedPlan
      val batchScans = executedPlan.collect {
        case scan: org.apache.spark.sql.execution.datasources.v2.BatchScanExec => scan
      }

      assert(batchScans.nonEmpty, "Should have a BatchScanExec in executed plan")
      val paimonScans = batchScans.filter(_.scan.isInstanceOf[PaimonScan])
      assert(paimonScans.nonEmpty, "Should have a PaimonScan in executed plan")

      val paimonScan = paimonScans.head.scan.asInstanceOf[PaimonScan]
      assert(paimonScan.pushedVectorSearch.isDefined, "Vector search should be pushed down")
      assert(paimonScan.pushedVectorSearch.get.fieldName() == "v", "Field name should be 'v'")
      assert(paimonScan.pushedVectorSearch.get.limit() == 5, "Limit should be 5")
    }
  }

  test("vector search topk returns correct results") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      // Insert rows with distinct vectors
      val values = (1 to 100)
        .map {
          i =>
            val v = math.sqrt(3.0 * i * i)
            val normalized = i.toFloat / v.toFloat
            s"($i, array($normalized, $normalized, $normalized))"
        }
        .mkString(",")
      spark.sql(s"INSERT INTO T VALUES $values")

      // Create vector index
      spark.sql(
        "CALL sys.create_global_index(table => 'test.T', index_column => 'v', index_type => 'lucene-vector-knn', options => 'vector.dim=3')")

      // Query for top 10 similar to (1, 1, 1) normalized
      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(0.577f, 0.577f, 0.577f), 10)
               |""".stripMargin)
        .collect()

      assert(result.length == 10)
    }
  }
}
