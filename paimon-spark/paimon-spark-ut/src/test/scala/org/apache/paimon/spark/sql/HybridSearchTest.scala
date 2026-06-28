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

import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory
import org.apache.paimon.spark.PaimonSparkTestBase

/** Tests for hybrid search. */
class HybridSearchTest extends PaimonSparkTestBase {

  test("hybrid search ranks results from multiple vector columns") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, title_vec ARRAY<FLOAT>, body_vec ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'test.vector.dimension' = '2',
                  |  'test.vector.required-option.key' = 'ivf.nprobe',
                  |  'test.vector.required-option.value' = '16')
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, array(1.0f, 0.0f), array(0.0f, 1.0f)),
                  |  (1, array(0.9f, 0.1f), array(0.1f, 0.9f)),
                  |  (2, array(0.0f, 1.0f), array(1.0f, 0.0f))
                  |""".stripMargin)

      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'title_vec', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()
      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'body_vec', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()

      val result = spark
        .sql("""
               |SELECT id, __paimon_search_score
               |FROM hybrid_search(
               |  'T',
               |  array(
               |    named_struct(
               |      'vector_column', 'title_vec',
               |      'query_vector', array(1.0f, 0.0f),
               |      'limit', 2,
               |      'weight', 2.0f,
               |      'options', map('ivf.nprobe', '16')),
               |    named_struct(
               |      'vector_column', 'body_vec',
               |      'query_vector', array(0.0f, 1.0f),
               |      'limit', 2,
               |      'weight', 1.0f,
               |      'options', map('ivf.nprobe', '16'))),
               |  array(),
               |  2,
               |  'weighted_score')
               |""".stripMargin)
        .collect()

      assert(result.length == 2)
      assert(result.map(_.getInt(0)).contains(1))
      assert(result.forall(row => !row.isNullAt(1)))
    }
  }

  test("weighted_score exposes per-route min-max normalized fused scores") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, vec_a ARRAY<FLOAT>, vec_b ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'test.vector.dimension' = '2',
                  |  'test.vector.required-option.key' = 'ivf.nprobe',
                  |  'test.vector.required-option.value' = '16')
                  |""".stripMargin)

      // Both columns hold identical vectors, so each route ranks id0 best and id2 worst.
      // Scores vs query [1, 0] are strictly ordered id0 > id1 > id2 under every supported
      // metric, so min-max maps id0 -> 1.0 and id2 -> 0.0 in BOTH routes regardless of metric.
      // id2's raw score is > 0, so a fused 0.0 can only come from normalization, not a raw sum.
      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, array(1.0f, 0.0f), array(1.0f, 0.0f)),
                  |  (1, array(0.8f, 0.6f), array(0.8f, 0.6f)),
                  |  (2, array(0.6f, 0.8f), array(0.6f, 0.8f))
                  |""".stripMargin)

      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'vec_a', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()
      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'vec_b', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()

      val scores = spark
        .sql("""
               |SELECT id, __paimon_search_score
               |FROM hybrid_search(
               |  'T',
               |  array(
               |    named_struct(
               |      'field', 'vec_a',
               |      'query_vector', array(1.0f, 0.0f),
               |      'limit', 3,
               |      'weight', 2.0f,
               |      'options', map('ivf.nprobe', '16')),
               |    named_struct(
               |      'field', 'vec_b',
               |      'query_vector', array(1.0f, 0.0f),
               |      'limit', 3,
               |      'weight', 1.0f,
               |      'options', map('ivf.nprobe', '16'))),
               |  array(),
               |  3,
               |  'weighted_score')
               |""".stripMargin)
        .collect()
        .map(row => row.getInt(0) -> row.getFloat(1))
        .toMap

      // id0 is the top hit in both routes -> 1.0 each -> 2.0 * 1.0 + 1.0 * 1.0 = sum(weights).
      assert(math.abs(scores(0) - 3.0f) < 1e-5)
      // id2 is the worst hit in both routes -> min-max maps it to 0.0 each -> fused 0.0.
      // Without normalization this would be 2.0 * raw_a + 1.0 * raw_b > 0.
      assert(math.abs(scores(2) - 0.0f) < 1e-5)
      assert(scores(0) > scores(2))
    }
  }

  test("hybrid search ranks vector and full-text routes") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, content STRING, vec ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'global-index.row-count-per-shard' = '10000',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true',
                  |  'test.vector.dimension' = '2')
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO T VALUES
                  |  (0, 'lake format', array(1.0f, 0.0f)),
                  |  (1, 'paimon hybrid search', array(0.9f, 0.1f)),
                  |  (2, 'paimon full text', array(0.0f, 1.0f))
                  |""".stripMargin)

      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'vec', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()
      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'content', " +
          "index_type => 'test-fulltext')")
        .collect()

      val result = spark
        .sql("""
               |SELECT id, __paimon_search_score
               |FROM hybrid_search(
               |  'T',
               |  array(
               |    named_struct(
               |      'field', 'vec',
               |      'query_vector', array(1.0f, 0.0f),
               |      'limit', 2,
               |      'weight', 1.0f,
               |      'options', map())),
               |  array(
               |    named_struct(
               |      'query', '{"match":{"column":"content","terms":"paimon"}}',
               |      'limit', 2,
               |      'weight', 1.0f,
               |      'options', map())),
               |  3,
               |  'rrf')
               |ORDER BY __paimon_search_score DESC
               |""".stripMargin)
        .collect()

      assert(result.length == 3)
      assert(result.head.getInt(0) == 1)
      assert(result.forall(row => !row.isNullAt(1)))
    }
  }

  test("hybrid full-text route uses partition filters before ranking") {
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
                  |  (0, 'paimon search', 1),
                  |  (1, 'paimon', 2)
                  |""".stripMargin)

      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'content', " +
          "index_type => 'test-fulltext')")
        .collect()

      val result = spark
        .sql("""
               |SELECT id
               |FROM hybrid_search(
               |  'T',
               |  array(),
               |  array(
               |    named_struct(
               |      'query', '{"match":{"column":"content","terms":"paimon search"}}',
               |      'limit', 1,
               |      'weight', 1.0f,
               |      'options', map())),
               |  1)
               |WHERE pt = 2
               |""".stripMargin)
        .collect()

      assert(result.map(_.getInt(0)).toSeq == Seq(1))
    }
  }

  test("hybrid full-text route rejects non-partition filters") {
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
                  |  (0, 'paimon search'),
                  |  (1, 'paimon')
                  |""".stripMargin)

      spark
        .sql("CALL sys.create_global_index(table => 'test.T', index_column => 'content', " +
          "index_type => 'test-fulltext')")
        .collect()

      val error = intercept[Exception] {
        spark
          .sql("""
                 |SELECT id
                 |FROM hybrid_search(
                 |  'T',
                 |  array(),
                 |  array(
                 |    named_struct(
                 |      'query', '{"match":{"column":"content","terms":"paimon"}}',
                 |      'limit', 1,
                 |      'weight', 1.0f,
                 |      'options', map())),
                 |  1)
                 |WHERE id = 1
                 |""".stripMargin)
          .collect()
      }

      assert(containsMessage(error, "does not support non-partition filters"))
    }
  }

  private def containsMessage(error: Throwable, expected: String): Boolean = {
    var current = error
    while (current != null) {
      if (current.getMessage != null && current.getMessage.contains(expected)) {
        return true
      }
      current = current.getCause
    }
    false
  }
}
