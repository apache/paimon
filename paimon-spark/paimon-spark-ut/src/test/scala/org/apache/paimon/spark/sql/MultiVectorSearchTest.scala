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

/** Tests for multi-vector search. */
class MultiVectorSearchTest extends PaimonSparkTestBase {

  test("multi vector search ranks results from multiple vector columns") {
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
               |SELECT id, __paimon_vector_search_score
               |FROM multi_vector_search(
               |  'T',
               |  map(
               |    'title_vec', array(1.0f, 0.0f),
               |    'body_vec', array(0.0f, 1.0f)),
               |  2,
               |  map('ranker', 'rrf', 'candidate_limit', '2', 'ivf.nprobe', '16'))
               |""".stripMargin)
        .collect()

      assert(result.length == 2)
      assert(result.map(_.getInt(0)).contains(1))
      assert(result.forall(row => !row.isNullAt(1)))

      val configuredRoutesResult = spark
        .sql("""
               |SELECT id, __paimon_vector_search_score
               |FROM multi_vector_search(
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
               |  2,
               |  map('ranker', 'weighted_score'))
               |""".stripMargin)
        .collect()

      assert(configuredRoutesResult.length == 2)
      assert(configuredRoutesResult.map(_.getInt(0)).contains(1))
      assert(configuredRoutesResult.forall(row => !row.isNullAt(1)))
    }
  }
}
