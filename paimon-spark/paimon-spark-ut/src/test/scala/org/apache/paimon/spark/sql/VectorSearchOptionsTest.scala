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

/** Tests for vector search query-time options. */
class VectorSearchOptionsTest extends PaimonSparkTestBase {

  test("vector search forwards query options to reader") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
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
                  |  (0, array(1.0f, 0.0f)),
                  |  (1, array(0.0f, 1.0f))
                  |""".stripMargin)

      spark
        .sql(s"CALL sys.create_global_index(table => 'test.T', index_column => 'v', " +
          s"index_type => '${TestVectorGlobalIndexerFactory.IDENTIFIER}')")
        .collect()

      intercept[Exception] {
        spark
          .sql("""
                 |SELECT id FROM vector_search('T', 'v', array(1.0f, 0.0f), 1)
                 |""".stripMargin)
          .collect()
      }

      val result = spark
        .sql("""
               |SELECT id FROM vector_search(
               |  'T', 'v', array(1.0f, 0.0f), 1, map('ivf.nprobe', '16'))
               |""".stripMargin)
        .collect()

      assert(result.length == 1)

      val scores = spark
        .sql("""
               |SELECT __paimon_search_score FROM vector_search(
               |  'T', 'v', array(1.0f, 0.0f), 1, map('ivf.nprobe', '16'))
               |""".stripMargin)
        .collect()
      assert(scores.length == 1)
      assert(!scores.head.isNullAt(0))
    }
  }
}
