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

import org.apache.spark.sql.streaming.StreamTest

/** Tests for vector search table-valued function. */
class BaseVectorSearchPushDownTest extends PaimonSparkTestBase with StreamTest {

  test("vector_search table function basic syntax") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id INT, v ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'bucket' = '-1',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |""".stripMargin)

      // Insert data with known vectors
      spark.sql("""
                  |INSERT INTO T VALUES
                  |(1, array(1.0, 0.0, 0.0)),
                  |(2, array(0.0, 1.0, 0.0)),
                  |(3, array(0.0, 0.0, 1.0)),
                  |(4, array(1.0, 1.0, 0.0)),
                  |(5, array(1.0, 1.0, 1.0))
                  |""".stripMargin)

      // Test vector_search table function syntax
      // Note: Without a global vector index, this will scan all rows
      val result = spark
        .sql("""
               |SELECT * FROM vector_search('T', 'v', array(1.0f, 0.0f, 0.0f), 3)
               |""".stripMargin)
        .collect()

      // Should return results (actual filtering depends on vector index)
      assert(result.nonEmpty)

      // Test invalid limit (negative)
      val ex1 = intercept[Exception] {
        spark
          .sql("""
                 |SELECT * FROM vector_search('T', 'v', array(1.0f, 0.0f, 0.0f), -3)
                 |""".stripMargin)
          .collect()
      }
      assert(ex1.getMessage.contains("Limit must be a positive integer"))

      // Test invalid limit (zero)
      val ex2 = intercept[Exception] {
        spark
          .sql("""
                 |SELECT * FROM vector_search('T', 'v', array(1.0f, 0.0f, 0.0f), 0)
                 |""".stripMargin)
          .collect()
      }
      assert(ex2.getMessage.contains("Limit must be a positive integer"))

      // Test missing parameters
      val ex3 = intercept[Exception] {
        spark
          .sql("""
                 |SELECT * FROM vector_search('T', 'v', array(1.0f, 0.0f, 0.0f))
                 |""".stripMargin)
          .collect()
      }
      assert(ex3.getMessage.contains("vector_search needs three parameters after table_name"))

      // Test non-existent column
      val ex4 = intercept[Exception] {
        spark
          .sql("""
                 |SELECT * FROM vector_search('T', 'non_existent_col', array(1.0f, 0.0f, 0.0f), 3)
                 |""".stripMargin)
          .collect()
      }
      assert(ex4.getMessage.nonEmpty)
    }
  }
}
