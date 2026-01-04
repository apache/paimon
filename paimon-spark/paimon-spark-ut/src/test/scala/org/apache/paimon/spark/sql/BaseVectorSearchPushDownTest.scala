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

import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.streaming.StreamTest

import scala.collection.JavaConverters._

/** Tests for vector search push down to Paimon's global vector index. */
class BaseVectorSearchPushDownTest extends PaimonSparkTestBase with StreamTest {

  test("cosine_similarity function works correctly") {
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

      // Test cosine_similarity function - use sys. prefix
      val result = spark
        .sql("""
               |SELECT id, sys.cosine_similarity(v, array(1.0f, 0.0f, 0.0f)) as similarity
               |FROM T
               |ORDER BY similarity DESC
               |""".stripMargin)
        .collect()

      // id=1 should have highest similarity (1.0) since it matches exactly
      assert(result(0).getInt(0) == 1)
      assert(math.abs(result(0).getDouble(1) - 1.0) < 0.001)

      // id=4 should have similarity ~0.707 (1/sqrt(2))
      assert(result(1).getInt(0) == 4)
      assert(math.abs(result(1).getDouble(1) - 0.707) < 0.01)

      // id=5 should have similarity ~0.577 (1/sqrt(3))
      assert(result(2).getInt(0) == 5)
      assert(math.abs(result(2).getDouble(1) - 0.577) < 0.01)

      // id=2 and id=3 should have similarity 0.0
      assert(result(3).getDouble(1) < 0.001)
      assert(result(4).getDouble(1) < 0.001)
    }
  }
}
