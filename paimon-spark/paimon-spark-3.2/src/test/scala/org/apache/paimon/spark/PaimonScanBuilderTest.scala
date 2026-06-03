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

package org.apache.paimon.spark

/** Tests for [[PaimonScanBuilder]] in spark-3.2 module. */
class PaimonScanBuilderTest extends PaimonSparkTestBase {

  test("PaimonScanBuilder: read vector table normally on spark-3.2") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (id BIGINT, embs ARRAY<FLOAT>)
                  |TBLPROPERTIES (
                  |  'vector.file.format' = 'lance',
                  |  'vector-field' = 'embs',
                  |  'field.embs.vector-dim' = '3',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true'
                  |)
                  |""".stripMargin)

      var rows = spark.sql("SELECT id, embs FROM T ORDER BY id")
      assert(rows.isEmpty)
      rows =
        spark.sql("select id, embs from vector_search('T', 'embs', array(1.0f, 2.0f, 3.0f), 5)")
      assert(rows.isEmpty)
    }
  }
}
