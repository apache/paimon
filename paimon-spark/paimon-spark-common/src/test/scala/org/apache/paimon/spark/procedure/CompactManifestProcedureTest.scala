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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.streaming.StreamTest
import org.assertj.core.api.Assertions

/** Test compact manifest procedure. See [[CompactManifestProcedure]]. */
class CompactManifestProcedureTest extends PaimonSparkTestBase with StreamTest {

  test("Paimon Procedure: compact manifest") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, value STRING, dt STRING, hh INT)
         |TBLPROPERTIES ('bucket'='-1', 'write-only'='true', 'compaction.min.file-num'='2', 'compaction.max.file-num'='2')
         |PARTITIONED BY (dt, hh)
         |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")
    spark.sql(s"INSERT OVERWRITE T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")
    spark.sql(s"INSERT OVERWRITE T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")
    spark.sql(s"INSERT OVERWRITE T VALUES (5, '5', '2024-01-02', 0), (6, '6', '2024-01-02', 1)")

    Thread.sleep(10000);

    var rows = spark.sql("SELECT sum(num_deleted_files) FROM `T$manifests`").collectAsList()
    Assertions.assertThat(rows.get(0).getLong(0)).isEqualTo(6L)
    spark.sql("CALL sys.compact_manifest(table => 'T')")
    rows = spark.sql("SELECT sum(num_deleted_files) FROM `T$manifests`").collectAsList()
    Assertions.assertThat(rows.get(0).getLong(0)).isEqualTo(0L)
  }
}
