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

import org.apache.spark.sql.Row

class RewriteFileIndexProcedureTest extends PaimonSparkTestBase {

  test("PaimonProcedure: rewrite file index") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (k INT, v STRING, dt STRING, hh INT)
            |PARTITIONED BY (dt, hh)
            |""".stripMargin)

      sql("INSERT INTO t VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      sql("ALTER TABLE t SET TBLPROPERTIES ('file-index.bloom-filter.columns'='k,v')")
      checkAnswer(
        sql("CALL sys.rewrite_file_index(table => 't')"),
        Row(2, 2)
      )
      checkAnswer(
        sql("""CALL sys.rewrite_file_index(table => "t", where => "dt='2025-08-17'")"""),
        Row(1, 1)
      )
    }
  }
}
