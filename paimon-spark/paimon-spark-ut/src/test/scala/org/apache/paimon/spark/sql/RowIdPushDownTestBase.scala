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

import org.apache.spark.sql.Row

class RowIdPushDownTestBase extends PaimonSparkTestBase {

  test("test paimon-spark row id push down") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT, c STRING) TBLPROPERTIES ('row-tracking.enabled'='true')")
      sql("INSERT INTO t VALUES (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')")

      // 1.LeafPredicate
      assertResult(Seq(0L))(getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0").rowIds)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0"),
        Seq(Row(1, 1, "1"))
      )
      assertResult(Seq(0L, 1L))(getPaimonScan("SELECT * FROM t WHERE _ROW_ID IN (0, 1)").rowIds)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (0, 1)"),
        Seq(Row(1, 1, "1"), Row(2, 2, "2"))
      )
      assertResult(Seq(4L, 5L))(getPaimonScan("SELECT * FROM t WHERE _ROW_ID IN (4, 5)").rowIds)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (4, 5)"),
        Seq()
      )

      // 2.CompoundPredicate
      assertResult(Seq(0L))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (0, 1)").rowIds)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (0, 1)"),
        Seq(Row(1, 1, "1"))
      )
      assertResult(Seq(0L, 1L, 2L))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0 OR _ROW_ID IN (1, 2)").rowIds)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 OR _ROW_ID IN (1, 2)"),
        Seq(Row(1, 1, "1"), Row(2, 2, "2"), Row(3, 3, "3"))
      )
      assertResult(Seq())(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (1, 2)").rowIds)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (1, 2)"),
        Seq()
      )
    }
  }
}
