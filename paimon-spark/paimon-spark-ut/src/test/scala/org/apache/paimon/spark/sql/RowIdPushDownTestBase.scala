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
import org.apache.paimon.table.source.ReadBuilderImpl
import org.apache.paimon.utils.Range

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class RowIdPushDownTestBase extends PaimonSparkTestBase {

  test("test paimon-spark row id push down") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT, c STRING) TBLPROPERTIES " +
        "('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'row-id-push-down.enabled'='true')")
      sql("INSERT INTO t VALUES (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')")

      // 1.LeafPredicate
      assertResult(Seq(new Range(0L, 0L)))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0").readBuilder
          .asInstanceOf[ReadBuilderImpl]
          .rowRanges
          .asScala)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0"),
        Seq(Row(1, 1, "1"))
      )
      assertResult(Seq(new Range(0L, 1L), new Range(3L, 3L)))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID IN (0, 1, 3)").readBuilder
          .asInstanceOf[ReadBuilderImpl]
          .rowRanges
          .asScala)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (0, 1, 3)"),
        Seq(Row(1, 1, "1"), Row(2, 2, "2"), Row(4, 4, "4"))
      )
      assertResult(Seq(new Range(4L, 5L)))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID IN (4, 5)").readBuilder
          .asInstanceOf[ReadBuilderImpl]
          .rowRanges
          .asScala)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (4, 5)"),
        Seq()
      )

      // 2.CompoundPredicate
      assertResult(Seq(new Range(0, 0)))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (0, 1)").readBuilder
          .asInstanceOf[ReadBuilderImpl]
          .rowRanges
          .asScala)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (0, 1)"),
        Seq(Row(1, 1, "1"))
      )
      assertResult(Seq(new Range(0, 2)))(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0 OR _ROW_ID IN (1, 2)").readBuilder
          .asInstanceOf[ReadBuilderImpl]
          .rowRanges
          .asScala)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 OR _ROW_ID IN (1, 2)"),
        Seq(Row(1, 1, "1"), Row(2, 2, "2"), Row(3, 3, "3"))
      )
      assertResult(Seq())(
        getPaimonScan("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (1, 2)").readBuilder
          .asInstanceOf[ReadBuilderImpl]
          .rowRanges
          .asScala)
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (1, 2)"),
        Seq()
      )
    }
  }
}
