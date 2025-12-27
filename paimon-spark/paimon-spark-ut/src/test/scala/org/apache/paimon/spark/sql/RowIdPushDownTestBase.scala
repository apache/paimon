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

      // first manifest
      sql("INSERT INTO t VALUES (0, 0, '0'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')")

      // second manifest
      sql("INSERT INTO t VALUES (4, 4, '4'), (5, 5, '5')")

      // delete second manifest
      // after push down, should never read it
      val table = loadTable("t")
      val manifests =
        table.store().manifestListFactory().create().readAllManifests(table.latestSnapshot().get)
      val secondManifest = manifests.asScala.find(m => m.minRowId() == 4L).get
      table.store().manifestFileFactory().create().delete(secondManifest.fileName())

      // 1.LeafPredicate
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0"),
        Seq(Row(0, 0, "0"))
      )
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (0, 1, 3)"),
        Seq(Row(0, 0, "0"), Row(1, 1, "1"), Row(3, 3, "3"))
      )
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (1, 6)"),
        Seq(Row(1, 1, "1"))
      )
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID IN (6, 7)"),
        Seq()
      )

      // 2.CompoundPredicate
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (0, 1)"),
        Seq(Row(0, 0, "0"))
      )
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 OR _ROW_ID IN (1, 2)"),
        Seq(Row(0, 0, "0"), Row(1, 1, "1"), Row(2, 2, "2"))
      )
      checkAnswer(
        sql("SELECT * FROM t WHERE _ROW_ID = 0 AND _ROW_ID IN (1, 2)"),
        Seq()
      )
    }
  }
}
