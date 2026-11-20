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

package org.apache.paimon.spark.commands

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class DataEvolutionIncrementalDiffTest extends PaimonSparkTestBase {

  test("DE diff preserves untouched columns and deletion visibility after SQL updates") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, a INT, b INT) TBLPROPERTIES (
            |'bucket'='-1','row-tracking.enabled'='true',
            |'data-evolution.enabled'='true','deletion-vectors.enabled'='true',
            |'write-only'='true')""".stripMargin)
      sql("INSERT INTO t VALUES (1,10,100),(2,20,200),(3,30,300)")
      val before = loadTable("t").snapshotManager().latestSnapshot().id()
      sql("DELETE FROM t WHERE id=2")
      sql("UPDATE t SET b=b+1 WHERE id IN (1,3)")
      val after = loadTable("t").snapshotManager().latestSnapshot().id()

      def diff = spark.read
        .option("incremental-between", s"$before,$after")
        .option("incremental-between-scan-mode", "diff")
        .table("t")

      checkAnswer(diff, Seq(Row(1, 10, 101), Row(3, 30, 301)))
      checkAnswer(diff.select("a"), Seq(Row(10), Row(30)))
      checkAnswer(diff.where("b=101").select("a"), Seq(Row(10)))
      checkAnswer(diff.orderBy("id").limit(1), Seq(Row(1, 10, 101)))
      assert(diff.limit(1).collect().length == 1)
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 10, 101), Row(3, 30, 301)))
    }
  }
}
