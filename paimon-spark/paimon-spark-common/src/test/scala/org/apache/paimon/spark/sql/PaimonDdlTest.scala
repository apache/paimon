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

class PaimonDdlTest extends PaimonSparkTestBase {

  test("Paimon ddl: alter table t rollback syntax") {
    spark.sql("""CREATE TABLE T (id INT, name STRING)
                |USING PAIMON
                |TBLPROPERTIES ('primary-key'='id')""".stripMargin)

    val table = loadTable("T")
    val snapshotManager = table.snapshotManager()

    spark.sql("insert into T values(1, 'a')")
    val timestamp = System.currentTimeMillis()

    spark.sql("insert into T values(2, 'b')")
    spark.sql("insert into T values(3, 'c')")
    spark.sql("insert into T values(4, 'd')")
    assertResult(4)(snapshotManager.snapshotCount())

    // rollback to snapshot
    checkAnswer(spark.sql("alter table T rollback to snapshot `3`"), Row("success") :: Nil)
    assertResult(3)(snapshotManager.latestSnapshotId())

    // create a tag based on snapshot-2
    spark.sql("alter table T create tag `test-tag` as of version 2")
    checkAnswer(spark.sql("show tags T"), Row("test-tag") :: Nil)

    // rollback to tag
    checkAnswer(spark.sql("alter table T rollback to tag `test-tag`"), Row("success") :: Nil)
    assertResult(2)(snapshotManager.latestSnapshotId())

    // rollback to timestamp
    checkAnswer(
      spark.sql(s"alter table T rollback to timestamp `$timestamp`"),
      Row("success") :: Nil)
    assertResult(1)(snapshotManager.latestSnapshotId())
  }
}
