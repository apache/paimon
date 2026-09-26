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

class RollbackToAsLatestProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: rollback to a snapshot as the latest snapshot") {
    spark.sql("CREATE TABLE T (id INT, name STRING)")
    spark.sql("INSERT INTO T VALUES (1, 'a')")
    spark.sql("INSERT INTO T VALUES (2, 'b')")
    spark.sql("INSERT INTO T VALUES (3, 'c')")

    val snapshotManager = loadTable("T").snapshotManager

    // Non-destructive: roll snapshot 1 forward as the new latest (snapshot 4);
    // snapshots 2 and 3 stay, unlike the destructive `rollback`.
    checkAnswer(
      spark.sql("CALL paimon.sys.rollback_to_as_latest(table => 'test.T', snapshot_id => 1)"),
      Row(3L, 1L, 4L) :: Nil)
    assert(snapshotManager.snapshotExists(2))
    assert(snapshotManager.snapshotExists(3))
    checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Nil)

    // Roll forward to snapshot 3 as the latest (snapshot 5).
    checkAnswer(
      spark.sql("CALL paimon.sys.rollback_to_as_latest(table => 'test.T', snapshot_id => 3)"),
      Row(4L, 3L, 5L) :: Nil)
    checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
  }
}
