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
import org.assertj.core.api.Assertions.assertThat

class RepairEarliestSnapshotProcedureTest extends PaimonSparkTestBase {

  test("Paimon procedure: repair earliest snapshot") {
    spark.sql("CREATE TABLE T (k INT) USING PAIMON")

    val snapshotManager = loadTable("T").snapshotManager()
    snapshotManager.fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(1), "")
    snapshotManager.fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(5), "")
    snapshotManager.commitEarliestHint(1)
    snapshotManager.commitLatestHint(5)

    checkAnswer(
      spark.sql("CALL paimon.sys.repair_earliest_snapshot(table => 'test.T', snapshot_id => 5)"),
      Row(1L, 5L) :: Nil)
    assertThat(snapshotManager.earliestSnapshotId).isEqualTo(5)
  }
}
