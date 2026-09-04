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

import org.apache.paimon.errors.ErrorMessages
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.catalyst.QueryPlanningTracker

import scala.util.Try

class DataEvolutionUpdateSnapshotTest extends PaimonSparkTestBase {

  test("V1 update detects a concurrent update after its snapshot is pinned") {
    withSparkSQLConf(
      "spark.paimon.write.use-v2-write" -> "false",
      "spark.paimon.write.data-evolution.update-conflict-retry.max-attempts" -> "1"
    ) {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, status STRING) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
        sql("INSERT INTO t VALUES (1, 'pending')")

        val parsed = spark.sessionState.sqlParser.parsePlan(
          "UPDATE t SET status = 'done' WHERE status = 'pending'")
        val updateCommand = spark.sessionState.analyzer
          .executeAndCheck(parsed, new QueryPlanningTracker)
          .asInstanceOf[UpdatePaimonDataEvolutionTableCommand]

        // Materialize the same pinned table produced at the beginning of runOnce, then commit a
        // conflicting update before MergeIntoPaimonDataEvolutionTable resolves its target snapshot.
        val (pinnedTable, pinnedRelation) =
          MergeIntoPaimonDataEvolutionTable.withMatchedUpdateScanOptions(
            updateCommand.v2Table,
            updateCommand.relation)
        val pinnedUpdate = updateCommand.copy(v2Table = pinnedTable, relation = pinnedRelation)

        sql("UPDATE t SET status = 'cancelled' WHERE id = 1")

        val result = Try(pinnedUpdate.run(spark))
        val finalStatus = sql("SELECT status FROM t WHERE id = 1").head().getString(0)
        val detectedConflict = result.failed.toOption.exists(
          hasMessage(_, ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE))

        assert(
          detectedConflict && finalStatus == "cancelled",
          s"Expected a row-id conflict and final status 'cancelled', but got " +
            s"failure=${result.failed.toOption.map(_.toString)}, finalStatus=$finalStatus"
        )
      }
    }
  }

  private def hasMessage(throwable: Throwable, expected: String): Boolean = {
    var current = throwable
    while (current != null) {
      if (Option(current.getMessage).exists(_.contains(expected))) {
        return true
      }
      current = current.getCause
    }
    false
  }
}
