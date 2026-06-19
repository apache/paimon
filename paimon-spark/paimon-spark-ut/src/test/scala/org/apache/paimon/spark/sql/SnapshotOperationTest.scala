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

import org.apache.paimon.Snapshot
import org.apache.paimon.spark.PaimonSparkTestBase

/** Verifies the logical operation type recorded in the committed snapshot. */
class SnapshotOperationTest extends PaimonSparkTestBase {

  private def latestOperation(tableName: String): Snapshot.Operation = {
    val snapshot = loadTable(tableName).snapshotManager().latestSnapshot()
    assert(snapshot != null, s"table $tableName has no snapshot")
    snapshot.operation()
  }

  test("Snapshot operation: INSERT / OVERWRITE / UPDATE / DELETE / MERGE") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("t", "s") {
          sql("CREATE TABLE t (id INT, name STRING)")
          sql("CREATE TABLE s (id INT, name STRING)")
          sql("INSERT INTO s VALUES (1, 'merged'), (9, 'new')")
          sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
          assert(latestOperation("t") == Snapshot.Operation.WRITE)

          sql("INSERT OVERWRITE t VALUES (1, 'a2'), (2, 'b2'), (3, 'c2')")
          assert(latestOperation("t") == Snapshot.Operation.OVERWRITE)

          sql("UPDATE t SET name = 'updated' WHERE id = 1")
          assert(latestOperation("t") == Snapshot.Operation.UPDATE)

          sql("DELETE FROM t WHERE id = 2")
          assert(latestOperation("t") == Snapshot.Operation.DELETE)

          sql("""
                |MERGE INTO t USING s ON t.id = s.id
                |WHEN MATCHED THEN UPDATE SET t.name = s.name
                |WHEN NOT MATCHED THEN INSERT *
                |""".stripMargin)
          assert(latestOperation("t") == Snapshot.Operation.MERGE)
        }
      }
    }
  }

  test("Snapshot operation: CTAS / RTAS") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("src", "ctas", "rtas") {
          sql("CREATE TABLE src (id INT, name STRING)")
          sql("INSERT INTO src VALUES (1, 'a'), (2, 'b')")

          sql("CREATE TABLE ctas AS SELECT * FROM src")
          assert(latestOperation("ctas") == Snapshot.Operation.CREATE_TABLE_AS_SELECT)

          sql("CREATE OR REPLACE TABLE rtas AS SELECT * FROM src")
          assert(latestOperation("rtas") == Snapshot.Operation.CREATE_OR_REPLACE_TABLE_AS_SELECT)

          // rtas twice
          sql("CREATE OR REPLACE TABLE rtas AS SELECT * FROM src")
          assert(latestOperation("rtas") == Snapshot.Operation.CREATE_OR_REPLACE_TABLE_AS_SELECT)
        }
      }
    }
  }

  test("Snapshot operation: TRUNCATE") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING, dt STRING) PARTITIONED BY (dt)")
      sql("INSERT INTO t VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02')")

      // Full-table DELETE (no WHERE) is optimized to truncateTable
      sql("DELETE FROM t")
      assert(latestOperation("t") == Snapshot.Operation.TRUNCATE)

      sql("INSERT INTO t VALUES (3, 'c', '2024-01-01'), (4, 'd', '2024-01-02')")

      // Partition DELETE is optimized to truncatePartitions
      sql("DELETE FROM t WHERE dt = '2024-01-01'")
      assert(latestOperation("t") == Snapshot.Operation.TRUNCATE)

      sql("INSERT INTO t VALUES (5, 'e', '2024-01-01')")

      // TRUNCATE TABLE
      sql("TRUNCATE TABLE t")
      assert(latestOperation("t") == Snapshot.Operation.TRUNCATE)
    }
  }
}
