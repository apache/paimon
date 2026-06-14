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
import org.apache.paimon.spark.write.SnapshotOperation

/** Verifies the logical operation type recorded in the committed snapshot's `properties` map. */
class SnapshotOperationTest extends PaimonSparkTestBase {

  /** The operation recorded in the latest snapshot of the given table, or null if absent. */
  private def latestOperation(tableName: String): String = {
    val snapshot = loadTable(tableName).snapshotManager().latestSnapshot()
    assert(snapshot != null, s"table $tableName has no snapshot")
    val properties = snapshot.properties()
    if (properties == null) null else properties.get(SnapshotOperation.OPERATION_PROPERTY)
  }

  test("Snapshot operation: INSERT / OVERWRITE / UPDATE / DELETE / MERGE") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("t", "s") {
          sql("CREATE TABLE t (id INT, name STRING)")
          sql("CREATE TABLE s (id INT, name STRING)")
          sql("INSERT INTO s VALUES (1, 'merged'), (9, 'new')")
          sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
          assert(latestOperation("t") == SnapshotOperation.WRITE)

          sql("INSERT OVERWRITE t VALUES (1, 'a2'), (2, 'b2'), (3, 'c2')")
          assert(latestOperation("t") == SnapshotOperation.OVERWRITE)

          sql("UPDATE t SET name = 'updated' WHERE id = 1")
          assert(latestOperation("t") == SnapshotOperation.UPDATE)

          sql("DELETE FROM t WHERE id = 2")
          assert(latestOperation("t") == SnapshotOperation.DELETE)

          sql("""
                |MERGE INTO t USING s ON t.id = s.id
                |WHEN MATCHED THEN UPDATE SET t.name = s.name
                |WHEN NOT MATCHED THEN INSERT *
                |""".stripMargin)
          assert(latestOperation("t") == SnapshotOperation.MERGE)
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
          assert(latestOperation("ctas") == SnapshotOperation.CREATE_TABLE_AS_SELECT)

          sql("CREATE OR REPLACE TABLE rtas AS SELECT * FROM src")
          assert(latestOperation("rtas") == SnapshotOperation.CREATE_OR_REPLACE_TABLE_AS_SELECT)

          // rtas twice
          sql("CREATE OR REPLACE TABLE rtas AS SELECT * FROM src")
          assert(latestOperation("rtas") == SnapshotOperation.CREATE_OR_REPLACE_TABLE_AS_SELECT)
        }
      }
    }
  }
}
