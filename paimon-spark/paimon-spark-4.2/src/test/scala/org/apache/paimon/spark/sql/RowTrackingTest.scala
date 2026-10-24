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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.schema.PaimonMetadataColumn

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.types.Metadata

class RowTrackingTest extends RowTrackingTestBase {

  test("Row Tracking: metadata columns expose Spark preserve flags") {
    val rowIdMetadata = Metadata.fromJson(PaimonMetadataColumn.ROW_ID.metadataInJSON())
    assert(rowIdMetadata.getBoolean("__preserve_on_delete"))
    assert(rowIdMetadata.getBoolean("__preserve_on_update"))
    assert(!rowIdMetadata.getBoolean("__preserve_on_reinsert"))

    val sequenceNumberMetadata =
      Metadata.fromJson(PaimonMetadataColumn.SEQUENCE_NUMBER.metadataInJSON())
    assert(sequenceNumberMetadata.getBoolean("__preserve_on_delete"))
    assert(!sequenceNumberMetadata.getBoolean("__preserve_on_update"))
    assert(!sequenceNumberMetadata.getBoolean("__preserve_on_reinsert"))
  }

  test("Row Tracking: Spark 4.1 uses V2 copy-on-write for DML") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("s", "t") {
        sql("CREATE TABLE t (id INT, data INT) TBLPROPERTIES ('row-tracking.enabled' = 'true')")
        sql("INSERT INTO t VALUES (1, 1), (2, 2)")
        sql("INSERT INTO t VALUES (3, 3), (4, 4)")

        assertPlanContains("DELETE FROM t WHERE id = 2", "ReplaceData")
        sql("DELETE FROM t WHERE id = 2")

        assertPlanContains("UPDATE t SET data = 30 WHERE id = 3", "ReplaceData")
        sql("UPDATE t SET data = 30 WHERE id = 3")

        sql("CREATE TABLE s (id INT, data INT)")
        sql("INSERT INTO s VALUES (3, 300), (5, 500)")
        assertPlanContains(
          """
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN UPDATE SET data = s.data
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin,
          "ReplaceData"
        )
        sql("""
              |MERGE INTO t
              |USING s
              |ON t.id = s.id
              |WHEN MATCHED THEN UPDATE SET data = s.data
              |WHEN NOT MATCHED THEN INSERT *
              |""".stripMargin)

        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(1, 1, 0, 1), Row(3, 300, 2, 5), Row(4, 4, 3, 2), Row(5, 500, 4, 5))
        )
      }
    }
  }

  test("Row Tracking: nested CHAR columns do not expose V2 row-level capability") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (
              |  id INT,
              |  info STRUCT<name: CHAR(3), data: INT>
              |) TBLPROPERTIES ('row-tracking.enabled' = 'true')
              |""".stripMargin)

        assert(!SparkTable.of(loadTable("t")).isInstanceOf[SupportsRowLevelOperations])
      }
    }
  }

  test("Row Tracking: Spark 4.1 restores metadata-only delete fast path") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, data INT, dt STRING)
              |PARTITIONED BY (dt)
              |TBLPROPERTIES ('row-tracking.enabled' = 'true')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 1, 'p1'), (2, 2, 'p1'), (3, 3, 'p2')")

        assertPlanContains("DELETE FROM t WHERE dt = 'p1'", "DeleteFromPaimonTableCommand")
        sql("DELETE FROM t WHERE dt = 'p1'")

        checkAnswer(
          sql("SELECT *, _ROW_ID, _SEQUENCE_NUMBER FROM t ORDER BY id"),
          Seq(Row(3, 3, "p2", 0, 1))
        )
      }
    }
  }

  private def assertPlanContains(sqlText: String, fragment: String): Unit = {
    val plan = explain(sqlText)
    assert(plan.contains(fragment), plan)
  }

  private def explain(sqlText: String): String = {
    sql(s"EXPLAIN EXTENDED $sqlText").collect().map(_.getString(0)).mkString("\n")
  }
}
