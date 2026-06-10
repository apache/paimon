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

import org.apache.spark.sql.Row

class RowTrackingTest extends RowTrackingTestBase {

  test("Data Evolution: Spark 4.0 uses V2 copy-on-write for DELETE and UPDATE") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql(
          "CREATE TABLE t (id INT, data INT) TBLPROPERTIES " +
            "('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 1), (2, 2)")
        sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (3, 3), (4, 4)")

        sql("DELETE FROM t WHERE id = 2")
        checkAnswer(
          sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
          Seq(Row(1, 1, 0), Row(3, 3, 2), Row(4, 4, 3))
        )

        sql("UPDATE t SET data = 30 WHERE id = 3")
        checkAnswer(
          sql("SELECT *, _ROW_ID FROM t ORDER BY id"),
          Seq(Row(1, 1, 0), Row(3, 30, 2), Row(4, 4, 3))
        )
      }
    }
  }

  test("Data Evolution: partition column update is rejected") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id INT, data INT, dt STRING)
              |PARTITIONED BY (dt)
              |TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 1, 'p1'), (2, 2, 'p2')")

        assert(
          intercept[Exception] {
            sql("UPDATE t SET dt = 'p3' WHERE id = 1")
          }.getMessage
            .contains("Update to partition columns is not supported for data evolution tables"))

        sql("UPDATE t SET data = 10 WHERE id = 1")
        checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 10, "p1"), Row(2, 2, "p2")))
      }
    }
  }
}
