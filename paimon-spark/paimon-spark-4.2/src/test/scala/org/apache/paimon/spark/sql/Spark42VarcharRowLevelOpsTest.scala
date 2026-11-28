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
import org.assertj.core.api.Assertions.assertThatThrownBy

/** V2-write UPDATE and MERGE on a table with a VARCHAR column. */
class Spark42VarcharRowLevelOpsTest extends PaimonSparkTestBase {

  private def explain(statement: String): String =
    sql(s"EXPLAIN EXTENDED $statement").head().getString(0)

  Seq(
    ("copy-on-write", "", "ReplaceData"),
    ("delta", ", 'deletion-vectors.enabled' = 'true'", "WriteDelta")).foreach {
    case (kind, props, plan) =>
      test(s"UPDATE and MERGE on a table with a VARCHAR column use the $kind plan") {
        withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
          withTable("t", "s") {
            sql(
              "CREATE TABLE t (id INT, v VARCHAR(4), b INT) " +
                s"TBLPROPERTIES ('bucket' = '-1'$props)")
            sql("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")
            sql("CREATE TABLE s (id INT, v VARCHAR(4))")
            sql("INSERT INTO s VALUES (1, 'x'), (3, 'z')")

            // Does not assign the VARCHAR column.
            val updateOther = "UPDATE t SET b = 11 WHERE id = 1"
            assert(explain(updateOther).contains(plan))
            sql(updateOther)
            val update = "UPDATE t SET v = 'c' WHERE id = 2"
            assert(explain(update).contains(plan))
            sql(update)
            checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, "a", 11), Row(2, "c", 20)))

            val merge = "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"
            assert(explain(merge).contains(plan))
            sql(merge)
            checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, "x", 11), Row(2, "c", 20)))

            sql(
              "MERGE INTO t USING s ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, v, b) VALUES (s.id, s.v, 0)")
            val merged = Seq(Row(1, "x", 11), Row(2, "c", 20), Row(3, "z", 0))
            checkAnswer(sql("SELECT * FROM t ORDER BY id"), merged)

            // The length limit still applies to the assigned values.
            val tooLong = "Exceeds char/varchar type length limitation"
            assertThatThrownBy(
              () =>
                sql("MERGE INTO t USING (SELECT 1 AS id, 'toolong' AS v) s ON t.id = s.id " +
                  "WHEN MATCHED THEN UPDATE SET v = s.v"))
              .hasMessageContaining(tooLong)
            assertThatThrownBy(() => sql("UPDATE t SET v = 'toolong' WHERE id = 1"))
              .hasMessageContaining(tooLong)
            checkAnswer(sql("SELECT * FROM t ORDER BY id"), merged)
          }
        }
      }
  }
}
