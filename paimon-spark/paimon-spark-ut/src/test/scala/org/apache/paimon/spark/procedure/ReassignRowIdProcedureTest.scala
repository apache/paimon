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
import org.assertj.core.api.Assertions.assertThatThrownBy

/** IT Case for [[ReassignRowIdProcedure]]. */
class ReassignRowIdProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: reassign row ids for interleaved partitions") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t (id INT, pt STRING)
             |TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true')
             |PARTITIONED BY (pt)
             |""".stripMargin)
      // each INSERT is its own commit, so row ids are assigned in commit order and
      // interleave across the 'a' and 'b' partitions
      sql("INSERT INTO t VALUES (0, 'a')")
      sql("INSERT INTO t VALUES (1, 'b')")
      sql("INSERT INTO t VALUES (2, 'a')")
      sql("INSERT INTO t VALUES (3, 'b')")
      sql("INSERT INTO t VALUES (4, 'a')")

      checkAnswer(
        sql("SELECT id, pt, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(0, "a", 0), Row(1, "b", 1), Row(2, "a", 2), Row(3, "b", 3), Row(4, "a", 4)))

      val result = sql("CALL sys.reassign_row_id(table => 't')").collect()
      assert(result.length == 1)
      assert(
        result(0).getString(0).startsWith("Success."),
        s"Unexpected result: ${result(0).getString(0)}")

      // row ids are now contiguous within each partition, data is unaffected
      checkAnswer(
        sql("SELECT id, pt, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(0, "a", 5), Row(1, "b", 8), Row(2, "a", 6), Row(3, "b", 9), Row(4, "a", 7)))

      // calling again on already-contiguous row ids is a no-op
      val second = sql("CALL sys.reassign_row_id(table => 't')").collect()
      assert(
        second(0).getString(0).startsWith("Skipped."),
        s"Unexpected result: ${second(0).getString(0)}")
    }
  }

  test("Paimon Procedure: reassign row id with partitions filter") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t (id INT, pt STRING)
             |TBLPROPERTIES (
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true')
             |PARTITIONED BY (pt)
             |""".stripMargin)
      sql("INSERT INTO t VALUES (0, 'a')")
      sql("INSERT INTO t VALUES (1, 'b')")
      sql("INSERT INTO t VALUES (2, 'a')")

      // no partition matches this filter, so nothing needs to be reassigned
      val skipped =
        sql("CALL sys.reassign_row_id(table => 't', partitions => 'pt=c')").collect()
      assert(
        skipped(0).getString(0).startsWith("Skipped."),
        s"Unexpected result: ${skipped(0).getString(0)}")

      val result =
        sql("CALL sys.reassign_row_id(table => 't', partitions => 'pt=a')").collect()
      assert(
        result(0).getString(0).startsWith("Success."),
        s"Unexpected result: ${result(0).getString(0)}")

      checkAnswer(
        sql("SELECT id, pt FROM t ORDER BY id"),
        Seq(Row(0, "a"), Row(1, "b"), Row(2, "a")))
    }
  }

  test("Paimon Procedure: reassign row id requires row tracking enabled") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, pt STRING) PARTITIONED BY (pt)")
      sql("INSERT INTO t VALUES (0, 'a')")

      assertThatThrownBy(() => sql("CALL sys.reassign_row_id(table => 't')"))
        .hasMessageContaining("row-tracking.enabled=true")
    }
  }

  test("Paimon Procedure: reassign row id requires data evolution enabled") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT, pt STRING) TBLPROPERTIES ('row-tracking.enabled' = 'true') PARTITIONED BY (pt)")
      sql("INSERT INTO t VALUES (0, 'a')")

      assertThatThrownBy(() => sql("CALL sys.reassign_row_id(table => 't')"))
        .hasMessageContaining("data-evolution.enabled=true")
    }
  }

  test("Paimon Procedure: reassign row id skips non-partitioned table") {
    withTable("t") {
      sql(
        "CREATE TABLE t (id INT) TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
      sql("INSERT INTO t VALUES (1)")

      val result = sql("CALL sys.reassign_row_id(table => 't')").collect()
      assert(
        result(0).getString(0).contains("table is not partitioned"),
        s"Unexpected result: ${result(0).getString(0)}")
    }
  }
}
