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

/** Tests for the DELETE point-delete fast path (pk fully pinned by literals -> no table scan). */
class DeletePointFastPathTest extends PaimonSparkTestBase {

  private def totalRecordCount(tableName: String): Long =
    spark
      .sql(s"SELECT COALESCE(SUM(record_count), 0) FROM `$tableName$$files`")
      .head()
      .getLong(0)

  /**
   * The fast path writes a -D row even for a key that does not exist, while the scan path finds
   * nothing to delete and writes no record at all. So deleting an absent key tells the two paths
   * apart. The table must be 'write-only' so that no compaction removes the record again.
   */
  private def assertFastPath(tableName: String, condition: String): Unit = {
    val before = totalRecordCount(tableName)
    spark.sql(s"DELETE FROM $tableName WHERE $condition")
    assert(
      totalRecordCount(tableName) == before + 1,
      s"expected the fast path to write one -D row for the absent key ($condition)")
  }

  private def assertScanPath(tableName: String, condition: String): Unit = {
    val before = totalRecordCount(tableName)
    spark.sql(s"DELETE FROM $tableName WHERE $condition")
    assert(
      totalRecordCount(tableName) == before,
      s"expected the scan path to write nothing for the absent key ($condition)")
  }

  test("Point delete fast path: pk IN literals") {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO T VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")

    // includes a non-existing key 99: harmless
    spark.sql("DELETE FROM T WHERE id IN (2, 4, 99)")

    checkAnswer(spark.sql("SELECT id FROM T ORDER BY id"), Row(1) :: Row(3) :: Row(5) :: Nil)
  }

  test("Point delete fast path: pk equality") {
    spark.sql("""
                |CREATE TABLE TEQ (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TEQ VALUES (1,10),(2,20),(3,30)")

    spark.sql("DELETE FROM TEQ WHERE id = 2")

    checkAnswer(spark.sql("SELECT id FROM TEQ ORDER BY id"), Row(1) :: Row(3) :: Nil)
  }

  test("Point delete fast path: composite pk (partition + id)") {
    spark.sql("""
                |CREATE TABLE PT (id INT, name STRING, dt STRING)
                |PARTITIONED BY (dt)
                |TBLPROPERTIES ('primary-key' = 'dt,id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("""
                |INSERT INTO PT VALUES
                | (1,'a','p1'),(2,'b','p1'),(3,'c','p1'),
                | (1,'x','p2'),(2,'y','p2')
                |""".stripMargin)

    // dt pinned by equality, id by IN -> covers full pk (dt, id)
    spark.sql("DELETE FROM PT WHERE dt = 'p1' AND id IN (1, 3)")

    checkAnswer(
      spark.sql("SELECT id, dt FROM PT ORDER BY dt, id"),
      Row(2, "p1") :: Row(1, "p2") :: Row(2, "p2") :: Nil)
  }

  test("Fallback: condition with non-pk column still works (scan path)") {
    spark.sql("""
                |CREATE TABLE TF (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TF VALUES (1,10),(2,20),(3,30)")

    // v is not a pk column -> must fall back to scan-based delete and still be correct
    spark.sql("DELETE FROM TF WHERE id IN (1, 2) AND v > 15")

    checkAnswer(spark.sql("SELECT id FROM TF ORDER BY id"), Row(1) :: Row(3) :: Nil)
  }

  test("Fallback: pk not fully pinned still works (scan path)") {
    spark.sql("""
                |CREATE TABLE TP (id INT, name STRING, dt STRING)
                |PARTITIONED BY (dt)
                |TBLPROPERTIES ('primary-key' = 'dt,id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TP VALUES (1,'a','p1'),(1,'x','p2'),(2,'b','p1')")

    // only id pinned, dt free -> not a point delete, scan path deletes across partitions
    spark.sql("DELETE FROM TP WHERE id = 1")

    checkAnswer(spark.sql("SELECT id, dt FROM TP ORDER BY dt, id"), Row(2, "p1") :: Nil)
  }

  test("Point delete then re-insert same key") {
    spark.sql("""
                |CREATE TABLE TR (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TR VALUES (1,10),(2,20)")
    spark.sql("DELETE FROM TR WHERE id = 1")
    spark.sql("INSERT INTO TR VALUES (1, 111)")

    checkAnswer(spark.sql("SELECT * FROM TR ORDER BY id"), Row(1, 111L) :: Row(2, 20L) :: Nil)
  }

  test("Subquery fast path: pk IN (SELECT ...) from key table") {
    spark.sql("""
                |CREATE TABLE TS (id INT, name STRING, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TS VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")

    // key table with existing keys (2, 4) and a non-existing key (99)
    spark.sql("CREATE TABLE SKEYS (id INT)")
    spark.sql("INSERT INTO SKEYS VALUES (2), (4), (99)")

    spark.sql("DELETE FROM TS WHERE id IN (SELECT id FROM SKEYS)")

    checkAnswer(spark.sql("SELECT id FROM TS ORDER BY id"), Row(1) :: Row(3) :: Row(5) :: Nil)

    spark.sql("CALL paimon.sys.compact(table => 'test.TS', compact_strategy => 'full')")
    checkAnswer(spark.sql("SELECT id FROM TS ORDER BY id"), Row(1) :: Row(3) :: Row(5) :: Nil)
  }

  test("Subquery fast path: composite pk IN (SELECT ...) with renamed columns") {
    spark.sql("""
                |CREATE TABLE TS2 (id INT, name STRING, dt STRING)
                |PARTITIONED BY (dt)
                |TBLPROPERTIES ('primary-key' = 'dt,id', 'bucket' = '2',
                | 'deletion-vectors.enabled' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TS2 VALUES (1,'a','p1'),(2,'b','p1'),(1,'x','p2'),(2,'y','p2')")

    // key table columns named differently; aligned via SELECT aliases in the subquery
    spark.sql("CREATE TABLE SKEYS2 (kid INT, kdt STRING)")
    spark.sql("INSERT INTO SKEYS2 VALUES (1, 'p1'), (2, 'p2')")

    spark.sql("DELETE FROM TS2 WHERE (dt, id) IN (SELECT kdt, kid FROM SKEYS2)")

    checkAnswer(
      spark.sql("SELECT id, dt FROM TS2 ORDER BY dt, id"),
      Row(2, "p1") :: Row(1, "p2") :: Nil)
  }

  test("Fallback: NULL literal in condition follows SQL three-valued logic") {
    spark.sql("""
                |CREATE TABLE TNULL (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
                |""".stripMargin)
    spark.sql("INSERT INTO TNULL VALUES (1,10),(2,20)")

    // id = NULL / IN (..., NULL): NULL never matches, rows must NOT be deleted blindly
    spark.sql("DELETE FROM TNULL WHERE id = NULL")
    checkAnswer(spark.sql("SELECT count(*) FROM TNULL"), Row(2) :: Nil)

    spark.sql("DELETE FROM TNULL WHERE id IN (1, NULL)")
    checkAnswer(spark.sql("SELECT id FROM TNULL"), Row(2) :: Nil)
  }

  test("Fallback: table with NOT NULL non-pk column still works (scan path)") {
    spark.sql("""
                |CREATE TABLE TNN (id INT, name STRING NOT NULL, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
                |""".stripMargin)
    spark.sql("INSERT INTO TNN VALUES (1,'a',10),(2,'b',20),(3,'c',30)")

    // fast path would write NULL into the NOT NULL column; must fall back and still succeed
    spark.sql("DELETE FROM TNN WHERE id IN (1, 3)")
    checkAnswer(spark.sql("SELECT id FROM TNN"), Row(2) :: Nil)
  }

  test("Fallback: partial-update with sequence-group delete keeps its semantics (scan path)") {
    spark.sql("""
                |CREATE TABLE TPU (id INT, g INT, v BIGINT)
                |TBLPROPERTIES (
                | 'primary-key' = 'id',
                | 'bucket' = '2',
                | 'merge-engine' = 'partial-update',
                | 'fields.g.sequence-group' = 'v',
                | 'partial-update.remove-record-on-sequence-group' = 'g')
                |""".stripMargin)
    spark.sql("INSERT INTO TPU VALUES (1, 1, 10), (2, 1, 20)")

    // The fast path would blank the sequence-group field g and change semantics; it must
    // fall back to the scan path so the behavior stays identical to master (which currently
    // keeps both rows for this configuration, see #8858).
    spark.sql("DELETE FROM TPU WHERE id = 1")
    checkAnswer(spark.sql("SELECT id FROM TPU ORDER BY id"), Row(1) :: Row(2) :: Nil)
  }

  test("Subquery fast path: NULL keys in the subquery delete nothing") {
    spark.sql("""
                |CREATE TABLE TSN (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'write-only' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TSN VALUES (1,10),(2,20)")
    spark.sql("CREATE TABLE SKEYS3 (id INT)")
    spark.sql("INSERT INTO SKEYS3 VALUES (CAST(NULL AS INT))")

    // NULL never matches, so no -D row may be written: otherwise we would store a NULL pk
    val before = totalRecordCount("TSN")
    spark.sql("DELETE FROM TSN WHERE id IN (SELECT id FROM SKEYS3)")
    assert(totalRecordCount("TSN") == before, "NULL keys must not produce -D rows")

    spark.sql("CALL sys.compact(table => 'test.TSN', compact_strategy => 'full')")
    checkAnswer(spark.sql("SELECT id FROM TSN ORDER BY id"), Row(1) :: Row(2) :: Nil)
  }

  test("Fallback: correlated subquery still works (scan path)") {
    spark.sql("""
                |CREATE TABLE TC (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
                |""".stripMargin)
    spark.sql("INSERT INTO TC VALUES (1,10),(2,20),(3,30)")
    spark.sql("CREATE TABLE CKEYS (id INT, v BIGINT)")
    spark.sql("INSERT INTO CKEYS VALUES (1,5),(2,30),(3,5)")

    // correlated: the subquery references the target table, so it can not be planned alone
    spark.sql("DELETE FROM TC WHERE id IN (SELECT id FROM CKEYS c WHERE c.v > TC.v)")
    checkAnswer(spark.sql("SELECT id FROM TC ORDER BY id"), Row(1) :: Row(3) :: Nil)
  }

  test("Fallback: sequence.field falls back to scan path") {
    spark.sql("""
                |CREATE TABLE TSEQ (id INT, v BIGINT, ts BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2',
                | 'sequence.field' = 'ts', 'write-only' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TSEQ VALUES (1,10,100),(2,20,100)")

    // a NULL sequence value would sort oldest and lose the merge, so the fast path is off
    assertScanPath("TSEQ", "id = 999")

    spark.sql("DELETE FROM TSEQ WHERE id = 1")
    spark.sql("CALL sys.compact(table => 'test.TSEQ', compact_strategy => 'full')")
    checkAnswer(spark.sql("SELECT id FROM TSEQ"), Row(2) :: Nil)
  }

  test("Fallback: cross-partition table falls back to scan path") {
    // pk does not contain the partition field -> cross partition update, the -D row would carry
    // a NULL partition
    spark.sql("""
                |CREATE TABLE TXP (id INT, v BIGINT, dt STRING)
                |PARTITIONED BY (dt)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '-1')
                |""".stripMargin)
    spark.sql("INSERT INTO TXP VALUES (1,10,'p1'),(2,20,'p2')")

    spark.sql("DELETE FROM TXP WHERE id = 1")
    checkAnswer(spark.sql("SELECT id, dt FROM TXP"), Row(2, "p2") :: Nil)
  }

  test("Fallback: too many literal keys falls back to scan path") {
    withSparkSQLConf("spark.paimon.delete.point-delete.max-rows" -> "1") {
      spark.sql("""
                  |CREATE TABLE TMAX (id INT, v BIGINT)
                  |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'write-only' = 'true')
                  |""".stripMargin)
      spark.sql("INSERT INTO TMAX VALUES (1,10),(2,20),(3,30)")

      assertScanPath("TMAX", "id IN (998, 999)")

      spark.sql("DELETE FROM TMAX WHERE id IN (1, 2)")
      spark.sql("CALL sys.compact(table => 'test.TMAX', compact_strategy => 'full')")
      checkAnswer(spark.sql("SELECT id FROM TMAX"), Row(3) :: Nil)
    }
  }

  test("Fast path is on by default") {
    spark.sql("""
                |CREATE TABLE TOFF (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'write-only' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TOFF VALUES (1,10),(2,20)")

    assertFastPath("TOFF", "id = 999")
  }

  test("Point delete fast path really skips the scan") {
    spark.sql("""
                |CREATE TABLE TFP (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'write-only' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TFP VALUES (1,10),(2,20)")

    assertFastPath("TFP", "id = 999")

    spark.sql("CALL sys.compact(table => 'test.TFP', compact_strategy => 'full')")
    checkAnswer(spark.sql("SELECT id FROM TFP ORDER BY id"), Row(1) :: Row(2) :: Nil)
  }

  test("Fallback: delete.force-produce-changelog falls back to scan path") {
    spark.sql("""
                |CREATE TABLE TFPC (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'write-only' = 'true',
                | 'delete.force-produce-changelog' = 'true')
                |""".stripMargin)
    spark.sql("INSERT INTO TFPC VALUES (1,10),(2,20)")

    // the user asks for a faithful changelog, so the -D rows must carry the old field values
    assertScanPath("TFPC", "id = 999")

    spark.sql("DELETE FROM TFPC WHERE id = 1")
    spark.sql("CALL sys.compact(table => 'test.TFPC', compact_strategy => 'full')")
    checkAnswer(spark.sql("SELECT id FROM TFPC"), Row(2) :: Nil)
  }

  test("Fallback: changelog-producer falls back to scan path") {
    spark.sql("""
                |CREATE TABLE TCP (id INT, v BIGINT)
                |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'write-only' = 'true',
                | 'changelog-producer' = 'input')
                |""".stripMargin)
    spark.sql("INSERT INTO TCP VALUES (1,10),(2,20)")

    // a streaming consumer would retract with NULL field values instead of the old ones
    assertScanPath("TCP", "id = 999")

    spark.sql("DELETE FROM TCP WHERE id = 1")
    spark.sql("CALL sys.compact(table => 'test.TCP', compact_strategy => 'full')")
    checkAnswer(spark.sql("SELECT id FROM TCP"), Row(2) :: Nil)
  }

  test("Fallback: cartesian product over max-rows falls back to scan path") {
    withSparkSQLConf("spark.paimon.delete.point-delete.max-rows" -> "3") {
      spark.sql("""
                  |CREATE TABLE TCART (id INT, name STRING, dt STRING)
                  |PARTITIONED BY (dt)
                  |TBLPROPERTIES ('primary-key' = 'dt,id', 'bucket' = '2', 'write-only' = 'true')
                  |""".stripMargin)
      spark.sql("INSERT INTO TCART VALUES (1,'a','p1'),(2,'b','p1'),(1,'x','p2'),(2,'y','p2')")

      // 2 partitions x 2 ids = 4 combinations > 3
      assertScanPath("TCART", "dt IN ('p8','p9') AND id IN (998, 999)")

      // 1 partition x 2 ids = 2 combinations, still within the limit
      spark.sql("DELETE FROM TCART WHERE dt IN ('p1') AND id IN (1, 2)")
      spark.sql("CALL sys.compact(table => 'test.TCART', compact_strategy => 'full')")
      checkAnswer(
        spark.sql("SELECT id, dt FROM TCART ORDER BY id"),
        Row(1, "p2") :: Row(2, "p2") :: Nil)
    }
  }
}
