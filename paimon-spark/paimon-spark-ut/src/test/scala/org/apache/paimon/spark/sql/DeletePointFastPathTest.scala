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
}
