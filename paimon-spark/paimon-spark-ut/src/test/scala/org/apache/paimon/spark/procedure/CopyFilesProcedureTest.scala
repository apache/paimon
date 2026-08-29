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

import java.util.concurrent.ThreadLocalRandom

import scala.collection.JavaConverters._

class CopyFilesProcedureTest extends PaimonSparkTestBase {

  test("Paimon copy files procedure: append table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      sql(s"INSERT INTO tbl$random VALUES (4, 'd'), (5, 'e'), (6, 'f')")

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )
    }
  }

  test("Paimon copy files procedure: partitioned append table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )
    }
  }

  test("Paimon copy files procedure: partitioned append table with partition filter") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      checkAnswer(
        sql(
          s"""CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random', where => "dt = '2025-08-17' and hh = 5")"""),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random WHERE dt = '2025-08-17' and hh = 5")
      )
    }
  }

  test("Paimon copy files procedure: pk table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |TBLPROPERTIES (
             |  'primary-key' = 'dt,hh,k',
             |  'bucket' = '-1')
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")
      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

    }
  }

  test("Paimon copy files procedure: schema change") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      // source table
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)
      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")

      sql(s"""
             |ALTER TABLE tbl$random
             |DROP COLUMN v
             |""".stripMargin)

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random"),
        sql(s"SELECT * FROM tbl$random")
      )

    }
  }

  test("Paimon copy files procedure: global index schema compatibility") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    val source = s"source_tbl$random"
    val compatibleTarget = s"compatible_target_tbl$random"
    val incompatibleTarget = s"incompatible_target_tbl$random"
    withTable(source, compatibleTarget, incompatibleTarget) {
      sql(s"""
             |CREATE TABLE $source (id INT, idx INT, payload STRING)
             |TBLPROPERTIES (
             |  'bucket' = '-1',
             |  'global-index.enabled' = 'true',
             |  'row-tracking.enabled' = 'true',
             |  'data-evolution.enabled' = 'true',
             |  'global-index.column-update-action' = 'IGNORE',
             |  'btree-index.records-per-range' = '2')
             |""".stripMargin)
      sql(s"ALTER TABLE $source RENAME COLUMN payload TO payload_at_build")
      sql(s"INSERT INTO $source VALUES (1, 10, 'a'), (2, 20, 'b')")
      spark
        .sql(s"CALL sys.create_global_index(table => 'test.$source', " +
          "index_column => 'idx', index_type => 'btree')")
        .collect()

      val sourceEntries =
        loadTable(source).store().newIndexFileHandler().scan("btree").asScala
      assert(sourceEntries.nonEmpty)
      val buildSchemaId = sourceEntries.head.schemaId().longValue()

      sql(s"ALTER TABLE $source RENAME COLUMN payload_at_build TO payload_after_build")
      assert(loadTable(source).schema().id() != buildSchemaId)

      checkAnswer(
        sql(s"CALL sys.copy(source_table => '$source', target_table => '$compatibleTarget')"),
        Row(true) :: Nil
      )

      val compatibleTargetTable = loadTable(compatibleTarget)
      val targetEntries =
        compatibleTargetTable.store().newIndexFileHandler().scan("btree").asScala
      assert(targetEntries.size == sourceEntries.size)
      assert(compatibleTargetTable.schema().id() != buildSchemaId)
      targetEntries.foreach {
        entry =>
          assert(entry.indexFile().globalIndexMeta() != null)
          assert(entry.schemaId().longValue() == compatibleTargetTable.schema().id())
          assert(entry.indexFile().schemaId().longValue() == compatibleTargetTable.schema().id())
          assert(
            compatibleTargetTable
              .fileIO()
              .exists(
                compatibleTargetTable
                  .store()
                  .pathFactory()
                  .globalIndexFileFactory()
                  .toPath(entry.indexFile())))
      }
      checkAnswer(sql(s"SELECT id FROM $compatibleTarget WHERE idx = 20"), Row(2) :: Nil)

      sql(s"ALTER TABLE $source ALTER COLUMN idx TYPE BIGINT")
      checkAnswer(
        sql(s"CALL sys.copy(source_table => '$source', target_table => '$incompatibleTarget')"),
        Row(true) :: Nil
      )
      assert(
        loadTable(incompatibleTarget)
          .store()
          .newIndexFileHandler()
          .scan("btree")
          .isEmpty)
    }
  }

  test("Paimon copy files procedure: copy to existed table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      // source table
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)
      sql(s"INSERT INTO tbl$random VALUES (1, 'a', '2025-08-17', 5), (2, 'b', '2025-10-06', 0)")

      // target table
      sql(s"""
             |CREATE TABLE target_tbl$random (k INT, v STRING, dt STRING, hh INT, v2 STRING)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)
      // partition should overwrite
      sql(
        s"INSERT INTO target_tbl$random VALUES (3, 'c', '2025-08-17', 5, 'c1'), (4, 'd', '2025-08-17', 6, 'd1')")

      checkAnswer(
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')"),
        Row(true) :: Nil
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random WHERE dt = '2025-08-17' and hh = 5"),
        Row(1, "a", "2025-08-17", 5, null)
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random WHERE dt = '2025-10-06' and hh = 0"),
        Row(2, "b", "2025-10-06", 0, null)
      )

      checkAnswer(
        sql(s"SELECT * FROM target_tbl$random WHERE dt = '2025-08-17' and hh = 6"),
        Row(4, "d", "2025-08-17", 6, "d1")
      )
    }
  }

  test("Paimon copy files procedure: copy to existed compatible table") {
    val random = ThreadLocalRandom.current().nextInt(100000);
    withTable(s"tbl$random") {
      // source table
      sql(s"""
             |CREATE TABLE tbl$random (k INT, v STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      // target table
      sql(s"""
             |CREATE TABLE target_tbl$random (k INT, v2 STRING, dt STRING, hh INT)
             |PARTITIONED BY (dt, hh)
             |""".stripMargin)

      assertThrows[RuntimeException] {
        sql(s"CALL sys.copy(source_table => 'tbl$random', target_table => 'target_tbl$random')")
      }
    }
  }
}
