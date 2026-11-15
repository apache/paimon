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

class ReplaceByKeysProcedureTest extends PaimonSparkTestBase {

  private def createTarget(): Unit = {
    sql("""
          |CREATE TABLE target (tenant INT, entity STRING, value STRING)
          |TBLPROPERTIES ('data-evolution.enabled'='true', 'row-tracking.enabled'='true',
          | 'deletion-vectors.enabled'='true', 'write-only'='true',
          | 'snapshot.num-retained.min'='10')
          |""".stripMargin)
    sql("""
          |INSERT INTO target VALUES (7,'a','old-1'),(7,'a','old-2'),(7,'b','keep'),
          | (8,'a','outside'),(7,NULL,'old-null')
          |""".stripMargin)
    sql("CREATE TABLE source (tenant INT, entity STRING, value STRING)")
    sql("""
          |INSERT INTO source VALUES (7,'a','new-1'),(7,'a','new-2'),
          | (7,'c',NULL),(7,NULL,'new-null'),(8,'a','excluded')
          |""".stripMargin)
  }

  test("replace by composite keys preserves IN semantics, duplicates and untouched row ids") {
    withTable("target", "source") {
      createTarget()
      val table = loadTable("target")
      val before = table.snapshotManager().latestSnapshot()
      val untouched = sql("SELECT entity, value, _ROW_ID FROM target WHERE entity='b'")
        .collect()
        .toSeq

      sql("""
            |CALL sys.replace_by_keys(
            | table => 'target',
            | source_query => 'SELECT value,entity,tenant FROM source WHERE tenant=7',
            | key_columns => array('tenant','entity'),
            | target_filter => 'tenant=7')
            |""".stripMargin)

      checkAnswer(
        sql("SELECT tenant,entity,value FROM target"),
        Seq(
          Row(7, "a", "new-1"),
          Row(7, "a", "new-2"),
          Row(7, "c", null),
          Row(7, null, "new-null"),
          Row(7, "b", "keep"),
          Row(8, "a", "outside"),
          Row(7, null, "old-null"))
      )
      checkAnswer(sql("SELECT entity,value,_ROW_ID FROM target WHERE entity='b'"), untouched)
      assert(table.snapshotManager().latestSnapshot().id() == before.id() + 1)
      val newIds = sql("SELECT _ROW_ID FROM target WHERE value LIKE 'new-%' OR entity='c'")
        .collect()
        .map(_.getLong(0))
      assert(newIds.length == 4 && newIds.distinct.length == 4)
      assert(newIds.forall(_ >= before.nextRowId().longValue()))
    }
  }

  test("failed append cannot publish the replacement's deletions") {
    withTable("target", "source") {
      createTarget()
      val table = loadTable("target")
      val beforeId = table.snapshotManager().latestSnapshot().id()
      val beforeRows = sql("SELECT tenant,entity,value,_ROW_ID FROM target").collect().toSeq
      val error = intercept[Exception] {
        sql("""
              |CALL sys.replace_by_keys(
              | table => 'target',
              | source_query => "SELECT tenant,entity,CASE WHEN length(entity)>0 THEN
              | CAST(raise_error('replace-insert-execution-failure') AS STRING)
              | ELSE value END AS value FROM source WHERE tenant=7",
              | key_columns => array('tenant','entity'),
              | target_filter => 'tenant=7')
              |""".stripMargin)
      }
      var cause: Throwable = error
      var executionFailed = false
      while (cause != null) {
        executionFailed ||= Option(cause.getMessage)
          .exists(_.contains("replace-insert-execution-failure"))
        cause = cause.getCause
      }
      assert(executionFailed)
      assert(table.snapshotManager().latestSnapshot().id() == beforeId)
      checkAnswer(sql("SELECT tenant,entity,value,_ROW_ID FROM target"), beforeRows)
    }
  }

  test("source query cannot execute a command") {
    withTable("target", "source") {
      createTarget()
      val beforeSource = sql("SELECT * FROM source").collect().toSeq
      val beforeTarget = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
      Seq("DELETE FROM source", "INSERT INTO source VALUES (9,'mutation','bad')").foreach {
        command =>
          val quoted = command.replace("\\", "\\\\").replace("\"", "\\\"")
          intercept[IllegalArgumentException] {
            sql(s"""
                   |CALL sys.replace_by_keys(table => 'target',
                   | source_query => "$quoted",
                   | key_columns => array('tenant','entity'))
                   |""".stripMargin)
          }
      }
      checkAnswer(sql("SELECT * FROM source"), beforeSource)
      checkAnswer(sql("SELECT *,_ROW_ID FROM target"), beforeTarget)
    }
  }

  test("INSERT coercion must not change the deletion keys") {
    withTable("target") {
      withSQLConf("spark.sql.storeAssignmentPolicy" -> "ANSI") {
        sql("""
              |CREATE TABLE target (id INT, value STRING)
              |TBLPROPERTIES ('data-evolution.enabled'='true','row-tracking.enabled'='true',
              | 'deletion-vectors.enabled'='true','write-only'='true')
              |""".stripMargin)
        sql("INSERT INTO target VALUES (1,'old')")
        sql("""
              |CALL sys.replace_by_keys(table => 'target',
              | source_query => "SELECT CAST(1.9 AS DOUBLE) AS id, 'new' AS value",
              | key_columns => ARRAY('id'))
              |""".stripMargin)
        // DELETE id IN (1.9D) does not select 1, although INSERT casts the new id to 1.
        checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "old"), Row(1, "new")))
      }
    }
  }

  test("case insensitive column matching follows the Spark resolver") {
    withTable("target") {
      withSQLConf("spark.sql.caseSensitive" -> "false") {
        sql("""
              |CREATE TABLE target (`İ` INT, value STRING)
              |TBLPROPERTIES ('data-evolution.enabled'='true','row-tracking.enabled'='true',
              | 'deletion-vectors.enabled'='true','write-only'='true')
              |""".stripMargin)
        sql("INSERT INTO target VALUES (1,'old')")
        sql("""
              |CALL sys.replace_by_keys(table => 'target',
              | source_query => "SELECT 1 AS i, 'new' AS value",
              | key_columns => ARRAY('i'))
              |""".stripMargin)
        checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "new")))
      }
    }
  }

  test("pinning source data preserves columns added after the last write") {
    withTable("target", "source") {
      createTarget()
      sql("ALTER TABLE source ADD COLUMN extra STRING")
      sql("ALTER TABLE target ADD COLUMN extra STRING")
      val before = loadTable("target").snapshotManager().latestSnapshot().id()
      try {
        spark.read
          .option("scan.mode", "latest-delta")
          .table("source")
          .createOrReplaceTempView("replacement_batch")
        val error = intercept[IllegalArgumentException] {
          sql("""
                |CALL sys.replace_by_keys(table => 'target',
                | source_query => 'SELECT * FROM replacement_batch',
                | key_columns => ARRAY('tenant','entity'))
                |""".stripMargin)
        }
        assert(error.getMessage.contains("Source tables must use a full snapshot scan"))
        assert(loadTable("target").snapshotManager().latestSnapshot().id() == before)
        spark.read
          .option("scan.mode", "latest-full")
          .table("source")
          .createOrReplaceTempView("replacement_batch")
        sql("""
              |CALL sys.replace_by_keys(table => 'target',
              | source_query => 'SELECT * FROM replacement_batch',
              | key_columns => ARRAY('tenant','entity'))
              |""".stripMargin)
      } finally spark.catalog.dropTempView("replacement_batch")
      checkAnswer(
        sql("SELECT * FROM target"),
        Seq(
          Row(7, "a", "new-1", null),
          Row(7, "a", "new-2", null),
          Row(7, "c", null, null),
          Row(7, null, "new-null", null),
          Row(7, "b", "keep", null),
          Row(8, "a", "excluded", null),
          Row(7, null, "old-null", null)
        )
      )
    }
  }
}
