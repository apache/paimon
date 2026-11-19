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

package org.apache.paimon.spark.commands

import org.apache.paimon.Snapshot
import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.errors.ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import java.util.Collections

import scala.collection.JavaConverters._

class ReplaceByKeysConflictTest extends PaimonSparkTestBase {

  private def createTarget(): Unit = {
    sql("""
          |CREATE TABLE target (id INT, value STRING)
          |TBLPROPERTIES ('data-evolution.enabled'='true','row-tracking.enabled'='true',
          | 'deletion-vectors.enabled'='true','write-only'='true')
          |""".stripMargin)
  }

  private def command(
      filter: Option[String] = None,
      sourceQuery: String = "SELECT 1 AS id, 'replacement' AS value"): ReplaceByKeysCommand = {
    val relation = spark
      .table("target")
      .queryExecution
      .analyzed
      .collect { case r: DataSourceV2Relation => r }
      .head
    ReplaceByKeysCommand(loadTable("target"), relation, sourceQuery, Seq("id"), filter)
  }

  private def commit(messages: Seq[CommitMessage], snapshotId: Long): Unit = {
    val checked: Seq[CommitMessage] =
      messages.map(_.asInstanceOf[CommitMessageImpl].withCheckFromSnapshot(snapshotId))
    val commit = loadTable("target").newBatchWriteBuilder().newCommit()
    try commit.withOperation(Snapshot.Operation.MERGE).commit(checked.asJava)
    finally commit.close()
  }

  test("an empty target baseline cannot delete a concurrent first append") {
    withTable("target") {
      createTarget()
      val baseline = loadTable("target").snapshotManager().latestSnapshot()
      assert(baseline == null)
      val replacement = command()
      sql("INSERT INTO target VALUES (1,'concurrent')")
      val sourceKeys = spark.sql("SELECT 1 AS id").alias("source_keys")
      assert(replacement.prepareDeletion(spark, baseline, sourceKeys, Seq("id")).isEmpty)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "concurrent")))
      intercept[Exception] {
        command(Some("missing_column = 1"))
          .prepareDeletion(spark, baseline, sourceKeys, Seq("id"))
      }
    }
  }

  test("replacement conflicts with a concurrent partial update of deleted rows") {
    withTable("target") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old')")
      val (messages, snapshotId) = command().prepare(spark)
      sql("UPDATE target SET value='concurrent' WHERE id=1")
      val afterUpdate = loadTable("target").snapshotManager().latestSnapshot().id()
      val failure = intercept[RuntimeException](commit(messages, snapshotId))
      val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
      assert(
        causes.exists(
          e => Option(e.getMessage).exists(_.contains(DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE))))
      assert(loadTable("target").snapshotManager().latestSnapshot().id() == afterUpdate)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "concurrent")))
    }
  }

  test("replacement permits concurrent updates in another anchor row range") {
    withTable("target") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old')")
      sql("INSERT INTO target VALUES (2,'other')")
      val (messages, snapshotId) = command(Some("id=1")).prepare(spark)
      sql("UPDATE target SET value='concurrent' WHERE id=2")
      commit(messages, snapshotId)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "replacement"), Row(2, "concurrent")))
    }
  }

  private def createPartitionedTarget(): Unit = {
    sql("""
          |CREATE TABLE target (id INT, value STRING, pt STRING) PARTITIONED BY (pt)
          |TBLPROPERTIES ('data-evolution.enabled'='true','row-tracking.enabled'='true',
          | 'deletion-vectors.enabled'='true','write-only'='true')
          |""".stripMargin)
    sql("INSERT INTO target VALUES (1,'old','a')")
    sql("INSERT INTO target VALUES (9,'other','b')")
    sql("INSERT INTO target VALUES (2,'third','a')")
  }

  Seq(true, false).foreach {
    reassignFirst =>
      test(s"replacement rejects anchor reassignment with reassignFirst=$reassignFirst") {
        withTable("target") {
          createPartitionedTarget()
          val (messages, snapshotId) =
            command(sourceQuery = "SELECT 1 AS id, 'replacement' AS value, 'a' AS pt")
              .prepare(spark)
          def reassign(): Unit = {
            val result = sql("CALL sys.reassign_row_id(table => 'target')").collect()
            assert(result.head.getString(0).startsWith("Success."))
          }
          if (reassignFirst) reassign()
          sql("UPDATE target SET id=8, value='concurrent' WHERE pt='a' AND id=1")
          if (!reassignFirst) reassign()
          val afterUpdate = loadTable("target").snapshotManager().latestSnapshot().id()
          val failure = intercept[RuntimeException](commit(messages, snapshotId))
          val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
          assert(
            causes.exists(
              e => Option(e.getMessage).exists(_.contains(DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE))))
          assert(loadTable("target").snapshotManager().latestSnapshot().id() == afterUpdate)
          checkAnswer(
            sql("SELECT * FROM target"),
            Seq(Row(8, "concurrent", "a"), Row(9, "other", "b"), Row(2, "third", "a")))
        }
      }
  }

  test("replacement permits reassignment and updates in an unrelated partition") {
    withTable("target") {
      createPartitionedTarget()
      val (messages, snapshotId) =
        command(sourceQuery = "SELECT 9 AS id, 'replacement' AS value, 'b' AS pt")
          .prepare(spark)
      val result =
        sql("CALL sys.reassign_row_id(table => 'target', partitions => 'pt=a')").collect()
      assert(result.head.getString(0).startsWith("Success."))
      sql("UPDATE target SET id=8, value='concurrent' WHERE pt='a' AND id=1")
      commit(messages, snapshotId)
      checkAnswer(
        sql("SELECT * FROM target"),
        Seq(Row(8, "concurrent", "a"), Row(9, "replacement", "b"), Row(2, "third", "a")))
    }
  }

  test("repacking an unchanged deletion vector does not conflict with a disjoint update") {
    withTable("target") {
      createTarget()
      sql("""INSERT INTO target SELECT /*+ COALESCE(1) */ * FROM
            |VALUES (1,'old'),(10,'delete-a') AS t(id,value)""".stripMargin)
      sql("""INSERT INTO target SELECT /*+ COALESCE(1) */ * FROM
            |VALUES (2,'other'),(20,'delete-b') AS t(id,value)""".stripMargin)
      sql("DELETE FROM target WHERE id IN (10,20)")
      val table = loadTable("target")
      val index = table
        .store()
        .newIndexFileHandler()
        .readManifest(table.snapshotManager().latestSnapshot().indexManifest())
      assert(
        index.asScala.exists(
          e => e.indexFile().dvRanges() != null && e.indexFile().dvRanges().size() == 2))
      val (messages, snapshotId) = command(Some("id=1")).prepare(spark)
      // SQL UPDATE can rewrite both anchor ranges to preserve gaps left by existing DVs.
      // Write only the second anchor's column file to isolate unchanged-index relocation.
      val write = table
        .newBatchWriteBuilder()
        .newWrite()
        .withWriteType(table.rowType().project(Collections.singletonList("value")))
      val updates =
        try {
          write.write(GenericRow.of(BinaryString.fromString("concurrent")))
          write.write(GenericRow.of(BinaryString.fromString("deleted")))
          write.prepareCommit()
        } finally write.close()
      updates.asScala.foreach {
        message =>
          val files = message.asInstanceOf[CommitMessageImpl].newFilesIncrement().newFiles()
          assert(files.size() == 1)
          files.replaceAll(file => file.assignFirstRowId(2L))
      }
      commit(updates.asScala.toSeq, snapshotId)
      commit(messages, snapshotId)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "replacement"), Row(2, "concurrent")))
    }
  }
}
