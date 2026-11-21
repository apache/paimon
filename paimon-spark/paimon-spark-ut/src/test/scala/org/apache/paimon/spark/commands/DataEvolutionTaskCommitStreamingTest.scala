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

import org.apache.paimon.Snapshot.Operation
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessageImpl
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.paimon.Utils

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class DataEvolutionTaskCommitStreamingTest extends PaimonSparkTestBase {

  private def withFixture(body: (FileStoreTable, Seq[DataSplit], Dataset[Row]) => Unit): Unit = {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT) TBLPROPERTIES (
            | 'bucket'='-1', 'row-tracking.enabled'='true',
            | 'data-evolution.enabled'='true', 'write-only'='true')
            |""".stripMargin)
      sql(
        "INSERT INTO t SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 10), (2, CAST(NULL AS INT)) AS v(id, b)")
      sql("INSERT INTO t SELECT /*+ COALESCE(1) */ * FROM VALUES (3, 30), (4, 40) AS v(id, b)")
      val table = loadTable("t")
      val splits = table
        .newSnapshotReader()
        .read()
        .splits()
        .asScala
        .collect { case split: DataSplit => split }
        .toSeq
      assert(splits.iterator.flatMap(_.dataFiles().asScala.iterator).size == 2)
      val data =
        sql("""
              |SELECT b + 1 AS b, _ROW_ID,
              | CAST(2 * FLOOR(_ROW_ID / 2) AS BIGINT) AS _FIRST_ROW_ID
              |FROM t
              |""".stripMargin).coalesce(1).sortWithinPartitions("_FIRST_ROW_ID", "_ROW_ID")
      body(table, splits, data)
    }
  }

  test("streaming Task needs no commit replay file after ordinary input planning") {
    withFixture {
      (table, splits, data) =>
        val snapshot = table.latestSnapshot().get().id()
        val streaming = table.copy(
          Map(
            "data-evolution.metadata-memory-optimization.mode" -> "streaming",
            "data-evolution.metadata-memory-optimization.max-disk-size" -> "8 b").asJava)
        val writer = DataEvolutionPaimonWriter(streaming, splits)
        val empty =
          spark.createDataFrame(spark.sparkContext.parallelize(Seq.empty[Row], 2), data.schema)
        assert(writer.writePartialFields(empty, Seq("b")).isEmpty)
        assert(writer.writePartialFields(data, Seq("b")).size == 2)
        Utils.waitUntilEventEmpty(spark)
        assert(table.latestSnapshot().get().id() == snapshot)
        checkAnswer(
          sql("SELECT id, b, _ROW_ID FROM t"),
          Seq(Row(1, 10, 0L), Row(2, null, 1L), Row(3, 30, 2L), Row(4, 40, 3L)))
    }
  }

  test("streaming Task returns original commit messages before atomic publication") {
    withFixture {
      (table, splits, data) =>
        val snapshot = table.latestSnapshot().get().id()
        val streaming =
          table.copy(Map("data-evolution.metadata-memory-optimization.mode" -> "streaming").asJava)
        val messages =
          DataEvolutionPaimonWriter(streaming, splits).writePartialFields(data, Seq("b"))
        assert(messages.size == 2)
        Utils.waitUntilEventEmpty(spark)
        assert(table.latestSnapshot().get().id() == snapshot)
        PaimonSparkWriter(streaming).commit(messages, Operation.UPDATE)
        assert(table.latestSnapshot().get().id() == snapshot + 1)
        checkAnswer(
          sql("SELECT id, b, _ROW_ID FROM t"),
          Seq(Row(1, 11, 0L), Row(2, null, 1L), Row(3, 31, 2L), Row(4, 41, 3L)))
    }
  }

  test("streaming Task preserves output metrics including filler rows") {
    withFixture {
      (table, splits, data) =>
        val snapshot = table.latestSnapshot().get().id()
        val streaming =
          table.copy(Map("data-evolution.metadata-memory-optimization.mode" -> "streaming").asJava)
        val outputs = new ConcurrentLinkedQueue[(Long, Long)]()
        val listener = new SparkListener {
          override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
            if (event.taskMetrics != null) {
              val metrics = event.taskMetrics.outputMetrics
              if (metrics.recordsWritten > 0) {
                outputs.add((metrics.recordsWritten, metrics.bytesWritten))
              }
            }
          }
        }
        Utils.waitUntilEventEmpty(spark)
        spark.sparkContext.addSparkListener(listener)
        try {
          val messages = DataEvolutionPaimonWriter(streaming, splits)
            .writePartialFields(data.filter("_ROW_ID % 2 = 0"), Seq("b"))
          val files = messages.iterator.flatMap {
            message =>
              message
                .asInstanceOf[CommitMessageImpl]
                .newFilesIncrement()
                .newFiles()
                .asScala
                .iterator
          }.toVector
          val rows = files.map(_.rowCount()).sum
          val bytes = files.map(_.fileSize()).sum
          assert(rows == 4L)
          Utils.waitUntilEventEmpty(spark)
          assert(outputs.asScala.toVector == Vector((rows, bytes)))
          assert(table.latestSnapshot().get().id() == snapshot)
        } finally spark.sparkContext.removeSparkListener(listener)
    }
  }

}
