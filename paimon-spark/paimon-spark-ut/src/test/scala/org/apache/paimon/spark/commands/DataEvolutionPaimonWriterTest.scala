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
import org.apache.paimon.table.sink.CommitMessageImpl
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class DataEvolutionPaimonWriterTest extends PaimonSparkTestBase {

  test("partial writes preserve all range messages and row IDs across empty output") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT) TBLPROPERTIES (
            |  'bucket' = '-1',
            |  'row-tracking.enabled' = 'true',
            |  'data-evolution.enabled' = 'true',
            |  'write-only' = 'true')
            |""".stripMargin)
      sql(
        "INSERT INTO t SELECT /*+ COALESCE(1) */ * " +
          "FROM VALUES (1, 10), (2, CAST(NULL AS INT)) AS v(id, b)")
      sql(
        "INSERT INTO t SELECT /*+ COALESCE(1) */ * " +
          "FROM VALUES (3, 30), (4, 40) AS v(id, b)")

      checkAnswer(
        sql("SELECT id, b, _ROW_ID FROM t"),
        Seq(Row(1, 10, 0L), Row(2, null, 1L), Row(3, 30, 2L), Row(4, 40, 3L)))
      val table = loadTable("t")
      val snapshotId = table.latestSnapshot().get().id()
      val splits = table
        .newSnapshotReader()
        .read()
        .splits()
        .asScala
        .collect { case split: DataSplit => split }
        .toSeq
      assert(
        splits
          .flatMap(_.dataFiles().asScala)
          .map(f => (f.nonNullFirstRowId(), f.rowCount()))
          .sorted ==
          Seq((0L, 2L), (2L, 2L)))

      val data = sql("""
                       |SELECT b + 1 AS b, _ROW_ID,
                       |       CAST(2 * FLOOR(_ROW_ID / 2) AS BIGINT) AS _FIRST_ROW_ID
                       |FROM t
                       |""".stripMargin)
        .coalesce(1)
        .sortWithinPartitions("_FIRST_ROW_ID", "_ROW_ID")
      val writer = DataEvolutionPaimonWriter(table, splits)
      val emptyData =
        spark.createDataFrame(spark.sparkContext.parallelize(Seq.empty[Row], 2), data.schema)
      assert(writer.writePartialFields(emptyData, Seq("b")).isEmpty)
      val messages = writer.writePartialFields(data, Seq("b"))
      assert(messages.size == 2)
      val ranges = messages.flatMap {
        message =>
          message
            .asInstanceOf[CommitMessageImpl]
            .newFilesIncrement()
            .newFiles()
            .asScala
            .map(f => (f.nonNullFirstRowId(), f.rowCount()))
      }
      assert(ranges == Seq((0L, 2L), (2L, 2L)))
      assert(table.latestSnapshot().get().id() == snapshotId)

      PaimonSparkWriter(table).commit(messages, Operation.UPDATE)
      assert(table.latestSnapshot().get().id() == snapshotId + 1)
      checkAnswer(
        sql("SELECT id, b, _ROW_ID FROM t"),
        Seq(Row(1, 11, 0L), Row(2, null, 1L), Row(3, 31, 2L), Row(4, 41, 3L)))
    }
  }
}
