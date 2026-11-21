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
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType}
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class DataEvolutionMetadataMemoryOptionTest extends PaimonSparkTestBase {

  Seq(None, Some("off"), Some("lightweight"), Some("streaming")).foreach {
    mode =>
      val label = mode.getOrElse("default")
      test(s"metadata memory option $label preserves partial writes and result encoding") {
        withTable("t") {
          val option = mode
            .map(value => s", 'data-evolution.metadata-memory-optimization.mode'='$value'")
            .getOrElse("")
          sql(s"""
                 |CREATE TABLE t (id INT, b INT) TBLPROPERTIES (
                 | 'bucket'='-1', 'row-tracking.enabled'='true',
                 | 'data-evolution.enabled'='true', 'write-only'='true'$option)
                 |""".stripMargin)
          sql(
            "INSERT INTO t SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 10), " +
              "(2, CAST(NULL AS INT)) AS v(id, b)")
          sql("INSERT INTO t SELECT /*+ COALESCE(1) */ * FROM VALUES (3, 30), (4, 40) AS v(id, b)")
          val table = loadTable("t")
          val snapshot = table.latestSnapshot().get().id()
          val splits = table
            .newSnapshotReader()
            .read()
            .splits()
            .asScala
            .collect { case split: DataSplit => split }
            .toSeq
          val data = sql(
            "SELECT b + 1 AS b, _ROW_ID, " +
              "CAST(2 * FLOOR(_ROW_ID / 2) AS BIGINT) AS _FIRST_ROW_ID FROM t")
            .coalesce(1)
            .sortWithinPartitions("_FIRST_ROW_ID", "_ROW_ID")
          val empty =
            spark.createDataFrame(spark.sparkContext.parallelize(Seq.empty[Row], 2), data.schema)
          val encodings = new ConcurrentLinkedQueue[DataType]()
          val listener = new QueryExecutionListener {
            override def onSuccess(name: String, qe: QueryExecution, nanos: Long): Unit = {
              if (qe.executedPlan.toString.contains("DataEvolutionPaimonWriter")) {
                assert(qe.analyzed.output.size == 1)
                encodings.add(qe.analyzed.output.head.dataType)
              }
            }
            override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {}
          }
          spark.listenerManager.register(listener)
          try {
            val writer = DataEvolutionPaimonWriter(table, splits)
            assert(writer.writePartialFields(empty, Seq("b")).isEmpty)
            val messages = writer.writePartialFields(data, Seq("b"))
            Utils.waitUntilEventEmpty(spark)
            val actual = encodings.asScala.toSeq
            assert(actual.size == 2, s"Expected both real writer calls, observed: $actual")
            if (mode.exists(_ != "off")) {
              assert(actual.forall(_ == BinaryType), actual)
            } else {
              assert(
                actual.forall {
                  case ArrayType(BinaryType, _) => true
                  case _ => false
                },
                actual)
            }
            assert(messages.size == 2)
            val ranges = messages.flatMap {
              message =>
                message
                  .asInstanceOf[CommitMessageImpl]
                  .newFilesIncrement()
                  .newFiles()
                  .asScala
                  .map(file => (file.nonNullFirstRowId(), file.rowCount()))
            }
            assert(ranges == Seq((0L, 2L), (2L, 2L)))
            assert(table.latestSnapshot().get().id() == snapshot)
            PaimonSparkWriter(table).commit(messages, Operation.UPDATE)
            assert(table.latestSnapshot().get().id() == snapshot + 1)
            checkAnswer(
              sql("SELECT id, b, _ROW_ID FROM t"),
              Seq(Row(1, 11, 0L), Row(2, null, 1L), Row(3, 31, 2L), Row(4, 41, 3L)))
          } finally {
            spark.listenerManager.unregister(listener)
          }
        }
      }
  }
}
