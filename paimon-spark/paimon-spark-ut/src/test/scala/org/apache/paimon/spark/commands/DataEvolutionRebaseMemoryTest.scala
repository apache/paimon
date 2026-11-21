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

import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

class DataEvolutionRebaseMemoryTest extends PaimonSparkTestBase {

  Seq("off", "lightweight").foreach {
    mode =>
      test(s"no-op rebase consumes messages incrementally when metadata memory mode is $mode") {
        withTable("t") {
          sql(s"""
                 |CREATE TABLE t (id INT, value INT) TBLPROPERTIES (
                 | 'bucket'='-1', 'row-tracking.enabled'='true',
                 | 'data-evolution.enabled'='true', 'write-only'='true',
                 | 'data-evolution.metadata-memory-optimization.mode'='$mode')
                 |""".stripMargin)
          sql("INSERT INTO t VALUES (1, 10)")
          sql("INSERT INTO t VALUES (2, 20)")
          val table = loadTable("t")
          val snapshot = table.latestSnapshot().get()
          val relation = sql("SELECT * FROM t").queryExecution.analyzed.collectFirst {
            case r: DataSourceV2Relation => r
          }.get
          val splits = table.newSnapshotReader().read().splits().asScala.collect {
            case split: DataSplit => split
          }
          val files = splits.flatMap(split => split.dataFiles().asScala.map(file => (split, file)))
          assert(files.size == 2)
          val firstConsumed = new AtomicBoolean(false)
          val messages = files.zipWithIndex.map {
            case ((split, file), index) =>
              new CommitMessageImpl(
                split.partition(),
                split.bucket(),
                split.totalBuckets(),
                new DataIncrement(
                  Collections.singletonList(file),
                  Collections.emptyList(),
                  Collections.emptyList()),
                CompactIncrement.emptyIncrement()
              ) {
                override def newFilesIncrement(): DataIncrement = {
                  if (index == 0) firstConsumed.set(true)
                  super.newFilesIncrement()
                }
              }
          }
          // A byte-backed producer can release the previous decoded message only after consumption.
          // This deterministic demand check catches collect-before-processing without GC heuristics.
          val source = new scala.collection.immutable.IndexedSeq[CommitMessage] {
            override def length: Int = messages.size
            override def apply(index: Int): CommitMessage = {
              if (mode != "off" && index > 0) {
                assert(firstConsumed.get(), "Decoded the next message before consuming the first")
              }
              messages(index)
            }
          }
          val result = new DataEvolutionRowIdConflictRewriter(table, relation)
            .rewrite(spark, snapshot, source)
          assert(result.isEmpty)
          assert(firstConsumed.get())
          assert(table.latestSnapshot().get().id() == snapshot.id())
          checkAnswer(sql("SELECT id, value FROM t"), sql("SELECT * FROM VALUES (1, 10), (2, 20)"))
        }
      }
  }
}
