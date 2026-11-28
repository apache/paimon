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
import org.apache.paimon.spark.write.DataEvolutionTableDataWrite
import org.apache.paimon.table.{BlobDescriptorReaderFactory, FileStoreTable}
import org.apache.paimon.table.sink.{CommitMessageImpl, CommitMessageSerializer}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

import java.io.IOException
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

class DataEvolutionTaskCommitDemandTest extends PaimonSparkTestBase {

  private def withWriter(
      body: (FileStoreTable, DataEvolutionTableDataWrite, Array[Row]) => Unit): Unit = {
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
      val ranges = mutable.HashMap.empty[Long, (Array[Byte], Long)]
      table.newSnapshotReader().read().splits().asScala.foreach {
        case split: DataSplit =>
          split.dataFiles().asScala.foreach {
            file =>
              ranges.put(
                file.firstRowId(),
                (SerializationUtils.serializeBinaryRow(split.partition()), file.rowCount()))
          }
      }
      assert(ranges.size == 2)
      val rows = sql("""
                       |SELECT b + 1 AS b, _ROW_ID,
                       | CAST(2 * FLOOR(_ROW_ID / 2) AS BIGINT) AS _FIRST_ROW_ID
                       |FROM t ORDER BY _ROW_ID
                       |""".stripMargin).collect()
      val writer = DataEvolutionTableDataWrite(
        table.newBatchWriteBuilder(),
        table.rowType().project(Collections.singletonList("b")),
        ranges,
        BlobDescriptorReaderFactory.create(table),
        Map.empty)
      try body(table, writer, rows)
      finally writer.close()
    }
  }

  private def decode(bytes: Array[Byte]): CommitMessageImpl = {
    val serializer = new CommitMessageSerializer()
    serializer.deserialize(serializer.getVersion, bytes).asInstanceOf[CommitMessageImpl]
  }

  test("produces each file message on demand without draining or retaining reusable input rows") {
    withWriter {
      (table, writer, rows) =>
        var consumed = 0
        val values = new Array[Any](3)
        val reused = new GenericRow(values)
        val input = rows.iterator.map {
          row =>
            consumed += 1
            values.indices.foreach(index => values(index) = row.get(index))
            reused
        }
        val snapshot = table.latestSnapshot().get().id()
        val output = writer.writeAndCommitRows(input)
        assert(consumed == 0)
        assert(output.hasNext)
        assert(consumed == 3)
        assert(output.hasNext)
        assert(consumed == 3)
        val first = decode(output.next())
        assert(first.newFilesIncrement().newFiles().get(0).firstRowId() == 0L)
        assert(consumed == 3)
        val second = decode(output.next())
        assert(consumed == 4)
        assert(second.newFilesIncrement().newFiles().get(0).firstRowId() == 2L)
        assert(!output.hasNext)
        assert(!output.hasNext)
        intercept[NoSuchElementException](output.next())
        assert(table.latestSnapshot().get().id() == snapshot)
        PaimonSparkWriter(table).commit(Seq(first, second), Operation.UPDATE)
        assert(table.latestSnapshot().get().id() == snapshot + 1)
        checkAnswer(
          sql("SELECT id, b, _ROW_ID FROM t"),
          Seq(Row(1, 11, 0L), Row(2, null, 1L), Row(3, 31, 2L), Row(4, 41, 3L)))
    }
  }

  test("accepts contiguous file ranges in either order") {
    withWriter {
      (table, writer, rows) =>
        val snapshot = table.latestSnapshot().get().id()
        val output = writer.writeAndCommitRows((rows.drop(2) ++ rows.take(2)).iterator)
        val starts =
          output.map(decode).map(_.newFilesIncrement().newFiles().get(0).firstRowId()).toVector
        assert(starts == Vector(2L, 0L))
        assert(table.latestSnapshot().get().id() == snapshot)
    }
  }

  test("early close stops input consumption and never publishes a partial result") {
    withWriter {
      (table, writer, rows) =>
        var consumed = 0
        val snapshot = table.latestSnapshot().get().id()
        val output = writer.writeAndCommitRows(rows.iterator.map { row => consumed += 1; row })
        decode(output.next())
        assert(consumed == 3)
        writer.close()
        writer.close()
        assert(!output.hasNext)
        assert(consumed == 3)
        intercept[NoSuchElementException](output.next())
        assert(table.latestSnapshot().get().id() == snapshot)
        checkAnswer(
          sql("SELECT id, b, _ROW_ID FROM t"),
          Seq(Row(1, 10, 0L), Row(2, null, 1L), Row(3, 30, 2L), Row(4, 40, 3L)))
    }
  }

  test("input failure after a completed file preserves the failure and stops output") {
    withWriter {
      (table, writer, rows) =>
        var consumed = 0
        val failure = new IOException("input failed after first file")
        val snapshot = table.latestSnapshot().get().id()
        val output = writer.writeAndCommitRows(rows.iterator.map {
          row =>
            consumed += 1
            if (consumed == 4) throw failure
            row
        })
        decode(output.next())
        assert(consumed == 3)
        assert(intercept[IOException](output.hasNext) eq failure)
        assert(!output.hasNext)
        writer.close()
        assert(table.latestSnapshot().get().id() == snapshot)
    }
  }

  test("empty input completes without producing a message or a snapshot") {
    withWriter {
      (table, writer, _) =>
        val snapshot = table.latestSnapshot().get().id()
        val output = writer.writeAndCommitRows(Iterator.empty)
        assert(!output.hasNext)
        assert(!output.hasNext)
        intercept[NoSuchElementException](output.next())
        writer.close()
        assert(table.latestSnapshot().get().id() == snapshot)
    }
  }
}
