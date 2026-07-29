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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.{BinaryRow, BinaryString, GenericRow}
import org.apache.paimon.fs.{FileStatus, Path}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.schema.{Schema, SchemaManager, TableSchema}
import org.apache.paimon.spark.write.DataEvolutionTableDataWrite
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessage, CommitMessageImpl}
import org.apache.paimon.types.DataTypes
import org.apache.paimon.utils.{SerializationUtils, UriReaderFactory}

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{AfterAll, Assertions, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.io.TempDir

import java.util.{ArrayList => JArrayList, Arrays => JArrays, Collections, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Regression tests for {@link DataEvolutionTableDataWrite} speculative-execution cleanup. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataEvolutionTableDataWriteTest {

  @BeforeAll
  def startSpark(): Unit = {
    SparkSession.builder
      .master("local[1]")
      .appName("DataEvolutionTableDataWriteTest")
      .getOrCreate()
  }

  @AfterAll
  def stopSpark(): Unit = {
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  @TempDir
  private var tempDir: java.nio.file.Path = null

  private val fileIO = LocalFileIO.create()

  private def buildTable(): FileStoreTable = {
    val tablePath = new Path(tempDir.toString)
    val schema = Schema.newBuilder
      .column("f0", DataTypes.INT())
      .column("f1", DataTypes.STRING())
      .column("f2", DataTypes.STRING())
      .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
      .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
      .option("file.format", "parquet")
      .option("manifest.format", "avro")
      .option("metadata.stats-mode", "none")
      .build()
    val schemaManager = new SchemaManager(fileIO, tablePath)
    val tableSchema: TableSchema = schemaManager.createTable(schema)
    FileStoreTableFactory.create(fileIO, tablePath, tableSchema)
  }

  private def setFirstRowId(messages: JList[CommitMessage], firstRowId: Long): Unit = {
    messages.asScala.foreach {
      c =>
        val commitMessage = c.asInstanceOf[CommitMessageImpl]
        val newFiles = new JArrayList(commitMessage.newFilesIncrement().newFiles())
        commitMessage.newFilesIncrement().newFiles().clear()
        newFiles.asScala.foreach {
          file =>
            commitMessage.newFilesIncrement().newFiles().add(file.assignFirstRowId(firstRowId))
        }
    }
  }

  /** Two committed data files with firstRowId 0 and 10, one row each. */
  private def seedTwoFileTable(table: FileStoreTable): Unit = {
    val builder = table.newBatchWriteBuilder()
    val writeType01 = table.rowType().project(JArrays.asList("f0", "f1"))

    val write: BatchTableWrite = builder.newWrite().withWriteType(table.rowType())
    try {
      write.write(
        GenericRow.of(Int.box(1), BinaryString.fromString("a"), BinaryString.fromString("b")))
      val commit1 = builder.newCommit()
      try {
        commit1.commit(write.prepareCommit())
      } finally {
        commit1.close()
      }
    } finally {
      write.close()
    }

    val write01: BatchTableWrite = builder.newWrite().withWriteType(writeType01)
    try {
      write01.write(GenericRow.of(Int.box(2), BinaryString.fromString("c")))
      val messages = write01.prepareCommit()
      setFirstRowId(messages, 10L)
      val commit2 = builder.newCommit()
      try {
        commit2.commit(messages)
      } finally {
        commit2.close()
      }
    } finally {
      write01.close()
    }
  }

  private def buildPartitionMap(
      table: FileStoreTable): mutable.HashMap[Long, (Array[Byte], Long)] = {
    val map = new mutable.HashMap[Long, (Array[Byte], Long)]
    table
      .store()
      .newScan()
      .readFileIterator()
      .forEachRemaining {
        entry =>
          map.put(
            entry.file().firstRowId(),
            (SerializationUtils.serializeBinaryRow(entry.partition()), entry.file().rowCount()))
      }
    map
  }

  private def countParquetFiles(table: FileStoreTable): Int = {
    val collected = new JArrayList[FileStatus]()
    listRecursive(fileIO, table.location(), collected)
    collected.asScala.count(_.getPath.getName.endsWith(".parquet"))
  }

  private def listRecursive(
      io: org.apache.paimon.fs.FileIO,
      path: Path,
      out: JList[FileStatus]): Unit = {
    try {
      io.listStatus(path).foreach {
        status =>
          if (status.isDir) listRecursive(io, status.getPath, out)
          else out.add(status)
      }
    } catch {
      case _: java.io.FileNotFoundException =>
    }
  }

  private def evolutionRow(f2: String, rowId: Long, firstRowId: Long): Row = {
    Row(f2, rowId, firstRowId)
  }

  @Test
  def closeAfterFinishCurrentWriterAbortsPreparedPerFileMessages(): Unit = {
    val table = buildTable()
    seedTwoFileTable(table)
    val baseline = countParquetFiles(table)
    Assertions.assertEquals(2, baseline)

    val writeBuilder: BatchWriteBuilder = table.newBatchWriteBuilder()
    val writeType = table.rowType().project(Collections.singletonList("f2"))
    val partitionMap = buildPartitionMap(table)
    val firstRowIds = partitionMap.keys.toSeq.sorted
    Assertions.assertEquals(2, firstRowIds.size)

    val uriReaderFactory = UriReaderFactory.fromFileIO(LocalFileIO.create())
    val writer =
      DataEvolutionTableDataWrite(
        writeBuilder,
        writeType,
        partitionMap,
        uriReaderFactory,
        Map.empty)
    try {
      writer.write(evolutionRow("new-f2-0", firstRowIds(0), firstRowIds(0)))
      // Switching firstRowId finishes the first PerFileWriter (prepare + addPrepared).
      writer.write(evolutionRow("new-f2-1", firstRowIds(1), firstRowIds(1)))
      writer.close()
    } finally {
      writer.close()
    }

    Assertions.assertEquals(
      baseline,
      countParquetFiles(table),
      "close without return must abort incrementally prepared per-file messages")
  }
}
