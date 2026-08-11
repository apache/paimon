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

package org.apache.paimon.spark.write

import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.disk.IOManagerImpl
import org.apache.paimon.fs.{FileStatus, Path}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.schema.{Schema, SchemaManager, TableSchema}
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}
import org.apache.paimon.table.sink.{BatchTableCommit, BatchTableWrite, BatchWriteBuilder, TableWriteImpl, WriteSelector}
import org.apache.paimon.types.{IntType, RowType, VarCharType}

import org.junit.jupiter.api.{Assertions, Test}
import org.junit.jupiter.api.io.TempDir

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

import scala.collection.JavaConverters._

class SparkAttemptCleanupTest {

  @TempDir
  private var tempDir: java.nio.file.Path = null

  private val fileIO = LocalFileIO.create()

  private def buildTable(): FileStoreTable = {
    val tablePath = new Path(tempDir.toString)
    val schema = Schema.newBuilder
      .column("id", new IntType())
      .column("v", new VarCharType())
      .option("file.format", "parquet")
      .option("manifest.format", "avro")
      .option("metadata.stats-mode", "none")
      .build()
    val schemaManager = new SchemaManager(fileIO, tablePath)
    val tableSchema: TableSchema = schemaManager.createTable(schema)
    FileStoreTableFactory.create(fileIO, tablePath, tableSchema)
  }

  private def countParquetFiles(table: FileStoreTable): Int = {
    val collected = new util.ArrayList[FileStatus]()
    listRecursive(fileIO, table.location(), collected)
    collected.asScala.count(_.getPath.getName.endsWith(".parquet"))
  }

  private def listRecursive(
      io: org.apache.paimon.fs.FileIO,
      path: Path,
      out: util.List[FileStatus]): Unit = {
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

  @Test
  def abortPreparedAlsoClosesUnpreparedWriter(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val closeCount = new AtomicInteger()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => closeCount.incrementAndGet())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      // Simulate multi-file writer: prepared messages exist AND an unprepared close callback.
      cleanup.abortPrepared()
    } finally {
      write.close()
      cleanup.close()
    }

    Assertions.assertEquals(
      2,
      closeCount.get(),
      "abort must close unprepared writer even when prepared messages are present; close() also closes")
    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def abortPreparedDeletesUnpublishedFiles(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val closeCount = new AtomicInteger()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => closeCount.incrementAndGet())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      cleanup.abortPrepared()
    } finally {
      write.close()
      cleanup.close()
    }

    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def abortPreparedUnderThreadInterruptStillDeletesFiles(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      Thread.currentThread().interrupt()
      try {
        cleanup.abortPrepared()
        Assertions.assertTrue(Thread.currentThread().isInterrupted)
      } finally {
        Thread.interrupted()
      }
    } finally {
      write.close()
      cleanup.close()
    }

    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def markReturnedDoesNotAbortPreparedFilesOnClose(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val closeCount = new AtomicInteger()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => closeCount.incrementAndGet())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      cleanup.markReturned()
    } finally {
      write.close()
      cleanup.close()
    }

    Assertions.assertEquals(1, closeCount.get())
    Assertions.assertEquals(1, countParquetFiles(table))
  }

  @Test
  def abortAfterMarkReturnedAndCloseReclaimsPreparedFiles(): Unit = {
    // P1: commit() marks Returned and close() reaches Closed before Spark accepts the
    // DataWritingSparkTaskResult. A subsequent DataWriter.abort() must still delete files.
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      cleanup.markReturned()
      cleanup.close()
      Assertions.assertEquals(1, countParquetFiles(table))

      cleanup.abortPrepared()
      Assertions.assertEquals(
        0,
        countParquetFiles(table),
        "Spark DataWriter.abort after markReturned+close must reclaim prepared files")
    } finally {
      write.close()
    }
  }

  @Test
  def abortAfterMarkReturnedAndCloseUnderInterruptReclaimsPreparedFiles(): Unit = {
    // P1: speculative kill after markReturned/close sets the interrupt flag; abort must
    // still reclaim (completion listener and DataWriter.abort share abortIfNeeded).
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      cleanup.markReturned()
      cleanup.close()
      Assertions.assertEquals(1, countParquetFiles(table))

      Thread.currentThread().interrupt()
      try {
        cleanup.abortPrepared()
        Assertions.assertTrue(Thread.currentThread().isInterrupted)
      } finally {
        Thread.interrupted()
      }
      Assertions.assertEquals(0, countParquetFiles(table))
    } finally {
      write.close()
    }
  }

  @Test
  def closeWithoutReturnAbortsPreparedFiles(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
    } finally {
      write.close()
      cleanup.close()
    }

    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def prepareCommitWithoutAddPreparedLeavesOrphanAfterClose(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      write.prepareCommit()
    } finally {
      // PerFileWriter.finish() used to close here before addPrepared; prepareCommit drains
      // newFiles so close() no longer deletes them.
      write.close()
    }

    Assertions.assertEquals(1, countParquetFiles(table))
    cleanup.abortPrepared()
    Assertions.assertEquals(
      1,
      countParquetFiles(table),
      "abort without addPrepared cannot reclaim drained files")
  }

  @Test
  def prepareCommitWithAddPreparedReclaimsAfterClose(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write = writeBuilder.newWrite().asInstanceOf[TableWriteImpl[_]]
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      write.prepareCommit(new Consumer[org.apache.paimon.table.sink.CommitMessage] {
        override def accept(message: org.apache.paimon.table.sink.CommitMessage): Unit =
          cleanup.addPreparedJava(java.util.Collections.singletonList(message))
      })
    } finally {
      write.close()
    }

    Assertions.assertEquals(1, countParquetFiles(table))
    cleanup.abortPrepared()
    cleanup.close()
    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def incrementalSetPreparedAbortsAllPreparedFiles(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val prepared = new util.ArrayList[org.apache.paimon.table.sink.CommitMessage]()
    val write1: BatchTableWrite = writeBuilder.newWrite()
    write1.withIOManager(new IOManagerImpl(tempDir.resolve("io1").toString))
    try {
      write1.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      prepared.addAll(write1.prepareCommit())
      cleanup.setPreparedJava(prepared)
    } finally {
      write1.close()
    }

    val write2: BatchTableWrite = writeBuilder.newWrite()
    write2.withIOManager(new IOManagerImpl(tempDir.resolve("io2").toString))
    try {
      write2.write(GenericRow.of(Int.box(2), BinaryString.fromString("b")))
      prepared.addAll(write2.prepareCommit())
      cleanup.setPreparedJava(prepared)
    } finally {
      write2.close()
    }

    Assertions.assertTrue(countParquetFiles(table) >= 2)
    cleanup.abortPrepared()
    cleanup.close()
    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def closeFailureOnReturnedPathAbortsPreparedFilesImmediately(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val closeCount = new AtomicInteger()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => {
        closeCount.incrementAndGet()
        throw new RuntimeException("injected writer close failure")
      })

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      cleanup.markReturned()

      val thrown =
        Assertions.assertThrows(classOf[RuntimeException], () => cleanup.close())
      Assertions.assertTrue(thrown.getMessage.contains("injected writer close failure"))
      Assertions.assertTrue(
        closeCount.get() >= 1,
        "Returned path must invoke closeUnprepared (and abort may also close)")
      // V1 / SparkAttemptWrite only close once — abort must run on the failing close itself.
      Assertions.assertEquals(0, countParquetFiles(table))
    } finally {
      write.close()
    }
  }

  @Test
  def newCommitFailureStillClosesUnpreparedWriter(): Unit = {
    val table = buildTable()
    val realWriteBuilder = table.newBatchWriteBuilder()
    val closeCount = new AtomicInteger()
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      realWriteBuilder.commitUser(),
      newCommitFailingBuilder(realWriteBuilder),
      () => closeCount.incrementAndGet())

    val write: BatchTableWrite = realWriteBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    val messages =
      try {
        write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
        write.prepareCommit()
      } finally {
        write.close()
      }

    cleanup.setPreparedJava(messages)
    cleanup.abortPrepared()
    Assertions.assertEquals(
      1,
      closeCount.get(),
      "writer close must run even when constructing the abort commit fails")

    // The injected builder could not delete the prepared file, so clean it with the real builder.
    val commit = realWriteBuilder.newCommit()
    try commit.abort(messages)
    finally commit.close()
  }

  @Test
  def closeOnAbortedPathSuppressesCloseFailure(): Unit = {
    // Abort/cleanup path: close failures are suppressed because the task is already failing.
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val closeCount = new AtomicInteger()

    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => {
        closeCount.incrementAndGet()
        throw new RuntimeException("injected writer close failure")
      })

    // abortPrepared transitions Writing -> Aborted and suppresses the close failure.
    cleanup.abortPrepared()
    // close() on Aborted must also suppress, never propagate.
    cleanup.close()
    Assertions.assertTrue(closeCount.get() >= 1, "abort path must still attempt close")
  }

  @Test
  def markCommittedAfterPublishPreventsAbortOfCommittedFiles(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    val messages =
      try {
        write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
        write.prepareCommit()
      } finally {
        write.close()
      }

    cleanup.setPreparedJava(messages)
    val commit = writeBuilder.newCommit()
    try {
      commit.commit(messages)
      cleanup.markCommitted()
    } finally {
      commit.close()
    }

    // Simulate close/interrupt/abort after successful snapshot publish: must not delete
    // snapshot files (prepared messages were cleared by markCommitted).
    Thread.currentThread().interrupt()
    try {
      cleanup.abortPrepared()
      cleanup.close()
    } finally {
      Thread.interrupted()
    }
    Assertions.assertEquals(1, countParquetFiles(table))
  }

  @Test
  def setPreparedIgnoredAfterAbort(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      writeBuilder.commitUser(),
      writeBuilder,
      () => ())

    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io").toString))
    val messages =
      try {
        write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
        write.prepareCommit()
      } finally {
        write.close()
      }

    cleanup.setPreparedJava(messages)
    cleanup.abortPrepared()
    Assertions.assertEquals(0, countParquetFiles(table))

    // Recreate files and try to re-register after abort: state must stay Aborted.
    val write2: BatchTableWrite = writeBuilder.newWrite()
    write2.withIOManager(new IOManagerImpl(tempDir.resolve("io2").toString))
    val messages2 =
      try {
        write2.write(GenericRow.of(Int.box(2), BinaryString.fromString("b")))
        write2.prepareCommit()
      } finally {
        write2.close()
      }
    Assertions.assertEquals(1, countParquetFiles(table))
    cleanup.setPreparedJava(messages2)
    cleanup.close()
    // Ignored setPrepared must not resurrect Prepared, so close does not abort messages2.
    // Caller owns messages2 cleanup here.
    Assertions.assertEquals(1, countParquetFiles(table))
    val abortCommit = writeBuilder.newCommit()
    try {
      abortCommit.abort(messages2)
    } finally {
      abortCommit.close()
    }
    Assertions.assertEquals(0, countParquetFiles(table))
  }

  @Test
  def commitUserOrUnknownFallsBackForBuildersWithoutCommitUser(): Unit = {
    val table = buildTable()
    val writeBuilder = table.newBatchWriteBuilder()
    Assertions.assertEquals(
      writeBuilder.commitUser(),
      SparkAttemptCleanup.commitUserOrUnknown(writeBuilder))

    // A custom BatchWriteBuilder that does not override commitUser(): the default interface
    // implementation throws, and the fallback must keep writer construction working.
    val builderWithoutCommitUser = new BatchWriteBuilder {
      override def tableName(): String = writeBuilder.tableName()

      override def rowType(): RowType = writeBuilder.rowType()

      override def newWriteSelector(): java.util.Optional[WriteSelector] =
        writeBuilder.newWriteSelector()

      override def newWrite(): BatchTableWrite = writeBuilder.newWrite()

      override def newCommit(): BatchTableCommit = writeBuilder.newCommit()

      override def withOverwrite(
          staticPartition: java.util.Map[String, String]): BatchWriteBuilder = {
        writeBuilder.withOverwrite(staticPartition)
        this
      }
    }
    Assertions.assertThrows(
      classOf[UnsupportedOperationException],
      () => builderWithoutCommitUser.commitUser())
    Assertions.assertEquals(
      "<unknown>",
      SparkAttemptCleanup.commitUserOrUnknown(builderWithoutCommitUser))

    // Construction with such a builder must succeed; abort still goes through newCommit().
    val cleanup = SparkAttemptCleanup.forJava(
      table.fullName(),
      SparkAttemptCleanup.commitUserOrUnknown(builderWithoutCommitUser),
      builderWithoutCommitUser,
      () => ())
    val write: BatchTableWrite = writeBuilder.newWrite()
    write.withIOManager(new IOManagerImpl(tempDir.resolve("io-unknown").toString))
    try {
      write.write(GenericRow.of(Int.box(1), BinaryString.fromString("a")))
      val messages = write.prepareCommit()
      cleanup.setPreparedJava(messages)
      cleanup.abortPrepared()
    } finally {
      write.close()
      cleanup.close()
    }
    Assertions.assertEquals(0, countParquetFiles(table))
  }

  private def newCommitFailingBuilder(delegate: BatchWriteBuilder): BatchWriteBuilder = {
    new BatchWriteBuilder {
      override def tableName(): String = delegate.tableName()

      override def rowType(): RowType = delegate.rowType()

      override def newWriteSelector(): java.util.Optional[WriteSelector] =
        delegate.newWriteSelector()

      override def newWrite(): BatchTableWrite = delegate.newWrite()

      override def newCommit(): BatchTableCommit =
        throw new RuntimeException("injected newCommit failure")

      override def withOverwrite(
          staticPartition: java.util.Map[String, String]): BatchWriteBuilder = {
        delegate.withOverwrite(staticPartition)
        this
      }

      override def commitUser(): String = delegate.commitUser()
    }
  }
}
