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

package org.apache.paimon.spark.format

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.{Path, PositionOutputStream}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.IOException

class FormatTablePartitionMarkDoneTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  // Unique table names: a dropped table's directory can be reused by a table of the same name.
  Seq(true, false).foreach {
    managed =>
      test(s"FormatTable mark done: writes mark their partitions (managed=$managed)") {
        val name = s"mark_done_write_$managed"
        withTable(name) {
          createTable(name, managed, "'partition.end-input-to-done'='true'")
          sql(s"INSERT INTO $name VALUES (1, '20240101', '08'), (2, '20240101', '09')")
          val table = formatTable(name)
          assert(successFileLength(table, "dt=20240101/hh=08") == 0)
          table.fileIO().overwriteFileUtf8(successPath(table, "dt=20240101/hh=09"), "stale")

          // Clears hh=08 without writing to it, and rewrites hh=09.
          sql(s"INSERT OVERWRITE $name PARTITION (dt = '20240101') SELECT 3, '09'")
          assert(!marked(table, "dt=20240101/hh=08"))
          assert(successFileLength(table, "dt=20240101/hh=09") == 0)

          sql(
            s"INSERT OVERWRITE $name PARTITION (dt = '20240102', hh = '00') SELECT id FROM $name WHERE id < 0")
          assert(marked(table, "dt=20240102/hh=00"))
          checkAnswer(sql(s"SELECT id, dt, hh FROM $name"), Seq(Row(3, "20240101", "09")))
        }
      }
  }

  test("FormatTable mark done: a failed mark keeps the committed data") {
    val name = "mark_done_failure"
    withTable(name) {
      createTable(name, managed = false, "'partition.end-input-to-done'='true'")
      val table = formatTable(name)
      // Only creating _SUCCESS fails, which happens after the data is committed.
      val fileIO = new LocalFileIO {
        override def newOutputStream(path: Path, overwrite: Boolean): PositionOutputStream = {
          if (path.getName == "_SUCCESS") {
            throw new IOException("Injected failure")
          }
          super.newOutputStream(path, overwrite)
        }
      }
      val failingTable = FormatTable
        .builder()
        .fileIO(fileIO)
        .identifier(Identifier.create("test_db", name))
        .rowType(table.rowType())
        .partitionKeys(table.partitionKeys())
        .location(table.location())
        .format(table.format())
        .options(table.options())
        .catalogContext(table.catalogContext())
        .build()
      val write = SparkShimLoader.shim.createFormatTableBatchWrite(
        failingTable,
        None,
        StructType.fromDDL("id INT, dt STRING, hh STRING"))
      val writer = write
        .createBatchWriterFactory(new PhysicalWriteInfo { override def numPartitions(): Int = 1 })
        .createWriter(0, 0)
      writer.write(InternalRow(1, UTF8String.fromString("20240101"), UTF8String.fromString("08")))
      val messages: Array[WriterCommitMessage] = Array(writer.commit())

      val error = intercept[RuntimeException](write.commit(messages))
      assert(error.getMessage.contains("failed to mark its partitions done"))
      // Spark aborts a write whose commit throws.
      write.abort(messages)
      checkAnswer(sql(s"SELECT id, dt, hh FROM $name"), Row(1, "20240101", "08") :: Nil)
      assert(!marked(table, "dt=20240101/hh=08"))
    }
  }

  test("FormatTable mark done: the procedure marks partitions") {
    val name = "mark_done_procedure"
    withTable(name) {
      createTable(name, managed = true, "'partition.mark-done-action'='success-file'")
      sql(s"INSERT INTO $name VALUES (1, '20240101', '08'), (2, '20240101', '09')")
      val table = formatTable(name)
      assert(!marked(table, "dt=20240101/hh=08"))

      checkAnswer(
        sql(
          "CALL paimon.sys.mark_partition_done(" +
            s"table => 'test_db.$name', partitions => 'dt=20240101,hh=08')"),
        Row(true) :: Nil)
      assert(marked(table, "dt=20240101/hh=08"))
      assert(!marked(table, "dt=20240101/hh=09"))
    }
  }

  test("FormatTable mark done: an unsupported action fails before writing") {
    val name = "mark_done_unsupported"
    withTable(name) {
      createTable(
        name,
        managed = false,
        "'partition.end-input-to-done'='true', 'partition.mark-done-action'='mark-event'")
      val expected = "only supports the 'success-file' partition mark done"

      val writeError = intercept[Exception](sql(s"INSERT INTO $name VALUES (1, '20240101', '08')"))
      assert(
        causes(writeError).exists(e => e.getMessage != null && e.getMessage.contains(expected)))
      checkAnswer(sql(s"SELECT id FROM $name"), Nil)

      val procedureError = intercept[Exception] {
        sql(
          "CALL paimon.sys.mark_partition_done(" +
            s"table => 'test_db.$name', partitions => 'dt=20240101,hh=08')")
      }
      assert(procedureError.getMessage.contains(expected))
    }
  }

  private def createTable(name: String, managed: Boolean, properties: String): Unit = {
    sql(
      s"CREATE TABLE $name (id INT, dt STRING, hh STRING) USING csv PARTITIONED BY (dt, hh) " +
        "TBLPROPERTIES ('format-table.implementation'='paimon', " +
        s"'metastore.partitioned-table'='$managed', $properties)")
  }

  private def formatTable(name: String): FormatTable =
    paimonCatalog.getTable(Identifier.create("test_db", name)).asInstanceOf[FormatTable]

  private def successPath(table: FormatTable, partitionDirectory: String): Path =
    new Path(new Path(table.location(), partitionDirectory), "_SUCCESS")

  private def marked(table: FormatTable, partitionDirectory: String): Boolean =
    table.fileIO().exists(successPath(table, partitionDirectory))

  private def successFileLength(table: FormatTable, partitionDirectory: String): Long =
    table.fileIO().getFileStatus(successPath(table, partitionDirectory)).getLen

  private def causes(e: Throwable): Iterator[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null)
}
