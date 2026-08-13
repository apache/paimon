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

import org.apache.paimon.blob.ManagedBlobReferenceFile
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.utils.DateTimeUtils

class ManagedBlobOrphanFilesProcedureTest extends PaimonSparkTestBase {

  Seq("local", "distributed").foreach {
    mode =>
      test(s"Paimon procedure: remove unreferenced managed blob pack ($mode)") {
        createManagedBlobTable()
        spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

        val table = loadTable("T")
        val orphan = new Path(bucketPath(table), "orphan.managed.blob")
        table.fileIO().newOutputStream(orphan, false).close()
        Thread.sleep(2000)

        val referenced = filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)
          .filterNot(_.getName == orphan.getName)
        assert(referenced.nonEmpty)

        val olderThan = DateTimeUtils.formatLocalDateTime(
          DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
          3)
        spark.sql(
          s"CALL sys.remove_orphan_files(table => 'T', older_than => '$olderThan', mode => '$mode')")

        assert(!table.fileIO().exists(orphan))
        referenced.foreach(pack => assert(table.fileIO().exists(pack)))
      }

      test(s"Paimon procedure: skip managed blob pack gc when sidecar missing ($mode)") {
        createManagedBlobTable()
        spark.sql("INSERT INTO T VALUES (1, 'a', X'0102')")

        val table = loadTable("T")
        val orphanPack = new Path(bucketPath(table), "orphan.managed.blob")
        val orphanOther = new Path(bucketPath(table), "orphan.txt")
        table.fileIO().newOutputStream(orphanPack, false).close()
        table.fileIO().writeFile(orphanOther, "x", true)
        Thread.sleep(2000)

        val referenced = filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)
          .filterNot(_.getName == orphanPack.getName)
        assert(referenced.nonEmpty)
        filesWithSuffix(table, ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)
          .foreach(table.fileIO().deleteQuietly)

        val olderThan = DateTimeUtils.formatLocalDateTime(
          DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
          3)
        spark.sql(
          s"CALL sys.remove_orphan_files(table => 'T', older_than => '$olderThan', mode => '$mode')")

        assert(table.fileIO().exists(orphanPack))
        assert(!table.fileIO().exists(orphanOther))
        referenced.foreach(pack => assert(table.fileIO().exists(pack)))
      }
  }

  private def createManagedBlobTable(): Unit = {
    spark.sql("""
                |CREATE TABLE T (id INT, name STRING, payload BINARY)
                |USING PAIMON
                |TBLPROPERTIES (
                |  'primary-key'='id',
                |  'bucket'='1',
                |  'changelog-producer'='none',
                |  'blob-field'='payload')
                |""".stripMargin)
  }

  private def bucketPath(table: FileStoreTable): Path = {
    table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0)
  }

  private def filesWithSuffix(table: FileStoreTable, suffix: String): Seq[Path] = {
    val statuses = table.fileIO().listStatus(bucketPath(table))
    if (statuses == null) {
      Seq.empty
    } else {
      statuses.map(_.getPath).filter(_.getName.endsWith(suffix))
    }
  }
}
