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

import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.TableCommitImpl
import org.apache.paimon.utils.SnapshotNotExistException

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class CreateTagFromWatermarkProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: create tags from snapshots watermark") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES ('primary-key'='a', 'bucket'='1')
                 |""".stripMargin)

    val table = loadTable("T")
    // A Spark batch write carries no watermark, so commit snapshots with explicit
    // watermarks the way a Flink writer would, mirroring the Flink IT case.
    writeWithWatermark(table, 1L, 1000L, GenericRow.of(1, BinaryString.fromString("a")))
    writeWithWatermark(table, 2L, 2000L, GenericRow.of(2, BinaryString.fromString("b")))
    writeWithWatermark(table, 3L, 3000L, GenericRow.of(3, BinaryString.fromString("c")))

    val commitTime1 = table.snapshotManager.snapshot(1).timeMillis
    val commitTime2 = table.snapshotManager.snapshot(2).timeMillis

    // watermark below snapshot 1 resolves to snapshot 1.
    checkAnswer(
      spark.sql(s"""CALL paimon.sys.create_tag_from_watermark(
                   |table => 'test.T', tag => 'tag1', watermark => 500)""".stripMargin),
      Row("tag1", 1, commitTime1, "1000") :: Nil
    )

    // watermark equal to snapshot 2 resolves to snapshot 2.
    checkAnswer(
      spark.sql(s"""CALL paimon.sys.create_tag_from_watermark(
                   |table => 'test.T', tag => 'tag2', watermark => 2000)""".stripMargin),
      Row("tag2", 2, commitTime2, "2000") :: Nil
    )

    // watermark later than the latest snapshot throws SnapshotNotExistException.
    assertThrows[SnapshotNotExistException] {
      spark.sql(s"""CALL paimon.sys.create_tag_from_watermark(
                   |table => 'test.T', tag => 'tag3', watermark => ${Long.MaxValue})""".stripMargin)
    }
  }

  private def writeWithWatermark(
      table: FileStoreTable,
      commitId: Long,
      watermark: Long,
      rows: GenericRow*): Unit = {
    val writeBuilder = table.newStreamWriteBuilder()
    val writer = writeBuilder.newWrite()
    val commit = writeBuilder.newCommit().asInstanceOf[TableCommitImpl]
    try {
      rows.foreach(writer.write)
      val committable = new ManifestCommittable(commitId, watermark)
      writer.prepareCommit(true, commitId).asScala.foreach(committable.addFileCommittable)
      commit.commit(committable)
    } finally {
      writer.close()
      commit.close()
    }
  }
}
