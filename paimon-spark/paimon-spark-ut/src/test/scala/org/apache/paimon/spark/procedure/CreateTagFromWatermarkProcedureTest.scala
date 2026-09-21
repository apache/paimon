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
import org.apache.paimon.table.sink.TableCommitImpl
import org.apache.paimon.utils.SnapshotNotExistException

import org.apache.spark.sql.Row

class CreateTagFromWatermarkProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: create tags from snapshot watermarks") {
    spark.sql("CREATE TABLE T (k INT, v STRING)")

    writeWithWatermark("T", null, GenericRow.of(Integer.valueOf(1), BinaryString.fromString("a")))

    intercept[SnapshotNotExistException] {
      spark.sql(
        "CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag1', watermark => 1000)")
    }

    writeWithWatermark("T", 1000L, GenericRow.of(Integer.valueOf(2), BinaryString.fromString("b")))
    writeWithWatermark("T", 2000L, GenericRow.of(Integer.valueOf(3), BinaryString.fromString("c")))

    val table = loadTable("T")
    val snapshot2 = table.snapshotManager.snapshot(2)
    val snapshot3 = table.snapshotManager.snapshot(3)

    checkAnswer(
      spark.sql(
        s"CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag2', watermark => ${1000L - 1})"),
      Row("tag2", 2L, snapshot2.timeMillis, String.valueOf(snapshot2.watermark)) :: Nil
    )

    checkAnswer(
      spark.sql(
        s"CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag3', watermark => ${1000L + 1})"),
      Row("tag3", 3L, snapshot3.timeMillis, String.valueOf(snapshot3.watermark)) :: Nil
    )

    intercept[SnapshotNotExistException] {
      spark.sql(
        s"CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag4', watermark => ${2000L + 1})")
    }
  }

  test("Paimon Procedure: create tags from tagged snapshot watermarks after expire") {
    spark.sql("CREATE TABLE T (k INT, v STRING)")

    writeWithWatermark("T", 1000L, GenericRow.of(Integer.valueOf(1), BinaryString.fromString("a")))
    spark.sql("CALL sys.create_tag(table => 'test.T', tag => 'tag1', snapshot => 1)")
    writeWithWatermark("T", 2000L, GenericRow.of(Integer.valueOf(2), BinaryString.fromString("b")))

    spark.sql("CALL sys.expire_snapshots(table => 'test.T', retain_max => 1, retain_min => 1)")

    val table = loadTable("T")
    assert(!table.snapshotManager.snapshotExists(1))

    val tagSnapshot = table.tagManager.getOrThrow("tag1")
    val snapshot2 = table.snapshotManager.snapshot(2)

    checkAnswer(
      spark.sql(
        s"CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag2', watermark => ${tagSnapshot.watermark - 1})"),
      Row("tag2", 1L, tagSnapshot.timeMillis, String.valueOf(tagSnapshot.watermark)) :: Nil
    )

    checkAnswer(
      spark.sql(
        s"CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag3', watermark => ${snapshot2.watermark - 1})"),
      Row("tag3", 2L, snapshot2.timeMillis, String.valueOf(snapshot2.watermark)) :: Nil
    )
  }

  test("Paimon Procedure: prefer earlier tagged snapshot when watermarks are equal") {
    spark.sql("CREATE TABLE T (k INT, v STRING)")

    writeWithWatermark("T", 1000L, GenericRow.of(Integer.valueOf(1), BinaryString.fromString("a")))
    spark.sql("CALL sys.create_tag(table => 'test.T', tag => 'keep1', snapshot => 1)")
    writeWithWatermark("T", 1000L, GenericRow.of(Integer.valueOf(2), BinaryString.fromString("b")))

    spark.sql("CALL sys.expire_snapshots(table => 'test.T', retain_max => 1, retain_min => 1)")

    val table = loadTable("T")
    assert(!table.snapshotManager.snapshotExists(1))
    val tagged = table.tagManager.getOrThrow("keep1")

    checkAnswer(
      spark.sql(
        "CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag_from_wm', watermark => 1000)"),
      Row("tag_from_wm", 1L, tagged.timeMillis, String.valueOf(tagged.watermark)) :: Nil
    )
  }

  test("Paimon Procedure: keep earlier live snapshot when a later tag has the same watermark") {
    spark.sql("CREATE TABLE T (k INT, v STRING)")

    writeWithWatermark("T", 1000L, GenericRow.of(Integer.valueOf(1), BinaryString.fromString("a")))
    writeWithWatermark("T", 1000L, GenericRow.of(Integer.valueOf(2), BinaryString.fromString("b")))
    spark.sql("CALL sys.create_tag(table => 'test.T', tag => 'keep2', snapshot => 2)")

    val table = loadTable("T")
    val snapshot1 = table.snapshotManager.snapshot(1)
    assert(table.tagManager.getOrThrow("keep2").id == 2)

    checkAnswer(
      spark.sql(
        "CALL sys.create_tag_from_watermark(table => 'test.T', tag => 'tag_from_wm', watermark => 1000)"),
      Row("tag_from_wm", 1L, snapshot1.timeMillis, String.valueOf(snapshot1.watermark)) :: Nil
    )
  }

  private var commitIdentifier = 0L

  private def writeWithWatermark(
      tableName: String,
      watermark: java.lang.Long,
      rows: GenericRow*): Unit = {
    val table = loadTable(tableName)
    val writeBuilder = table.newStreamWriteBuilder
    val writer = writeBuilder.newWrite
    val commit = writeBuilder.newCommit.asInstanceOf[TableCommitImpl]
    try {
      rows.foreach(writer.write)
      commitIdentifier += 1
      val committable = new ManifestCommittable(commitIdentifier, watermark)
      val messages = writer.prepareCommit(true, commitIdentifier)
      var i = 0
      while (i < messages.size()) {
        committable.addFileCommittable(messages.get(i))
        i += 1
      }
      commit.commit(committable)
    } finally {
      writer.close()
      commit.close()
    }
  }
}
