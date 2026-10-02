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

import org.apache.paimon.manifest.ManifestFileMeta
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.ScanMode

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class RemoveUnexistingManifestsProcedureTest extends PaimonSparkTestBase {

  private val tableProperties =
    """
      |TBLPROPERTIES (
      |  'primary-key' = 'k',
      |  'bucket' = '1',
      |  'write-only' = 'true',
      |  'manifest.target-file-size' = '10 B')
      |""".stripMargin

  test("Paimon Procedure: remove unexisting manifests") {
    spark.sql(s"""
                 |CREATE TABLE T (k BIGINT, v STRING)
                 |$tableProperties
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'Hi')")
    spark.sql("INSERT INTO T VALUES (2, 'Hello')")
    spark.sql("INSERT INTO T VALUES (3, 'Paimon')")

    val originals =
      Set(Row(1L, "Hi"), Row(2L, "Hello"), Row(3L, "Paimon"))
    checkAnswer(spark.sql("SELECT * FROM T ORDER BY k"), originals.toList)

    val (table, manifests) = loadManifests("T")
    assert(manifests.size() >= 2)
    val dropped = manifests.get(1)
    val droppedRows = rowCount(table, dropped)
    assert(droppedRows > 0 && droppedRows < originals.size)

    val beforeId = table.snapshotManager().latestSnapshot().id()
    assert(table.fileIO.delete(table.store.pathFactory.toManifestFilePath(dropped.fileName), false))

    checkAnswer(
      spark.sql("CALL sys.remove_unexisting_manifests(table => 'test.T')"),
      Row("Success") :: Nil)

    val remaining = spark.sql("SELECT * FROM T ORDER BY k").collect()
    assert(remaining.length == originals.size - droppedRows)
    assert(remaining.forall(originals.contains))

    val repaired = loadTable("T")
    val latest = repaired.snapshotManager().latestSnapshot()
    assert(latest.id() == beforeId + 1)
    assert(latest.totalRecordCount() == remaining.length.toLong)
    assert(!manifestNames(repaired).contains(dropped.fileName))
  }

  test("Paimon Procedure: remove unexisting manifests is a no-op when files exist") {
    spark.sql("CREATE TABLE T (k INT, v STRING)")
    spark.sql("INSERT INTO T VALUES (1, 'a')")
    val beforeId = loadTable("T").snapshotManager().latestSnapshot().id()

    checkAnswer(
      spark.sql("CALL sys.remove_unexisting_manifests(table => 'test.T')"),
      Row("Success") :: Nil)
    checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Nil)
    assert(loadTable("T").snapshotManager().latestSnapshot().id() == beforeId)
  }

  test("Paimon Procedure: remove unexisting manifests on a branch") {
    spark.sql(s"""
                 |CREATE TABLE T (k BIGINT, v STRING)
                 |$tableProperties
                 |""".stripMargin)
    spark.sql("INSERT INTO T VALUES (1, 'Hi')")
    spark.sql("INSERT INTO T VALUES (2, 'Hello')")
    spark.sql("INSERT INTO T VALUES (3, 'Paimon')")
    spark.sql("CALL sys.create_tag(table => 'test.T', tag => 'base')")
    spark.sql("CALL sys.create_branch(table => 'test.T', branch => 'rt', tag => 'base')")
    spark.sql("INSERT INTO `T$branch_rt` VALUES (4, 'Hi 4')")
    spark.sql("INSERT INTO `T$branch_rt` VALUES (5, 'Hello 5')")
    spark.sql("INSERT INTO `T$branch_rt` VALUES (6, 'Paimon 6')")

    val branchRowsBefore = spark.sql("SELECT * FROM `T$branch_rt`").collect()
    val mainNames = manifestNames(loadTable("T")).toSet
    val (branch, branchManifests) = loadManifests("T$branch_rt")
    val dropped = branchManifests.asScala.find(meta => !mainNames.contains(meta.fileName()))
    assert(dropped.isDefined)
    val droppedMeta = dropped.get
    val droppedRows = rowCount(branch, droppedMeta)
    assert(droppedRows > 0)

    assert(
      branch.fileIO
        .delete(branch.store.pathFactory.toManifestFilePath(droppedMeta.fileName), false))

    checkAnswer(
      spark.sql("CALL sys.remove_unexisting_manifests(table => 'test.`T$branch_rt`')"),
      Row("Success") :: Nil)

    val branchRows = spark.sql("SELECT * FROM `T$branch_rt` ORDER BY k").collect()
    assert(branchRows.length == branchRowsBefore.length - droppedRows)
    val repaired = loadTable("T$branch_rt")
    assert(
      repaired.snapshotManager().latestSnapshot().totalRecordCount() == branchRows.length.toLong)
    assert(!manifestNames(repaired).contains(droppedMeta.fileName))

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY k"),
      Row(1L, "Hi") :: Row(2L, "Hello") :: Row(3L, "Paimon") :: Nil)
  }

  private def loadManifests(
      tableName: String): (FileStoreTable, java.util.List[ManifestFileMeta]) = {
    val table = loadTable(tableName)
    val manifests = table.store
      .newScan()
      .manifestsReader()
      .read(table.snapshotManager.latestSnapshot, ScanMode.ALL)
      .allManifests
    (table, manifests)
  }

  private def manifestNames(table: FileStoreTable): Seq[String] = {
    table.store
      .newScan()
      .manifestsReader()
      .read(table.snapshotManager.latestSnapshot, ScanMode.ALL)
      .allManifests
      .asScala
      .map(_.fileName())
      .toSeq
  }

  private def rowCount(table: FileStoreTable, manifest: ManifestFileMeta): Long = {
    table.store.newScan().readManifest(manifest).asScala.map(_.file().rowCount()).sum
  }
}
