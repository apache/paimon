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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.errors.ErrorMessages
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockUpdated}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.storage.RDDBlockId

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

class DataEvolutionMergeJoinReuseConflictTest extends PaimonSparkTestBase {

  private def table: FileStoreTable =
    paimonCatalog.getTable(Identifier.create(dbName0, "target")).asInstanceOf[FileStoreTable]

  private def snapshotId: Long = table.snapshotManager().latestSnapshotId()

  private def files: Seq[DataFileMeta] =
    table
      .newReadBuilder()
      .newScan()
      .plan()
      .splits()
      .asScala
      .flatMap(_.asInstanceOf[DataSplit].dataFiles().asScala)
      .map(file => file.fileName() -> file)
      .toMap
      .values
      .toSeq

  private def withTarget(body: => Unit): Unit = {
    withSparkSQLConf(
      "spark.paimon.write.use-v2-write" -> "false",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "true") {
      withTable("target") {
        sql("""CREATE TABLE target (id INT, value INT, payload STRING) TBLPROPERTIES (
              |'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
              |'data-evolution.merge-into.join-reuse'='true', 'write-only'='true')""".stripMargin)
        sql("""INSERT INTO target SELECT /*+ REPARTITION(1) */ *
              |FROM VALUES (1,10,'a'),(2,20,'b') AS v(id,value,payload)""".stripMargin)
        sql("""INSERT INTO target SELECT /*+ REPARTITION(1) */ *
              |FROM VALUES (3,30,'c'),(4,40,'d') AS v(id,value,payload)""".stripMargin)
        body
      }
    }
  }

  private def pinnedMerge(): MergeIntoPaimonDataEvolutionTable = {
    val parsed = spark.sessionState.sqlParser.parsePlan(
      """MERGE INTO target t USING (SELECT * FROM VALUES (1,100),(5,500) AS v(id,value)) s
        |ON t.id=s.id
        |WHEN MATCHED THEN UPDATE SET value=s.value
        |WHEN NOT MATCHED THEN INSERT (id,value,payload) VALUES (s.id,s.value,'new')""".stripMargin)
    val command = spark.sessionState.analyzer
      .executeAndCheck(parsed, new QueryPlanningTracker)
      .asInstanceOf[MergeIntoPaimonDataEvolutionTable]
    val original = PaimonRelation.getPaimonRelation(command.targetTable)
    val (pinnedTable, pinnedRelation) =
      MergeIntoPaimonDataEvolutionTable.withMatchedUpdateScanOptions(command.v2Table, original)
    command.copy(
      v2Table = pinnedTable,
      targetTable = command.targetTable.transformDown {
        case relation: DataSourceV2Relation if relation == original => pinnedRelation
      })
  }

  private def withMaterializedCache(body: => Unit): Unit = {
    val before = spark.sparkContext.getPersistentRDDs.keySet
    val created = new CountDownLatch(1)
    val listener = new SparkListener {
      override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
        val info = event.blockUpdatedInfo
        if (
          info.blockId.isInstanceOf[RDDBlockId] && info.diskSize > 0 &&
          info.storageLevel.useDisk && !info.storageLevel.useMemory &&
          !before.contains(info.blockId.asInstanceOf[RDDBlockId].rddId)
        ) {
          created.countDown()
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      assert(created.await(10, TimeUnit.SECONDS), "The shared disk cache must actually materialize")
      assert(spark.sparkContext.getPersistentRDDs.keySet == before)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("join reuse preserves a concurrent update and publishes no partial insert on conflict") {
    withTarget {
      val merge = pinnedMerge()
      sql("UPDATE target SET value=-1 WHERE id=1")
      val concurrentRows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
      val concurrentSnapshot = snapshotId
      withMaterializedCache {
        val error = intercept[Exception](merge.run(spark))
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        assert(
          causes.exists(
            cause =>
              Option(cause.getMessage).exists(
                _.contains(ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE))),
          s"Expected a row-ID conflict, got $error"
        )
      }
      checkAnswer(sql("SELECT *,_ROW_ID FROM target"), concurrentRows)
      assert(snapshotId == concurrentSnapshot)
    }
  }

  test("join reuse rewrites stale partial ranges after compaction and commits inserts atomically") {
    withTarget {
      val oldPairs = sql("SELECT id,_ROW_ID FROM target").collect().toSeq
      val oldRanges = files.map(file => (file.firstRowId().longValue(), file.rowCount())).toSet
      assert(oldRanges == Set((0L, 2L), (2L, 2L)))
      val merge = pinnedMerge()
      sql("CALL sys.compact(table => 'target', options => 'compaction.min.file-num=2')")
      val compactedRanges =
        files.map(file => (file.firstRowId().longValue(), file.rowCount())).toSet
      assert(
        compactedRanges == Set((0L, 4L)),
        s"Expected changed compaction ranges: $compactedRanges")
      val compactedSnapshot = snapshotId
      withMaterializedCache {
        merge.run(spark)
      }
      checkAnswer(
        sql("SELECT * FROM target"),
        Seq(
          Row(1, 100, "a"),
          Row(2, 20, "b"),
          Row(3, 30, "c"),
          Row(4, 40, "d"),
          Row(5, 500, "new")))
      checkAnswer(sql("SELECT id,_ROW_ID FROM target WHERE id<=4"), oldPairs)
      checkAnswer(sql("SELECT _ROW_ID FROM target"), (0L to 4L).map(Row(_)))
      checkAnswer(sql("SELECT _ROW_ID FROM target WHERE id=5"), Seq(Row(4L)))
      assert(snapshotId == compactedSnapshot + 1)
      val rewritten =
        files.filter(file => Option(file.writeCols()).exists(_.asScala.toSeq == Seq("value")))
      assert(
        rewritten.exists(file => file.firstRowId().longValue() == 0L && file.rowCount() == 4L),
        "The stale two-row partial update must be rewritten to the current four-row range"
      )
    }
  }
}
