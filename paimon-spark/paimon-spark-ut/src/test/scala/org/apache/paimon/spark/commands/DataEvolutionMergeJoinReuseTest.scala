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
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockUpdated}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.storage.RDDBlockId

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

class DataEvolutionMergeJoinReuseTest extends PaimonSparkTestBase {

  private def snapshotId: Long =
    paimonCatalog
      .getTable(Identifier.create(dbName0, "target"))
      .asInstanceOf[FileStoreTable]
      .snapshotManager()
      .latestSnapshotId()

  private def createTarget(enabled: Boolean): Unit = {
    sql(s"""CREATE TABLE target (id INT, value INT, payload STRING) TBLPROPERTIES (
           |'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
           |'data-evolution.merge-into.join-reuse'='$enabled')""".stripMargin)
    sql("INSERT INTO target VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(NULL,40,'d')")
  }

  for (nullSafe <- Seq(false, true)) {
    test(s"join reuse preserves complete ranges, action conditions and nullSafe=$nullSafe") {
      for (enabled <- Seq(false, true)) {
        withSparkSQLConf(
          "spark.paimon.write.use-v2-write" -> "false",
          "spark.sql.adaptive.enabled" -> "true",
          "spark.sql.adaptive.coalescePartitions.enabled" -> "true") {
          withTable("target") {
            withTempView("source") {
              createTarget(enabled)
              sql("""CREATE TEMPORARY VIEW source AS SELECT * FROM VALUES
                    |(1,100,'new1'),(2,200,'new2'),(4,400,'new4'),(NULL,900,'newnull')
                    |AS s(id,value,payload)""".stripMargin)
              val oldPairs = sql("SELECT payload,_ROW_ID FROM target").collect().toSet
              val before = snapshotId
              val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
              val sharedPlanSeen = new CountDownLatch(1)
              val targetScanSeen = new AtomicBoolean(false)
              val unusedTargetPayloadRead = new AtomicBoolean(false)
              val listener = new QueryExecutionListener {
                override def onSuccess(name: String, qe: QueryExecution, nanos: Long): Unit = {
                  val plan = qe.executedPlan.toString
                  if (plan.contains("InMemoryTableScan") && plan.contains("FullOuter")) {
                    val targetScans = plan
                      .split("\n")
                      .filter(_.contains("PaimonSplitScan: [target]"))
                    if (targetScans.nonEmpty) targetScanSeen.set(true)
                    if (targetScans.exists(_.takeWhile(_ != ']').contains("payload#"))) {
                      unusedTargetPayloadRead.set(true)
                    }
                    sharedPlanSeen.countDown()
                  }
                }
                override def onFailure(name: String, qe: QueryExecution, e: Exception): Unit = {}
              }
              spark.listenerManager.register(listener)
              try {
                val equality = if (nullSafe) "<=>" else "="
                sql(s"""MERGE INTO target t USING source s ON t.id $equality s.id
                       |WHEN MATCHED AND s.id=1 THEN UPDATE SET value=s.value
                       |WHEN MATCHED AND s.id IS NULL THEN UPDATE SET value=s.value
                       |WHEN NOT MATCHED AND (s.id=4 OR s.id IS NULL) THEN
                       |INSERT (id,value,payload) VALUES (s.id,s.value,s.payload)""".stripMargin)
                val expected = Seq(
                  Row(1, 100, "a"),
                  Row(2, 20, "b"),
                  Row(3, 30, "c"),
                  Row(null, if (nullSafe) 900 else 40, "d"),
                  Row(4, 400, "new4")) ++
                  (if (nullSafe) Nil else Seq(Row(null, 900, "newnull")))
                checkAnswer(sql("SELECT * FROM target"), expected)
                assert(
                  sql("SELECT payload,_ROW_ID FROM target WHERE payload IN ('a','b','c','d')")
                    .collect()
                    .toSet == oldPairs)
                assert(snapshotId == before + 1)
                assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
                if (enabled) {
                  assert(
                    sharedPlanSeen.await(10, TimeUnit.SECONDS),
                    "Expected a cached full-outer match plan")
                  assert(
                    targetScanSeen.get(),
                    "Expected the cached plan to identify its target scan")
                  assert(
                    !unusedTargetPayloadRead.get(),
                    "Unchanged target payload must stay pruned")
                }
              } finally {
                spark.listenerManager.unregister(listener)
              }
            }
          }
        }
      }
    }
  }

  test("join reuse identifies presence markers by identity rather than business column names") {
    withSparkSQLConf(
      "spark.paimon.write.use-v2-write" -> "false",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "true") {
      withTable("target") {
        withTempView("source") {
          createTarget(enabled = true)
          sql("""CREATE TEMPORARY VIEW source AS SELECT id,value,
                |CAST(NULL AS INT) AS __row_from_target, CAST(NULL AS INT) AS __row_from_source
                |FROM VALUES (1,100),(4,400) AS s(id,value)""".stripMargin)
          sql(
            """MERGE INTO target t USING source s ON t.id=s.id
              |WHEN MATCHED THEN UPDATE SET value=s.value
              |WHEN NOT MATCHED THEN INSERT (id,value,payload) VALUES
              |(s.id,s.value,COALESCE(CAST(s.__row_from_target+s.__row_from_source AS STRING),'new'))""".stripMargin)
          checkAnswer(
            sql("SELECT * FROM target"),
            Seq(
              Row(1, 100, "a"),
              Row(2, 20, "b"),
              Row(3, 30, "c"),
              Row(null, 40, "d"),
              Row(4, 400, "new")))
        }
      }
    }
  }

  test("join reuse preserves adaptive insert file sizing and falls back without coalescing") {
    for ((adaptive, coalesce) <- Seq((false, true), (true, false), (true, true))) {
      withSparkSQLConf(
        "spark.paimon.write.use-v2-write" -> "false",
        "spark.sql.shuffle.partitions" -> "32",
        "spark.sql.adaptive.enabled" -> adaptive.toString,
        "spark.sql.adaptive.coalescePartitions.enabled" -> coalesce.toString,
        "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "67108864",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1048576"
      ) {
        withTable("target") {
          withTempView("source") {
            createTarget(enabled = true)
            sql("""CREATE TEMPORARY VIEW source AS
                  |SELECT CAST(id AS INT) AS id, CAST(id*100 AS INT) AS value
                  |FROM range(0,128,1,2)""".stripMargin)
            val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
            val sharedPlanSeen = new CountDownLatch(1)
            val listener = new QueryExecutionListener {
              override def onSuccess(name: String, qe: QueryExecution, nanos: Long): Unit = {
                val plan = qe.executedPlan.toString
                if (plan.contains("InMemoryTableScan") && plan.contains("FullOuter")) {
                  sharedPlanSeen.countDown()
                }
              }
              override def onFailure(name: String, qe: QueryExecution, e: Exception): Unit = {}
            }
            spark.listenerManager.register(listener)
            try {
              sql("""MERGE INTO target t USING source s ON t.id=s.id
                    |WHEN MATCHED THEN UPDATE SET value=s.value
                    |WHEN NOT MATCHED THEN INSERT (id,value,payload)
                    |VALUES (s.id,s.value,'new')""".stripMargin)
              val retainedPayloads = Map(1 -> "a", 2 -> "b", 3 -> "c")
              val expected = (0 until 128).map {
                id => Row(id, id * 100, retainedPayloads.getOrElse(id, "new"))
              } :+ Row(null, 40, "d")
              checkAnswer(sql("SELECT * FROM target"), expected)
              val table = paimonCatalog
                .getTable(Identifier.create(dbName0, "target"))
                .asInstanceOf[FileStoreTable]
              val insertedFiles = table
                .newReadBuilder()
                .newScan()
                .plan()
                .splits()
                .asScala
                .flatMap(_.asInstanceOf[DataSplit].dataFiles().asScala)
                .filter(file => Option(file.firstRowId()).exists(_.longValue() >= 4L))
                .map(file => file.fileName() -> file)
                .toMap
                .values
                .toSeq
              assert(
                insertedFiles.nonEmpty && insertedFiles.size <= 2,
                s"Unexpected INSERT files ${insertedFiles.size}: adaptive=$adaptive, coalesce=$coalesce")
              assert(insertedFiles.map(_.rowCount()).sum == 125L)
              assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
              if (adaptive && coalesce) {
                assert(
                  sharedPlanSeen.await(10, TimeUnit.SECONDS),
                  "Expected the supported configuration to reuse the join")
              }
            } finally {
              spark.listenerManager.unregister(listener)
            }
          }
        }
      }
    }
  }

  test("join reuse releases persisted matches when duplicate source matches fail") {
    for (enabled <- Seq(false, true)) {
      withSparkSQLConf(
        "spark.paimon.write.use-v2-write" -> "false",
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true") {
        withTable("target") {
          withTempView("source") {
            createTarget(enabled)
            sql(
              "CREATE TEMPORARY VIEW source AS SELECT * FROM VALUES (1,100),(1,200) AS s(id,value)")
            val rows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
            val before = snapshotId
            val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
            val cachedBlockSeen = new CountDownLatch(1)
            val listener = new SparkListener {
              override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
                val block = event.blockUpdatedInfo
                block.blockId match {
                  case id: RDDBlockId
                      if !persistedBefore.contains(id.rddId) && block.diskSize > 0 &&
                        block.storageLevel.useDisk && !block.storageLevel.useMemory =>
                    cachedBlockSeen.countDown()
                  case _ =>
                }
              }
            }
            spark.sparkContext.addSparkListener(listener)
            try {
              val error = intercept[Exception] {
                sql(
                  """MERGE INTO target t USING source s ON t.id=s.id
                    |WHEN MATCHED THEN UPDATE SET value=s.value
                    |WHEN NOT MATCHED THEN INSERT (id,value,payload) VALUES (s.id,s.value,'new')""".stripMargin)
              }
              val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toSeq
              assert(
                causes.exists(
                  cause =>
                    Option(cause.getMessage).exists(_.contains("Row ID should be incremental."))),
                s"Expected duplicate RowID rejection, got ${causes.mkString("; ")}"
              )
              assert(
                !causes.exists(
                  cause =>
                    Option(cause.getMessage).exists(
                      message =>
                        message.contains("MISSING_ATTRIBUTES") || message.contains(
                          "Resolved attribute(s)"))),
                "Duplicate source matching must not fail because of cache attribute binding"
              )
              checkAnswer(sql("SELECT *,_ROW_ID FROM target"), rows)
              assert(snapshotId == before)
              assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
              if (enabled) {
                assert(
                  cachedBlockSeen.await(10, TimeUnit.SECONDS),
                  "Expected a materialized DISK_ONLY RDD block before duplicate RowID rejection")
              }
              info(
                s"Duplicate RowID rejected: enabled=$enabled, " +
                  s"diskCacheCreated=${cachedBlockSeen.getCount == 0}, " +
                  s"snapshot=$before, persistentRDDsRestored=true; " +
                  causes
                    .map(cause => s"${cause.getClass.getName}: ${cause.getMessage}")
                    .mkString("; "))
            } finally {
              spark.sparkContext.removeSparkListener(listener)
            }
          }
        }
      }
    }
  }
}
