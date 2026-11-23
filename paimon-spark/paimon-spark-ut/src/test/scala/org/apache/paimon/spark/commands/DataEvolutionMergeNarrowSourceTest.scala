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

import org.apache.paimon.spark.{PaimonSparkTestBase, SparkTable}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{FallbackReadFileStoreTable, FileStoreTable}

import org.apache.spark.scheduler.{SparkListener, SparkListenerBlockUpdated}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.storage.RDDBlockId

import java.io.FileNotFoundException
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class DataEvolutionMergeNarrowSourceTest extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  private def relation(plan: LogicalPlan): DataSourceV2Relation =
    plan.collectFirst { case r: DataSourceV2Relation => r }.get

  private def fileTable(plan: LogicalPlan): FileStoreTable =
    relation(plan).table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]

  private def withTables(dataEvolutionSource: Boolean = false)(body: => Unit): Unit = {
    withSparkSQLConf(
      "spark.paimon.write.use-v2-write" -> "false",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "true") {
      withTable("target", "source") {
        sql("""CREATE TABLE target (id INT, value INT, payload STRING) TBLPROPERTIES (
              |'bucket'='-1', 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
              |'data-evolution.merge-into.join-reuse'='true',
              |'data-evolution.merge-into.join-reuse.narrow-source.enabled'='true')""".stripMargin)
        val sourceOptions = if (dataEvolutionSource) {
          ",'row-tracking.enabled'='true','data-evolution.enabled'='true'"
        } else {
          ""
        }
        sql(s"""CREATE TABLE source (id INT, value INT, insert_payload STRING)
               |TBLPROPERTIES ('bucket'='-1'$sourceOptions)""".stripMargin)
        body
      }
    }
  }

  private def withNarrowPlan(expectNarrow: Boolean = true)(body: => Unit): Unit = {
    val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
    val semiJoinSeen = new AtomicBoolean(false)
    val sharedCacheSeen = new AtomicBoolean(false)
    val cachedInsertPayload = new AtomicBoolean(false)
    val listener = new QueryExecutionListener {
      override def onSuccess(name: String, qe: QueryExecution, nanos: Long): Unit = {
        if (qe.executedPlan.toString.contains("LeftSemi")) semiJoinSeen.set(true)
        foreach(qe.executedPlan) {
          case memory: InMemoryTableScanExec
              if memory.relation.cachedPlan.toString.contains("FullOuter") =>
            sharedCacheSeen.set(true)
            if (memory.relation.output.exists(_.name == "insert_payload")) {
              cachedInsertPayload.set(true)
            }
          case _ =>
        }
      }
      override def onFailure(name: String, qe: QueryExecution, e: Exception): Unit = {}
    }
    Utils.waitUntilEventEmpty(spark)
    spark.listenerManager.register(listener)
    try {
      body
      Utils.waitUntilEventEmpty(spark)
      assert(sharedCacheSeen.get(), "Expected the shared full-outer match cache to be consumed")
      if (expectNarrow) {
        assert(semiJoinSeen.get(), "Expected late source payload materialization through LeftSemi")
        assert(!cachedInsertPayload.get(), "INSERT-only payload must not enter the shared cache")
      } else {
        assert(!semiJoinSeen.get(), "Fallback must not add a source payload LeftSemi join")
        assert(cachedInsertPayload.get(), "Fallback must retain INSERT payload in the shared cache")
      }
      assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  for (dataEvolution <- Seq(false, true)) {
    test(s"pin narrow source preserves snapshot and ExprIds for dataEvolution=$dataEvolution") {
      withTables(dataEvolution) {
        sql("INSERT INTO source VALUES (1,10,'old')")
        val metadataColumn = if (dataEvolution) {
          PaimonMetadataColumn.ROW_ID_COLUMN
        } else {
          PaimonMetadataColumn.FILE_PATH_COLUMN
        }
        val metadataValue = sql(s"SELECT $metadataColumn FROM source").head().get(0)
        assert(metadataValue != null)
        if (dataEvolution) assert(metadataValue == 0L)
        val plan = sql(
          s"SELECT s.id AS source_id,s.value,s.insert_payload,s.$metadataColumn FROM source s").queryExecution.analyzed
        val original = fileTable(plan)
        val originalOptions = original.options().asScala.toMap
        val snapshot = original.snapshotManager().latestSnapshotId()
        val pinned = MergeIntoPaimonDataEvolutionTable
          .pinNarrowSource(plan, createRelationV2("target"))
        assert(pinned.isDefined, "A passthrough alias/project over a source table must be pinnable")
        val pinnedPlan = pinned.get
        assert(pinnedPlan.output.map(_.exprId) == plan.output.map(_.exprId))
        assert(relation(pinnedPlan).output.map(_.exprId) == relation(plan).output.map(_.exprId))
        assert(!(fileTable(pinnedPlan) eq original), "Pinning must copy the source table")
        assert(fileTable(pinnedPlan).options().get("scan.snapshot-id") == snapshot.toString)

        // Construct and execute the Dataset only after the source has changed.
        if (dataEvolution) sql("UPDATE source SET value=99 WHERE id=1")
        sql("INSERT INTO source VALUES (2,20,'new')")
        checkAnswer(createDataset(spark, pinnedPlan), Seq(Row(1, 10, "old", metadataValue)))
        checkAnswer(
          sql("SELECT * FROM source"),
          Seq(Row(1, if (dataEvolution) 99 else 10, "old"), Row(2, 20, "new")))
        assert(original.options().asScala.toMap == originalOptions)
        assert(loadTable("source").options().get("scan.snapshot-id") == null)
      }
    }
  }

  test("pinned source retains its snapshot when recomputed after unpersist") {
    withTables(dataEvolutionSource = true) {
      sql("INSERT INTO source VALUES (1,10,'old')")
      val plan = sql("SELECT * FROM source").queryExecution.analyzed
      val pinned = MergeIntoPaimonDataEvolutionTable
        .pinNarrowSource(plan, createRelationV2("target"))
      assert(pinned.isDefined)
      val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
      val dataset = createDataset(spark, pinned.get).persist()
      try {
        checkAnswer(dataset, Seq(Row(1, 10, "old")))
        assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).nonEmpty)
        dataset.unpersist(blocking = true)
        assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
        sql("UPDATE source SET value=99 WHERE id=1")
        sql("INSERT INTO source VALUES (2,20,'new')")
        checkAnswer(dataset, Seq(Row(1, 10, "old")))
        checkAnswer(sql("SELECT * FROM source"), Seq(Row(1, 99, "old"), Row(2, 20, "new")))
      } finally {
        dataset.unpersist(blocking = true)
      }
      assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
    }
  }

  test("pin narrow source preserves snapshot contents when rollback reuses the ID") {
    withTables() {
      sql("INSERT INTO source VALUES (1,100,'matched')")
      val previous = loadTable("source").snapshotManager().latestSnapshotId()
      sql("INSERT INTO source VALUES (4,400,'old')")
      val original = loadTable("source").snapshotManager().latestSnapshot()
      val plan = sql("SELECT * FROM source").queryExecution.analyzed
      val pinned = MergeIntoPaimonDataEvolutionTable
        .pinNarrowSource(plan, createRelationV2("target"))
        .get
      checkAnswer(createDataset(spark, pinned), Seq(Row(1, 100, "matched"), Row(4, 400, "old")))

      loadTable("source").rollbackTo(previous)
      spark.catalog.refreshTable("source")
      sql("INSERT INTO source VALUES (4,499,'replacement')")
      val replacement = loadTable("source").snapshotManager().latestSnapshot()
      assert(replacement.id() == original.id())
      assert(replacement.schemaId() == original.schemaId())
      assert(replacement.deltaManifestList() != original.deltaManifestList())
      checkAnswer(
        sql("SELECT * FROM source"),
        Seq(Row(1, 100, "matched"), Row(4, 499, "replacement")))
      assert(fileTable(pinned).snapshotManager().snapshot(original.id()) == original)
      // The old files may have been deleted by rollback, but the reread must never
      // substitute the replacement contents, even though the IDs and schemas match.
      Try(createDataset(spark, pinned).collect().toSet) match {
        case Success(rows) => assert(rows == Set(Row(1, 100, "matched"), Row(4, 400, "old")))
        case Failure(error) =>
          assert(
            Iterator
              .iterate[Throwable](error)(_.getCause)
              .takeWhile(_ != null)
              .exists(_.isInstanceOf[FileNotFoundException]),
            s"Expected old snapshot files to be unavailable, got $error"
          )
      }
    }
  }

  test("pin narrow source retains explicit time travel instead of choosing latest") {
    withTables(dataEvolutionSource = true) {
      sql("INSERT INTO source VALUES (1,10,'historical')")
      val historicalSnapshot = loadTable("source").snapshotManager().latestSnapshotId()
      sql("UPDATE source SET value=20 WHERE id=1")
      val plan =
        sql(s"SELECT * FROM source VERSION AS OF $historicalSnapshot").queryExecution.analyzed
      val originalOptions = fileTable(plan).options().asScala.toMap
      val pinned = MergeIntoPaimonDataEvolutionTable
        .pinNarrowSource(plan, createRelationV2("target"))
      assert(pinned.isDefined)
      sql("INSERT INTO source VALUES (2,30,'latest')")
      checkAnswer(createDataset(spark, pinned.get), Seq(Row(1, 10, "historical")))
      assert(fileTable(pinned.get).options().get("scan.snapshot-id") == historicalSnapshot.toString)
      assert(fileTable(plan).options().asScala.toMap == originalOptions)
    }
  }

  test("pin narrow source falls back when reads can skip source files") {
    for (option <- Seq("scan.ignore-lost-files", "scan.ignore-corrupt-files")) {
      withTables() {
        sql(s"ALTER TABLE source SET TBLPROPERTIES ('$option'='true')")
        sql("INSERT INTO source VALUES (1,10,'source')")
        val plan = sql("SELECT * FROM source").queryExecution.analyzed
        assert(
          MergeIntoPaimonDataEvolutionTable
            .pinNarrowSource(plan, createRelationV2("target"))
            .isEmpty)
      }
    }
  }

  for (rename <- Seq(true, false)) {
    test(s"pin narrow source falls back after a schema-only change: rename=$rename") {
      withTables() {
        sql("INSERT INTO target VALUES (1,10,'old')")
        sql("INSERT INTO source VALUES (1,100,'unused'),(4,400,'new')")
        val before = loadTable("source")
        val snapshot = before.snapshotManager().latestSnapshotId()
        val schemaId = before.schema().id()
        if (rename) {
          sql("ALTER TABLE source RENAME COLUMN insert_payload TO payload2")
        } else {
          sql("ALTER TABLE source ADD COLUMNS (payload2 STRING)")
        }
        val plan = sql("SELECT * FROM source").queryExecution.analyzed
        val original = fileTable(plan)
        val originalOptions = original.options().asScala.toMap
        assert(original.snapshotManager().latestSnapshotId() == snapshot)
        assert(original.schema().id() > schemaId)
        assert(
          MergeIntoPaimonDataEvolutionTable
            .pinNarrowSource(plan, createRelationV2("target"))
            .isEmpty,
          "Pinning must not replace the current source schema with the data snapshot schema"
        )
        val payload = if (rename) "s.payload2" else "COALESCE(s.payload2,s.insert_payload)"
        sql(s"""MERGE INTO target t USING source s ON t.id=s.id
               |WHEN MATCHED THEN UPDATE SET value=s.value
               |WHEN NOT MATCHED THEN INSERT (id,value,payload)
               |VALUES (s.id,s.value,$payload)""".stripMargin)
        checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, 100, "old"), Row(4, 400, "new")))
        assert(original.options().asScala.toMap == originalOptions)
      }
    }
  }

  test("pin narrow source falls back when a retained tag has no snapshot metadata file") {
    withTables() {
      sql("INSERT INTO source VALUES (1,100,'tagged')")
      val table = loadTable("source")
      val snapshot = table.snapshotManager().latestSnapshotId()
      table.createTag("retained", snapshot)
      sql("INSERT INTO source VALUES (4,400,'latest')")
      assert(table.fileIO().delete(table.snapshotManager().snapshotPath(snapshot), false))
      assert(!table.snapshotManager().snapshotExists(snapshot))
      val query = "SELECT * FROM source VERSION AS OF 'retained'"
      checkAnswer(sql(query), Seq(Row(1, 100, "tagged")))
      val plan = sql(query).queryExecution.analyzed
      val originalOptions = fileTable(plan).options().asScala.toMap
      assert(
        MergeIntoPaimonDataEvolutionTable
          .pinNarrowSource(plan, createRelationV2("target"))
          .isEmpty,
        "A retained tag must keep its tag reader when the numbered snapshot file has expired"
      )
      checkAnswer(createDataset(spark, plan), Seq(Row(1, 100, "tagged")))
      assert(fileTable(plan).options().asScala.toMap == originalOptions)
    }
  }

  test("pin narrow source rejects self reads and complex analyzed source plans") {
    withTables() {
      sql("INSERT INTO source VALUES (1,10,'source')")
      sql("INSERT INTO target VALUES (1,10,'target')")
      val target = createRelationV2("target")
      val self = sql("SELECT t.id AS source_id,t.value FROM target t").queryExecution.analyzed
      assert(MergeIntoPaimonDataEvolutionTable.pinNarrowSource(self, target).isEmpty)
      withTempView("computed_source") {
        sql("CREATE TEMPORARY VIEW computed_source AS SELECT id,value+1 AS value FROM source")
        Seq(
          "SELECT * FROM computed_source",
          "SELECT * FROM source WHERE id=1",
          "SELECT id,value+1 AS value FROM source",
          "SELECT id,COUNT(*) FROM source GROUP BY id",
          "SELECT s.id FROM source s JOIN target t ON s.id=t.id",
          "SELECT * FROM source UNION ALL SELECT * FROM source"
        ).foreach {
          query =>
            assert(
              MergeIntoPaimonDataEvolutionTable
                .pinNarrowSource(sql(query).queryExecution.analyzed, target)
                .isEmpty,
              s"Expected fallback for $query")
        }
      }
    }
  }

  test("pin narrow source distinguishes branches of the same table") {
    withTables() {
      sql("INSERT INTO target VALUES (1,10,'branch-base')")
      sql("CALL sys.create_tag(table => 'test.target', tag => 'base', snapshot => 1)")
      sql("CALL sys.create_branch(table => 'test.target', branch => 'other', tag => 'base')")
      val plan = sql("SELECT * FROM `target$branch_other`").queryExecution.analyzed
      val pinned = MergeIntoPaimonDataEvolutionTable
        .pinNarrowSource(plan, createRelationV2("target"))
      assert(pinned.isDefined, "A different branch is a distinct source identity")
      sql("INSERT INTO target VALUES (2,20,'main-only')")
      checkAnswer(createDataset(spark, pinned.get), Seq(Row(1, 10, "branch-base")))
    }
  }

  test("narrow source preserves newer fallback branch partitions with the flag on or off") {
    Seq(false, true).foreach {
      narrow =>
        withTables() {
          sql("DROP TABLE source")
          sql("""CREATE TABLE source (id INT, value INT, insert_payload STRING, p INT)
                |PARTITIONED BY (p) TBLPROPERTIES ('bucket'='-1')""".stripMargin)
          sql("INSERT INTO source VALUES (1,100,'unused',0)")
          sql("CALL sys.create_tag(table => 'test.source', tag => 'base', snapshot => 1)")
          sql("CALL sys.create_branch(table => 'test.source', branch => 'other', tag => 'base')")
          sql("ALTER TABLE source SET TBLPROPERTIES ('scan.fallback-branch'='other')")
          // Keep the main data snapshot's schema current so schema mismatch cannot mask pinning.
          sql("INSERT INTO source VALUES (2,200,'main',0)")
          sql("INSERT INTO `source$branch_other` VALUES (4,400,'fallback',1)")
          val source = loadTable("source").asInstanceOf[FallbackReadFileStoreTable]
          val mainSnapshot = source.latestSnapshot().get()
          assert(mainSnapshot.schemaId() == source.schema().id())
          assert(source.other().latestSnapshot().get().timeMillis() > mainSnapshot.timeMillis())
          checkAnswer(
            sql("SELECT id,value,insert_payload FROM source"),
            Seq(Row(1, 100, "unused"), Row(2, 200, "main"), Row(4, 400, "fallback")))
          sql(
            s"""ALTER TABLE target SET TBLPROPERTIES (
               |'data-evolution.merge-into.join-reuse.narrow-source.enabled'='$narrow')""".stripMargin)
          sql("INSERT INTO target VALUES (1,10,'old')")
          withNarrowPlan(expectNarrow = false) {
            sql("""MERGE INTO target t USING source s ON t.id=s.id
                  |WHEN MATCHED THEN UPDATE SET value=s.value
                  |WHEN NOT MATCHED THEN INSERT (id,value,payload)
                  |VALUES (s.id,s.value,s.insert_payload)""".stripMargin)
            checkAnswer(
              sql("SELECT * FROM target"),
              Seq(Row(1, 100, "old"), Row(2, 200, "main"), Row(4, 400, "fallback")))
          }
        }
    }
  }

  test("pin narrow source returns None before the first source snapshot") {
    withTables() {
      val plan = sql("SELECT * FROM source").queryExecution.analyzed
      assert(loadTable("source").snapshotManager().latestSnapshotId() == null)
      assert(
        MergeIntoPaimonDataEvolutionTable
          .pinNarrowSource(plan, createRelationV2("target"))
          .isEmpty)
      sql("INSERT INTO target VALUES (1,10,'unchanged')")
      sql("""MERGE INTO target t USING source s ON t.id=s.id
            |WHEN MATCHED THEN UPDATE SET value=s.value
            |WHEN NOT MATCHED THEN INSERT (id,value,payload)
            |VALUES (s.id,s.value,s.insert_payload)""".stripMargin)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, 10, "unchanged")))
    }
  }

  test("pin narrow source falls back for compacted-full startup mode") {
    withTables() {
      sql("INSERT INTO source VALUES (1,10,'source')")
      val target = createRelationV2("target")
      withSparkSQLConf("spark.paimon.scan.mode" -> "compacted-full") {
        val plan = sql("SELECT * FROM source").queryExecution.analyzed
        assert(fileTable(plan).options().get("scan.mode") == "compacted-full")
        assert(MergeIntoPaimonDataEvolutionTable.pinNarrowSource(plan, target).isEmpty)
      }
    }
  }

  for (nullSafe <- Seq(false, true)) {
    test(s"narrow source preserves NULL matching and false update conditions: nullSafe=$nullSafe") {
      withTables() {
        sql("INSERT INTO target VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(NULL,40,'d')")
        sql("""INSERT INTO source VALUES (1,100,'new1'),(2,200,'new2'),
              |(4,400,'new4'),(NULL,900,'newnull')""".stripMargin)
        val oldPairs = sql("SELECT payload,_ROW_ID FROM target").collect().toSet
        val snapshot = loadTable("target").snapshotManager().latestSnapshotId()
        withNarrowPlan(expectNarrow = !nullSafe) {
          val equality = if (nullSafe) "<=>" else "="
          sql(s"""MERGE INTO target t USING source s ON t.id $equality s.id
                 |WHEN MATCHED AND (s.id=1 OR s.id IS NULL) THEN UPDATE SET value=s.value
                 |WHEN NOT MATCHED AND (s.id=4 OR s.id IS NULL) THEN
                 |INSERT (id,value,payload) VALUES (s.id,s.value,s.insert_payload)""".stripMargin)
          checkAnswer(
            sql("SELECT * FROM target"),
            Seq(
              Row(1, 100, "a"),
              Row(2, 20, "b"),
              Row(3, 30, "c"),
              Row(null, if (nullSafe) 900 else 40, "d"),
              Row(4, 400, "new4")) ++
              (if (nullSafe) Nil else Seq(Row(null, 900, "newnull")))
          )
          assert(
            sql("SELECT payload,_ROW_ID FROM target WHERE payload IN ('a','b','c','d')")
              .collect()
              .toSet == oldPairs)
          assert(loadTable("target").snapshotManager().latestSnapshotId() == snapshot + 1)
        }
      }
    }
  }

  test("narrow source preserves multiple insert action schemas and duplicate unmatched rows") {
    withTables(dataEvolutionSource = true) {
      sql("INSERT INTO target VALUES (1,10,'old')")
      sql("""INSERT INTO source VALUES (1,100,'unused'),(4,400,'first'),
            |(5,500,'second'),(5,500,'second'),(6,600,'ignored')""".stripMargin)
      withNarrowPlan() {
        sql("""MERGE INTO target t USING source s ON t.id=s.id
              |WHEN MATCHED THEN UPDATE SET value=s.value
              |WHEN NOT MATCHED AND s.id=4 THEN INSERT (id,value,payload)
              |VALUES (s.id,s.value+1,s.insert_payload)
              |WHEN NOT MATCHED AND s.id=5 THEN INSERT (payload,id,value)
              |VALUES (s.insert_payload,s.id,s.value+2)""".stripMargin)
        checkAnswer(
          sql("SELECT * FROM target"),
          Seq(
            Row(1, 100, "old"),
            Row(4, 401, "first"),
            Row(5, 502, "second"),
            Row(5, 502, "second")))
      }
    }
  }

  test("narrow source rebuilds matches for a reused source schema after cleanup") {
    withTables(dataEvolutionSource = true) {
      sql("INSERT INTO target VALUES (1,10,'old')")
      sql("INSERT INTO source VALUES (1,100,'unused'),(4,400,'first')")
      val merge = """MERGE INTO target t USING
                    |(SELECT id,value,insert_payload AS payload FROM source) s ON t.id=s.id
                    |WHEN MATCHED THEN UPDATE SET value=s.value
                    |WHEN NOT MATCHED THEN INSERT (id,value,payload)
                    |VALUES (s.id,s.value,s.payload)""".stripMargin
      val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
      sql(merge)
      sql("UPDATE source SET value=value+1")
      sql("INSERT INTO source VALUES (5,500,'second')")
      sql(merge)
      checkAnswer(
        sql("SELECT * FROM target"),
        Seq(Row(1, 101, "old"), Row(4, 401, "first"), Row(5, 500, "second")))
      assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
    }
  }

  test("narrow source releases materialized matches when duplicate source matches fail") {
    withTables() {
      sql("INSERT INTO target VALUES (1,10,'unchanged')")
      sql("INSERT INTO source VALUES (1,100,'a'),(1,200,'b'),(4,400,'not-committed')")
      val rows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
      val snapshot = loadTable("target").snapshotManager().latestSnapshotId()
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
          sql("""MERGE INTO target t USING source s ON t.id=s.id
                |WHEN MATCHED THEN UPDATE SET value=s.value
                |WHEN NOT MATCHED THEN INSERT (id,value,payload)
                |VALUES (s.id,s.value,s.insert_payload)""".stripMargin)
        }
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toSeq
        assert(
          causes.exists(
            cause => Option(cause.getMessage).exists(_.contains("Row ID should be incremental."))),
          s"Expected duplicate RowID rejection, got ${causes.mkString("; ")}"
        )
        assert(
          !causes.exists(
            cause =>
              Option(cause.getMessage).exists(
                message =>
                  message.contains("MISSING_ATTRIBUTES") ||
                    message.contains("Resolved attribute(s)"))),
          "Duplicate matches must fail on RowID validation rather than cache attribute binding"
        )
        assert(cachedBlockSeen.await(10, TimeUnit.SECONDS), "Expected a materialized disk cache")
        checkAnswer(sql("SELECT *,_ROW_ID FROM target"), rows)
        assert(loadTable("target").snapshotManager().latestSnapshotId() == snapshot)
        assert(spark.sparkContext.getPersistentRDDs.keySet == persistedBefore)
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }
}
