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

import org.apache.paimon.Snapshot
import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.errors.ErrorMessages
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class DataEvolutionMergeRetryTest extends PaimonSparkTestBase {

  test("merge retries an append update conflict using the latest target values") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id UNION ALL SELECT 2 AS id) s " +
          "ON t.id = s.id WHEN MATCHED THEN UPDATE SET b = t.b + 1")

      concurrentMerge("b = t.b + 10")
      assert(loadTable("t").latestSnapshot().get().commitKind() == CommitKind.APPEND)
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 12), Row(2, 3)))
    }
  }

  test("merge retry reevaluates matched conditions") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id) s ON t.id = s.id " +
          "WHEN MATCHED AND t.status = 'pending' THEN UPDATE SET status = 'done'")

      concurrentMerge("status = 'cancelled'")
      merge.run(spark)

      checkAnswer(sql("SELECT status FROM t"), Seq(Row("cancelled")))
    }
  }

  test("merge retry honors the commit retry limit") {
    withTable("t") {
      createTable("'commit.max-retries' = '0',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id UNION ALL SELECT 2 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET b = t.b + 1 " +
          "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, 0, 'new')")

      concurrentMerge("b = t.b + 10")
      assertUpdateConflict(intercept[RuntimeException](merge.run(spark)))
      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 11)))
    }
  }

  test("retry plan retains the exact staged files outside the conflict") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val table = loadTable("t")
      val readSnapshot = table.latestSnapshot().get()
      val splits = table
        .newSnapshotReader()
        .withSnapshot(readSnapshot)
        .read()
        .splits()
        .asScala
        .collect { case split: DataSplit => split }
        .toSeq
      val firstRowIds =
        splits.flatMap(_.dataFiles().asScala).map(_.firstRowId().longValue()).distinct.sorted
      assert(firstRowIds.size == 2)
      val firstRowId = udf((rowId: Long) => firstRowIds.takeWhile(_ <= rowId).last)
      val rows = sql("SELECT b + 1 AS b, _ROW_ID FROM t")
        .withColumn("_FIRST_ROW_ID", firstRowId(col("_ROW_ID")))
        .select("b", "_FIRST_ROW_ID", "_ROW_ID")
      val staged = DataEvolutionPaimonWriter(table, splits).writePartialFields(rows, Seq("b"))
      val unaffectedFiles = staged
        .flatMap(
          _.asInstanceOf[CommitMessageImpl]
            .newFilesIncrement()
            .newFiles()
            .asScala)
        .filter(_.firstRowId() == firstRowIds.last)
        .map(_.fileName())
        .toSet

      concurrentMerge("b = t.b + 10")
      val plan = DataEvolutionMergeConflictRetry
        .plan(table, staged, readSnapshot.id(), table.latestSnapshot().get())
        .get

      val retainedFiles = plan.remainingMessages
        .flatMap(
          _.asInstanceOf[CommitMessageImpl]
            .newFilesIncrement()
            .newFiles()
            .asScala)
        .map(_.fileName())
        .toSet
      assert(unaffectedFiles.nonEmpty)
      assert(retainedFiles == unaffectedFiles)
      assert(
        plan.dataSplits.flatMap(_.dataFiles().asScala).map(_.firstRowId().longValue()).toSet ==
          Set(firstRowIds.head))
    }
  }

  test("retry plan rejects row ids removed by overwrite") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val table = loadTable("t")
      val readSnapshot = table.latestSnapshot().get()
      val splits = table
        .newSnapshotReader()
        .withSnapshot(readSnapshot)
        .read()
        .splits()
        .asScala
        .collect { case split: DataSplit => split }
        .toSeq
      val firstRowId = splits.head.dataFiles().get(0).firstRowId().longValue()
      val rows = sql("SELECT b + 1 AS b, _ROW_ID FROM t")
        .withColumn("_FIRST_ROW_ID", lit(firstRowId))
        .select("b", "_FIRST_ROW_ID", "_ROW_ID")
      val staged = DataEvolutionPaimonWriter(table, splits).writePartialFields(rows, Seq("b"))

      concurrentMerge("b = t.b + 10")
      sql("INSERT OVERWRITE t VALUES (1, 99, 'replaced')")

      Seq(false, true).foreach {
        includeConcurrentChanges =>
          assert(
            DataEvolutionMergeConflictRetry
              .plan(
                table,
                staged,
                readSnapshot.id(),
                table.latestSnapshot().get(),
                includeConcurrentChanges)
              .isEmpty)
      }
      checkAnswer(sql("SELECT b, status FROM t"), Seq(Row(99, "replaced")))
    }
  }

  test("merge retry checks every intervening append before advancing its snapshot") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id UNION ALL SELECT 2 AS id) s " +
          "ON t.id = s.id WHEN MATCHED THEN UPDATE SET b = t.b + 1")

      concurrentMerge("b = t.b + 10")
      sql(
        "MERGE INTO t USING (SELECT 2 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET b = t.b + 20").collect()
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 12), Row(2, 23)))
    }
  }

  test("merge retries concurrent updates after compaction changes the file boundaries") {
    withTable("t") {
      createTable("'compaction.min.file-num' = '2',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id UNION ALL SELECT 2 AS id) s " +
          "ON t.id = s.id WHEN MATCHED THEN UPDATE SET b = t.b + 1")

      concurrentMerge("b = t.b + 10")
      sql("CALL sys.compact(table => 't')").collect()
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 12), Row(2, 3)))
    }
  }

  test("merge retry reevaluates the ON condition") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id) s " +
          "ON t.id = s.id AND t.status = 'pending' " +
          "WHEN MATCHED THEN UPDATE SET b = t.b + 1")

      concurrentMerge("b = t.b + 10, status = 'cancelled'")
      merge.run(spark)

      checkAnswer(sql("SELECT b, status FROM t"), Seq(Row(11, "cancelled")))
    }
  }

  test("merge does not retry a nondeterministic source") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id, rand() AS delta) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET b = CAST(t.b + s.delta AS INT)")

      concurrentMerge("b = t.b + 10")
      assertUpdateConflict(intercept[RuntimeException](merge.run(spark)))
      checkAnswer(sql("SELECT b FROM t"), Seq(Row(11)))
    }
  }

  test("merge retries updates and inserts without retaining stale insert results") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id UNION ALL SELECT 2 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET b = t.b + 1 " +
          "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, 0, 'new')")

      concurrentMerge("b = t.b + 10")
      merge.run(spark)
      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 12), Row(2, 0)))
    }
  }

  for (persistSource <- Seq(false, true)) {
    test(
      s"upsert retries null-safe composite keys and COALESCE with source persist $persistSource") {
      withTable("t") {
        sql(
          "CREATE TABLE t (data_type STRING, clip_id STRING, data_revision INT, " +
            "collected_date STRING, quality_status STRING) PARTITIONED BY (data_type) " +
            "TBLPROPERTIES ('row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true', " +
            s"'data-evolution.merge-into.source-persist' = '$persistSource')")
        sql("INSERT INTO t VALUES ('video', 'clip1', NULL, 'old', 'pending')")
        val source = "(SELECT * FROM VALUES " +
          "('video', 'clip1', CAST(NULL AS INT), CAST(NULL AS STRING), 'source1'), " +
          "('audio', 'clip2', 2, CAST(NULL AS STRING), 'source2'), " +
          "('audio', 'clip3', 3, 'new-date', 'source3') " +
          "AS batch(data_type, clip_id, data_revision, collected_date, quality_status))"
        val merge = pinnedMerge(
          s"MERGE INTO t USING $source s ON t.data_type <=> s.data_type " +
            "AND t.clip_id <=> s.clip_id AND t.data_revision <=> s.data_revision " +
            "WHEN MATCHED THEN UPDATE SET " +
            "collected_date = COALESCE(s.collected_date, t.collected_date), " +
            "quality_status = COALESCE(s.quality_status, t.quality_status) " +
            "WHEN NOT MATCHED THEN INSERT *")

        sql(
          "MERGE INTO t USING (SELECT 'clip1' AS clip_id) s ON t.clip_id = s.clip_id " +
            "WHEN MATCHED THEN UPDATE SET collected_date = 'latest', quality_status = 'rival'")
        // A formerly unmatched source row now matches a file in a new partition. It must be
        // updated, rather than duplicated or silently discarded from both action branches.
        sql("INSERT INTO t VALUES ('audio', 'clip2', 2, 'rival-date', 'rival')")
        merge.run(spark)

        checkAnswer(
          sql("SELECT * FROM t"),
          Seq(
            Row("video", "clip1", null, "latest", "source1"),
            Row("audio", "clip2", 2, "rival-date", "source2"),
            Row("audio", "clip3", 3, "new-date", "source3"))
        )
      }
    }
  }

  test("upsert retry reevaluates a target key changed by a concurrent writer") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (3, 20, 'pending')")
      val merge = pinnedMerge(
        "MERGE INTO t USING (SELECT 1 AS id UNION ALL SELECT 3 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET b = t.b + 1 " +
          "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, 0, 'new')")

      concurrentMerge("b = t.b + 10")
      sql(
        "MERGE INTO t USING (SELECT 3 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET id = 99")
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 12), Row(99, 20), Row(3, 0)))
    }
  }

  for (
    (dependency, action, firstValue, secondValue) <- Seq(
      ("condition", "WHEN MATCHED AND t.status = 'pending' THEN UPDATE SET b = t.b + 1", 12, 2),
      ("assignment", "WHEN MATCHED THEN UPDATE SET b = t.b + length(t.status)", 18, 6)
    )
  ) {
    test(s"upsert retry refreshes a column read by the matched $dependency") {
      withTable("t") {
        createTable()
        sql("INSERT INTO t VALUES (1, 1, 'pending')")
        sql("INSERT INTO t VALUES (2, 2, 'pending')")
        val merge = pinnedMerge(
          "MERGE INTO t USING (SELECT id FROM VALUES (1), (2), (3) s(id)) s " +
            s"ON t.id = s.id $action " +
            "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, 0, 'new')")

        concurrentMerge("b = t.b + 10")
        sql(
          "MERGE INTO t USING (SELECT 2 AS id) s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET status = 'done'")
        merge.run(spark)

        checkAnswer(
          sql("SELECT id, b FROM t"),
          Seq(Row(1, firstValue), Row(2, secondValue), Row(3, 0)))
      }
    }
  }

  test("upsert retries preserve unaffected update files across repeated conflicts") {
    withTable("t") {
      createTable("'commit.max-retries' = '2',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val original = analyzedMerge(
        "MERGE INTO t USING (SELECT id FROM VALUES (1), (2), (3), (4) s(id)) s " +
          "ON t.id = s.id WHEN MATCHED THEN UPDATE SET b = t.b + 1 " +
          "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, 0, 'new')")
      val attempts = ArrayBuffer.empty[Seq[CommitMessage]]
      val merge = new MergeIntoPaimonDataEvolutionTable(
        original.v2Table,
        original.targetTable,
        original.sourceTable,
        original.matchedCondition,
        original.matchedActions,
        original.notMatchedActions,
        original.notMatchedBySourceActions
      ) {
        override lazy val writer: PaimonSparkWriter = new InterleavingMergeWriter(
          table,
          messages => {
            attempts += messages
            attempts.size match {
              case 1 =>
                concurrentMerge("b = t.b + 10")
                sql(
                  "MERGE INTO t USING (SELECT 2 AS id) s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET status = 'rival'")
              case 2 =>
                concurrentMerge("b = t.b + 100")
                sql("INSERT INTO t VALUES (3, 30, 'rival')")
              case _ =>
            }
          }
        )
      }
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 112), Row(2, 3), Row(3, 31), Row(4, 0)))
      checkAnswer(sql("SELECT status FROM t WHERE id = 2"), Seq(Row("rival")))
      assert(attempts.size == 3)
      def files(messages: Seq[CommitMessage]) =
        messages.flatMap(_.asInstanceOf[CommitMessageImpl].newFilesIncrement().newFiles().asScala)
      val retainedNames = attempts.map(
        messages =>
          files(messages)
            .filter(file => Option(file.firstRowId()).exists(_.longValue() == 1L))
            .map(_.fileName())
            .toSet)
      assert(retainedNames.head.nonEmpty)
      assert(retainedNames.distinct.size == 1)
      val insertNames = attempts.map(
        messages =>
          files(messages)
            .filter(_.firstRowId() == null)
            .map(_.fileName())
            .toSet)
      assert(insertNames.forall(_.nonEmpty))
      assert(insertNames.flatten.size == insertNames.flatten.toSet.size)
      val liveNames = loadTable("t")
        .newSnapshotReader()
        .read()
        .splits()
        .asScala
        .collect { case split: DataSplit => split }
        .flatMap(_.dataFiles().asScala)
        .map(_.fileName())
        .toSet
      assert(insertNames.last.subsetOf(liveNames))
      assert(insertNames.dropRight(1).flatten.forall(name => !liveNames.contains(name)))
      val table = loadTable("t")
      val abandonedPaths = attempts
        .dropRight(1)
        .flatMap(_.flatMap {
          message =>
            val factory = table
              .store()
              .pathFactory()
              .createDataFilePathFactory(message.partition(), message.bucket())
            files(Seq(message))
              .filter(_.firstRowId() == null)
              .flatMap(_.collectFiles(factory).asScala)
        })
      assert(abandonedPaths.nonEmpty)
      assert(abandonedPaths.forall(path => !table.fileIO().exists(path)))
    }
  }

  private def createTable(extraOptions: String = ""): Unit = {
    sql(
      "CREATE TABLE t (id INT, b INT, status STRING) TBLPROPERTIES (" +
        extraOptions + "'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
  }

  private def pinnedMerge(statement: String): MergeIntoPaimonDataEvolutionTable = {
    val command = analyzedMerge(statement)
    // Resolve the same pinned target used by execution before the competing APPEND commits.
    command.table
    command
  }

  private def analyzedMerge(statement: String): MergeIntoPaimonDataEvolutionTable = {
    val parsed = spark.sessionState.sqlParser.parsePlan(statement)
    spark.sessionState.analyzer
      .executeAndCheck(parsed, new QueryPlanningTracker)
      .asInstanceOf[MergeIntoPaimonDataEvolutionTable]
  }

  private def concurrentMerge(assignments: String): Unit = {
    sql(
      "MERGE INTO t USING (SELECT 1 AS id) s ON t.id = s.id " +
        "WHEN MATCHED THEN UPDATE SET " + assignments).collect()
  }

  private def assertUpdateConflict(error: Throwable): Unit = {
    val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null)
    assert(
      causes.exists(
        cause =>
          Option(cause.getMessage).exists(
            _.contains(ErrorMessages.DATA_EVOLUTION_ROW_ID_CONFLICT_MESSAGE))))
  }
}

// The callback is driver-only. Keeping it transient avoids serializing Spark's catalog when
// the writer's normal write closures are sent to executors.
private class InterleavingMergeWriter(
    fileStoreTable: FileStoreTable,
    @transient private val beforeCommit: Seq[CommitMessage] => Unit)
  extends PaimonSparkWriter(fileStoreTable) {

  override def commit(messages: Seq[CommitMessage], operation: Snapshot.Operation): Unit = {
    beforeCommit(messages)
    super.commit(messages, operation)
  }
}
