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
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.DateType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class DataEvolutionMergeRetryTest extends PaimonSparkTestBase {

  test("merge retries an append update conflict using the latest target values") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = pinnedMerge(
        upsert("(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 30), (3, 4) batch(id, b))"))

      concurrentMerge("b = t.b + 10")
      assert(loadTable("t").latestSnapshot().get().commitKind() == CommitKind.APPEND)
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 11), Row(2, 30), Row(3, 4)))
    }
  }

  test("merge retry honors the commit retry limit") {
    withTable("t") {
      createTable("'commit.max-retries' = '0',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge =
        pinnedMerge(upsert("(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 0) batch(id, b))"))

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
        .plan(
          table,
          staged,
          readSnapshot.id(),
          table.latestSnapshot().get(),
          Set(table.rowType().getField("id").id()))
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

      assert(
        DataEvolutionMergeConflictRetry
          .plan(
            table,
            staged,
            readSnapshot.id(),
            table.latestSnapshot().get(),
            Set(table.rowType().getField("id").id()))
          .isEmpty)
      checkAnswer(sql("SELECT b, status FROM t"), Seq(Row(99, "replaced")))
    }
  }

  test("merge retry checks every intervening append before advancing its snapshot") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = pinnedMerge(
        upsert(
          "(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, CAST(NULL AS INT)) batch(id, b))"))

      concurrentMerge("b = t.b + 10")
      sql(
        "MERGE INTO t USING (SELECT 2 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET b = t.b + 20").collect()
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 11), Row(2, 22)))
    }
  }

  test("merge retries concurrent updates after compaction changes the file boundaries") {
    withTable("t") {
      createTable("'compaction.min.file-num' = '2',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = pinnedMerge(
        upsert(
          "(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, CAST(NULL AS INT)) batch(id, b))"))

      concurrentMerge("b = t.b + 10")
      sql("CALL sys.compact(table => 't')").collect()
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 11), Row(2, 2)))
    }
  }

  test("merge does not retry a nondeterministic source") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(upsert("(SELECT 1 AS id, CAST(rand() * 100 AS INT) AS b)"))

      concurrentMerge("b = t.b + 10")
      assertUpdateConflict(intercept[RuntimeException](merge.run(spark)))
      checkAnswer(sql("SELECT b FROM t"), Seq(Row(11)))
    }
  }

  for (
    (name, on, matched, insert) <- Seq(
      (
        "ordinary equality",
        "t.id = s.id",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        true),
      (
        "extra predicate",
        "t.id <=> s.id AND t.status = 'pending'",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        true),
      (
        "conditional update",
        "t.id <=> s.id",
        "WHEN MATCHED AND t.status = 'pending' THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        true),
      (
        "wrong fallback",
        "t.id <=> s.id",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.id)",
        true),
      (
        "insert expression",
        "t.id <=> s.id",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        true),
      (
        "missing insert",
        "t.id <=> s.id",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        false),
      (
        "key update",
        "t.id <=> s.id",
        "WHEN MATCHED THEN UPDATE SET id = COALESCE(s.delta, t.id), b = COALESCE(s.delta, t.b)",
        true),
      (
        "source value missing from insert",
        "t.id <=> s.id",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        true),
      ("target arithmetic", "t.id <=> s.id", "WHEN MATCHED THEN UPDATE SET b = t.b + 1", true),
      (
        "value mapped to another column",
        "t.id <=> s.id",
        "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b)",
        true)
    )
  ) {
    test(s"retry rejects noncanonical upsert: $name") {
      withTable("t") {
        createTable()
        sql("INSERT INTO t VALUES (1, 1, 'pending')")
        val insertValue = name match {
          case "insert expression" => "s.delta + 1"
          case "source value missing from insert" | "value mapped to another column" => "0"
          case _ => "s.delta"
        }
        val statusValue = if (name == "value mapped to another column") {
          "CAST(s.delta AS STRING)"
        } else "'new'"
        val insertClause = if (insert) {
          s"WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, $insertValue, $statusValue)"
        } else ""
        var attempts = 0
        val merge = interleavedMerge(
          "MERGE INTO t USING (SELECT 1 AS id, CAST(NULL AS INT) AS delta) s " +
            s"ON $on $matched $insertClause",
          _ => {
            attempts += 1
            concurrentMerge("b = t.b + 10")
          }
        )
        assertUpdateConflict(intercept[RuntimeException](merge.run(spark)))
        checkAnswer(sql("SELECT b FROM t"), Seq(Row(11)))
        assert(attempts == 1)
      }
    }
  }

  test("retry supports optimized COALESCE with a nonnullable source") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      var attempts = 0
      val merge = interleavedMerge(
        "MERGE INTO t USING (SELECT 1 AS id, 10 AS b) s ON t.id <=> s.id " +
          "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.b, t.b) " +
          "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, s.b, 'new')",
        _ => {
          attempts += 1
          if (attempts == 1) concurrentMerge("b = t.b + 100")
        },
        optimize = true
      )
      merge.run(spark)
      checkAnswer(sql("SELECT b FROM t"), Seq(Row(10)))
      assert(attempts == 2)
    }
  }

  for (notNull <- Seq(false, true)) {
    test(s"retry supports optimized STRING to DATE COALESCE with not-null=$notNull") {
      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        withTable("t") {
          val nullability = if (notNull) " NOT NULL" else ""
          sql(
            s"CREATE TABLE t (id INT, collected_date DATE$nullability) TBLPROPERTIES (" +
              "'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true')")
          sql("INSERT INTO t VALUES (1, DATE '2026-01-01')")
          var attempts = 0
          val merge = interleavedMerge(
            "MERGE INTO t USING (SELECT id, '2026-11-03' AS collected_date " +
              "FROM VALUES (1), (2) s(id)) s ON t.id <=> s.id " +
              "WHEN MATCHED THEN UPDATE SET " +
              "collected_date = COALESCE(s.collected_date, t.collected_date) " +
              "WHEN NOT MATCHED THEN INSERT (id, collected_date) VALUES (s.id, s.collected_date)",
            _ => {
              attempts += 1
              if (attempts == 1) concurrentMerge("collected_date = DATE '2026-02-02'")
            },
            optimize = true
          )
          val casts = merge.matchedActions.flatMap(_.collect {
            case cast: Cast if cast.dataType == DateType => cast
          })
          assert(casts.exists(
            cast =>
              cast.nullable && cast.child.isInstanceOf[AttributeReference] && !cast.child.nullable))
          merge.run(spark)
          checkAnswer(
            sql("SELECT id, CAST(collected_date AS STRING) FROM t"),
            Seq(Row(1, "2026-11-03"), Row(2, "2026-11-03")))
          assert(attempts == 2)
        }
      }
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
        upsert(
          "(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (3, CAST(NULL AS INT)) batch(id, b))"))

      concurrentMerge("b = t.b + 10")
      sql(
        "MERGE INTO t USING (SELECT 3 AS id) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET id = 99")
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 11), Row(99, 20), Row(3, null)))
    }
  }

  for (persistSource <- Seq(false, true)) {
    test(s"COALESCE upsert keeps one source snapshot with source persist $persistSource") {
      withTable("t", "src") {
        createTable(s"'data-evolution.merge-into.source-persist' = '$persistSource',")
        sql("INSERT INTO t VALUES (1, 1, 'pending')")
        sql("INSERT INTO t VALUES (2, 2, 'pending')")
        sql("CREATE TABLE src (id INT, delta INT)")
        sql("INSERT INTO src VALUES (1, 10), (2, 10), (3, 10)")
        var attempts = 0
        val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
        val merge = interleavedMerge(
          "MERGE INTO t USING src s ON t.id <=> s.id " +
            "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b) " +
            "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, s.delta, 'new')",
          _ => {
            attempts += 1
            if (attempts == 1) {
              sql("INSERT OVERWRITE src VALUES (1, 20), (2, 20), (4, 20)")
              concurrentMerge("b = t.b + 100")
            }
          }
        )
        merge.run(spark)

        checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 10), Row(2, 10), Row(3, 10)))
        assert(attempts == 2)
        assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
      }
    }
  }

  test("failed retry insert staging keeps its original error and releases cached source") {
    withTable("t") {
      createTable("'data-evolution.merge-into.source-persist' = 'true',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val merge = interleavedMerge(
        upsert("(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 3), (3, 0) batch(id, b))"),
        _ => concurrentMerge("b = t.b + 100"),
        failInsertAttempt = 2
      )

      val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
      val error = intercept[RuntimeException](merge.run(spark))
      assert(error.getMessage == "retry insert staging failed")
      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 101), Row(2, 2)))
      assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
    }
  }

  test("a failure after publication does not delete committed files") {
    withTable("t") {
      createTable("'data-evolution.merge-into.source-persist' = 'true',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      var attempts = 0
      val merge = interleavedMerge(
        upsert("(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 0) batch(id, b))"),
        _ => {
          attempts += 1
          if (attempts == 1) concurrentMerge("b = t.b + 100")
        },
        afterCommit = () => throw new RuntimeException("publication response failed")
      )

      val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
      val error = intercept[RuntimeException](merge.run(spark))
      assert(error.getMessage == "publication response failed")
      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 101), Row(2, 0)))
      assert(attempts == 2)
      assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
    }
  }

  test("COALESCE upsert source checkpoint prunes unused columns") {
    withTable("t", "src") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("CREATE TABLE src (id INT)")
      sql("INSERT INTO src VALUES (1)")
      spark.udf.register(
        "unused_retry_source",
        (id: Int) => {
          if (id >= 0) throw new IllegalStateException("unused source column evaluated") else id
        })
      try {
        var attempts = 0
        val merge = interleavedMerge(
          "MERGE INTO t USING (SELECT id, CAST(NULL AS INT) AS delta, " +
            "unused_retry_source(id) AS unused FROM src) s ON t.id <=> s.id " +
            "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b) " +
            "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, s.delta, 'new')",
          _ => {
            attempts += 1
            if (attempts == 1) concurrentMerge("b = t.b + 100")
          }
        )
        val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
        merge.run(spark)
        checkAnswer(sql("SELECT b FROM t"), Seq(Row(101)))
        assert(attempts == 2)
        assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS unused_retry_source")
      }
    }
  }

  test("complex source references retain nested pruning and the original conflict behavior") {
    withTable("t", "src") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("CREATE TABLE src (id INT)")
      sql("INSERT INTO src VALUES (1)")
      spark.udf.register(
        "unused_nested_retry_source",
        (id: Int) => {
          if (id >= 0) throw new IllegalStateException("unused nested source evaluated") else id
        })
      try {
        val statement =
          "MERGE INTO t USING (SELECT named_struct('id', id, " +
            "'unused', unused_nested_retry_source(id)) AS k FROM src) s " +
            "ON t.id = s.k.id WHEN MATCHED THEN UPDATE SET b = t.b + 1"
        val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
        analyzedMerge(statement).run(spark)
        checkAnswer(sql("SELECT b FROM t"), Seq(Row(2)))
        var attempts = 0
        val merge = interleavedMerge(
          statement,
          _ => {
            attempts += 1
            concurrentMerge("b = t.b + 100")
          })
        val error = intercept[RuntimeException](merge.run(spark))
        assert(DataEvolutionMergeConflictRetry.isUpdateConflict(error))
        assert(attempts == 1)
        checkAnswer(sql("SELECT b FROM t"), Seq(Row(102)))
        assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS unused_nested_retry_source")
      }
    }
  }

  test("source materialization failure releases its checkpoint before any target write") {
    withTable("t", "src") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("CREATE TABLE src (id INT)")
      sql("INSERT INTO src VALUES (1)")
      spark.udf.register(
        "fail_retry_source",
        (id: Int) => {
          if (id >= 0) throw new IllegalStateException("source materialization failed") else id
        })
      try {
        val merge = analyzedMerge(
          "MERGE INTO t USING (SELECT id, fail_retry_source(id) AS delta FROM src) s " +
            "ON t.id <=> s.id WHEN MATCHED THEN UPDATE SET b = COALESCE(s.delta, t.b) " +
            "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, s.delta, 'new')")
        val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
        val error = intercept[Exception](merge.run(spark))
        assert(
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(
              cause =>
                Option(cause.getMessage).exists(_.contains("source materialization failed"))))
        assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
        checkAnswer(sql("SELECT b FROM t"), Seq(Row(1)))
        assertOnlyLiveDataFiles()
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS fail_retry_source")
      }
    }
  }

  test("upsert retries reuse unaffected files and leave superseded files intact") {
    withTable("t") {
      createTable("'commit.max-retries' = '2',")
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (2, 2, 'pending')")
      val attempts = ArrayBuffer.empty[Seq[CommitMessage]]
      val merge = interleavedMerge(
        upsert(
          "(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 3), " +
            "(3, CAST(NULL AS INT)), (4, 0) batch(id, b))"),
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
      merge.run(spark)

      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 111), Row(2, 3), Row(3, 30), Row(4, 0)))
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
              .filter(file => !liveNames.contains(file.fileName()))
              .flatMap(_.collectFiles(factory).asScala)
        })
      assert(abandonedPaths.nonEmpty)
      assert(abandonedPaths.forall(table.fileIO().exists))
    }
  }

  test("upsert retry supports typed literal insert defaults") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      val merge = pinnedMerge(
        upsert("(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 20) batch(id, b))")
          .replace("'new'", "CAST(NULL AS STRING)"))
      concurrentMerge("b = t.b + 10")
      merge.run(spark)
      checkAnswer(sql("SELECT id, b, status FROM t"), Seq(Row(1, 11, "pending"), Row(2, 20, null)))
    }
  }

  test("upsert retry finds a previously unmatched group whose key changed") {
    withTable("t") {
      createTable()
      sql("INSERT INTO t VALUES (1, 1, 'pending')")
      sql("INSERT INTO t VALUES (99, 99, 'pending')")
      var attempts = 0
      val merge = interleavedMerge(
        upsert("(SELECT * FROM VALUES (1, CAST(NULL AS INT)), (2, 20) batch(id, b))"),
        messages => {
          attempts += 1
          if (attempts == 1) {
            val updated = messages
              .flatMap(_.asInstanceOf[CommitMessageImpl].newFilesIncrement().newFiles().asScala)
              .flatMap(file => Option(file.firstRowId()).map(_.longValue()))
            assert(updated.toSet == Set(0L))
            concurrentMerge("b = t.b + 10")
            sql(
              "MERGE INTO t USING (SELECT 99 AS id) s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET id = 2")
          }
        }
      )
      merge.run(spark)
      checkAnswer(sql("SELECT id, b FROM t"), Seq(Row(1, 11), Row(2, 20)))
      assert(attempts == 2)
    }
  }

  for (nestedFields <- Seq(false, true)) {
    test(s"upsert retry replaces whole complex columns with nested fields $nestedFields") {
      withTable("t", "src") {
        val schema = "(id INT, payload STRUCT<x: INT, tags: ARRAY<STRING>>, " +
          "items ARRAY<STRUCT<x: INT>>, attrs MAP<STRING, STRUCT<x: INT>>)"
        sql(
          s"CREATE TABLE t $schema TBLPROPERTIES (" +
            "'row-tracking.enabled' = 'true', 'data-evolution.enabled' = 'true', " +
            s"'data-evolution.nested-field.enabled' = '$nestedFields')")
        sql(s"CREATE TABLE src $schema")
        def values(id: Int, x: Int, tag: String): String =
          s"($id, named_struct('x', $x, 'tags', array('$tag')), " +
            s"array(named_struct('x', $x)), map('$tag', named_struct('x', $x)))"
        sql(s"INSERT INTO t VALUES (1, NULL, NULL, NULL), ${values(2, 2, "old")}")
        sql(
          "INSERT INTO src VALUES (1, NULL, NULL, NULL), " +
            s"${values(2, 20, "source")}, ${values(3, 30, "source")}, (4, NULL, NULL, NULL)")
        var attempts = 0
        val merge = interleavedMerge(
          "MERGE INTO t USING src s ON t.id <=> s.id " +
            "WHEN MATCHED THEN UPDATE SET payload = COALESCE(s.payload, t.payload), " +
            "items = COALESCE(s.items, t.items), attrs = COALESCE(s.attrs, t.attrs) " +
            "WHEN NOT MATCHED THEN INSERT *",
          _ => {
            attempts += 1
            if (attempts == 1) {
              sql(
                s"INSERT OVERWRITE src VALUES ${values(1, 2000, "changed")}, " +
                  s"${values(2, 2000, "changed")}, ${values(5, 2000, "changed")}")
              concurrentMerge(
                "payload = named_struct('x', 101, 'tags', array('rival')), " +
                  "items = array(named_struct('x', 101)), " +
                  "attrs = map('rival', named_struct('x', 101))")
            }
          }
        )
        val persistedBefore = spark.sparkContext.getPersistentRDDs.keySet
        merge.run(spark)
        def expected(id: Int, x: Int, tag: String): Row =
          Row(id, s"""{"x":$x,"tags":["$tag"]}""", s"""[{"x":$x}]""", s"""{"$tag":{"x":$x}}""")
        checkAnswer(
          sql("SELECT id, to_json(payload), to_json(items), to_json(attrs) FROM t"),
          Seq(
            expected(1, 101, "rival"),
            expected(2, 20, "source"),
            expected(3, 30, "source"),
            Row(4, null, null, null))
        )
        assert(attempts == 2)
        assert(spark.sparkContext.getPersistentRDDs.keySet.diff(persistedBefore).isEmpty)
      }
    }
  }

  private def upsert(source: String): String =
    s"MERGE INTO t USING $source s ON t.id <=> s.id " +
      "WHEN MATCHED THEN UPDATE SET b = COALESCE(s.b, t.b) " +
      "WHEN NOT MATCHED THEN INSERT (id, b, status) VALUES (s.id, s.b, 'new')"

  private def interleavedMerge(
      statement: String,
      beforeCommit: Seq[CommitMessage] => Unit,
      failInsertAttempt: Int = 0,
      afterCommit: () => Unit = () => (),
      optimize: Boolean = false): MergeIntoPaimonDataEvolutionTable = {
    val analyzed = analyzedMerge(statement)
    val original =
      if (optimize)
        spark.sessionState.optimizer
          .execute(analyzed)
          .asInstanceOf[MergeIntoPaimonDataEvolutionTable]
      else analyzed
    new MergeIntoPaimonDataEvolutionTable(
      original.v2Table,
      original.targetTable,
      original.sourceTable,
      original.matchedCondition,
      original.matchedActions,
      original.notMatchedActions,
      original.notMatchedBySourceActions,
      upsertKeyColumns = original.upsertKeyColumns
    ) {
      override lazy val writer: PaimonSparkWriter =
        new InterleavingMergeWriter(table, beforeCommit, failInsertAttempt, afterCommit)
    }
  }

  private def assertOnlyLiveDataFiles(): Unit = {
    val table = loadTable("t")
    val splits = table
      .newSnapshotReader()
      .read()
      .splits()
      .asScala
      .collect { case split: DataSplit => split }
    val live = splits.flatMap(_.dataFiles().asScala).map(_.fileName()).toSet
    val physical = splits
      .map(_.bucketPath())
      .distinct
      .flatMap {
        bucket =>
          table
            .fileIO()
            .listStatus(new Path(bucket))
            .toSeq
            .filter(file => !file.isDir() && file.getPath.getName.startsWith("data-"))
            .map(_.getPath.getName)
      }
      .toSet
    assert(physical == live)
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
    @transient private val beforeCommit: Seq[CommitMessage] => Unit,
    private val failInsertAttempt: Int = 0,
    @transient private val afterCommit: () => Unit = () => ())
  extends PaimonSparkWriter(fileStoreTable) {

  @transient private var insertAttempt = 0

  override def write(data: DataFrame): Seq[CommitMessage] = {
    insertAttempt += 1
    if (insertAttempt == failInsertAttempt) {
      throw new RuntimeException("retry insert staging failed")
    }
    super.write(data)
  }

  override def commit(messages: Seq[CommitMessage], operation: Snapshot.Operation): Unit = {
    beforeCommit(messages)
    super.commit(messages, operation)
    afterCommit()
  }
}
