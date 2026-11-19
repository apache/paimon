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

import org.apache.paimon.Snapshot.CommitKind
import org.apache.paimon.append.dataevolution.DataEvolutionRangeSortPlan
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.data.{GenericRow, Timestamp}
import org.apache.paimon.fs.Path
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitCallback, CommitMessage, CommitMessageImpl}
import org.apache.paimon.types.DataTypes
import org.apache.paimon.utils.Range

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThatThrownBy

import scala.collection.JavaConverters._

class SortDataEvolutionProcedureTest extends PaimonSparkTestBase {

  test("a post-commit sort failure refreshes cached row IDs") {
    withTable("T") {
      val callback = classOf[FailingRangeSortCallback].getName
      sql(s"""CREATE TABLE T (id INT) TBLPROPERTIES (
             |'data-evolution.enabled'='true', 'row-tracking.enabled'='true',
             |'write-only'='true', 'commit.callbacks'='$callback')""".stripMargin)
      sql("INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES (2),(1) AS s(id)")
      def rowIds = sql("SELECT _ROW_ID, id FROM T")
      val cached = rowIds.cache()
      try {
        checkAnswer(cached, Seq(Row(0L, 2), Row(1L, 1)))
        assertThatThrownBy(() => sql("CALL sys.sort_data_evolution('T', '[0,1]', 'id')").collect())
          .hasStackTraceContaining("File deletion conflicts detected")
        val table = loadTable("T")
        assert(table.latestSnapshot().get().id() == 2L)
        val files = table.newSnapshotReader().read().dataSplits().asScala.flatMap {
          split =>
            split.dataFiles().asScala.map(file => new Path(split.bucketPath(), file.fileName()))
        }
        assert(files.nonEmpty && files.forall(table.fileIO().exists))
        checkAnswer(rowIds, Seq(Row(2L, 1), Row(3L, 2)))
      } finally {
        cached.unpersist()
      }
    }
  }

  test("a rejected range sort removes its staged files and keeps the winning sort") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, payload BINARY, embedding ARRAY<FLOAT>) TBLPROPERTIES (
            |'data-evolution.enabled'='true', 'row-tracking.enabled'='true', 'write-only'='true',
            |'blob-field'='payload', 'vector-field'='embedding', 'field.embedding.vector-dim'='1',
            |'vector.file.format'='parquet')""".stripMargin)
      sql("""INSERT INTO T SELECT /*+ COALESCE(1) */ id, CAST(CAST(id AS STRING) AS BINARY),
            |array(CAST(id AS FLOAT)) FROM VALUES (2),(1) AS s(id)""".stripMargin)
      val table = loadTable("T")
      val snapshot = table.latestSnapshot().get()
      val fields = Seq("id").asJava
      val plans = DataEvolutionRangeSortPlan.plan(table, snapshot, new Range(0, 1), null, fields)
      def bucketFiles: Set[String] =
        table
          .fileIO()
          .listStatus(new Path(table.location(), "bucket-0"))
          .map(_.getPath.getName)
          .toSet

      withSQLConf("spark.sql.shuffle.partitions" -> "2") {
        checkAnswer(sql("CALL sys.sort_data_evolution('T', '[0,1]', 'id')"), Row(1) :: Nil)
        val committed = table.latestSnapshot().get().id()
        val files = bucketFiles
        assert(files.exists(_.endsWith(".blob")) && files.exists(_.contains(".vector.")))
        assertThatThrownBy(
          () => SparkDataEvolutionRangeSort.execute(table, snapshot, fields, plans, spark))
          .hasStackTraceContaining("File deletion conflicts detected")
        assert(table.latestSnapshot().get().id() == committed)
        assert(bucketFiles == files)
        checkAnswer(
          sql("SELECT _ROW_ID, _SEQUENCE_NUMBER, id, CAST(payload AS STRING), embedding[0] FROM T"),
          Seq(Row(2L, committed, 1, "1", 1f), Row(3L, committed, 2, "2", 2f))
        )
      }
    }
  }

  test("range sort preserves nanoseconds and signed zeros in sort keys") {
    withTable("T") {
      val schema = Schema
        .newBuilder()
        .column("id", DataTypes.INT())
        .column("ts", DataTypes.TIMESTAMP(9))
        .column("f", DataTypes.FLOAT())
        .column("d", DataTypes.DOUBLE())
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .build()
      paimonCatalog.createTable(Identifier.create(dbName0, "T"), schema, false)
      val builder = loadTable("T").newBatchWriteBuilder()
      val write = builder.newWrite()
      val commit = builder.newCommit()
      try {
        // Write with the Paimon API so that the input retains sub-microsecond precision.
        val values = Seq((200, -1f, -1d), (100, 0f, 0d), (100, -0.0f, 0d), (100, -0.0f, -0.0d))
        values.zipWithIndex.foreach {
          case ((nanos, f, d), id) =>
            write.write(
              GenericRow
                .of(Int.box(id), Timestamp.fromEpochMillis(0, nanos), Float.box(f), Double.box(d)))
        }
        commit.commit(write.prepareCommit())
      } finally {
        write.close()
        commit.close()
      }
      withSQLConf("spark.sql.shuffle.partitions" -> "2") {
        checkAnswer(sql("CALL sys.sort_data_evolution('T', '[0,3]', 'ts,f,d')"), Row(1) :: Nil)
      }
      assert(
        sql("SELECT id FROM T ORDER BY _ROW_ID")
          .collect()
          .toList == List(Row(3), Row(2), Row(1), Row(0)))
    }
  }

  test("sort preserves disjoint global index files in the same partition") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT) TBLPROPERTIES (
            |'row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'write-only'='true')
            |""".stripMargin)
      def buildIndex(): Unit = {
        sql(
          "CALL sys.create_global_index(table => 'test.T', index_column => 'id', index_type => 'bitmap')")
          .collect()
      }
      def indexes: Seq[Row] = sql(
        "SELECT file_name, row_range_start, row_range_end FROM `T$table_indexes` " +
          "WHERE index_type = 'bitmap' ORDER BY row_range_start").collect().toSeq
      sql("INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES (2), (1) AS r(id)")
      buildIndex()
      sql("INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES (4), (3) AS r(id)")
      buildIndex()
      val before = indexes
      assert(before.map(r => (r.getLong(1), r.getLong(2))) == Seq((0L, 1L), (2L, 3L)))
      checkAnswer(
        sql("CALL sys.sort_data_evolution(table => 'T', `range` => '[0, 1]', order_by => 'id')"),
        Row(1) :: Nil)
      assert(indexes == before.tail)
    }
  }

  Seq(1, 2).foreach {
    workers =>
      test(s"rewrite multiple partitions including a fully deleted input with $workers writers") {
        checkPartitionReplacements(workers)
      }
      test(s"sort complete groups and rewrite aligned payloads with $workers shuffle partitions") {
        checkAlignedRewrite(workers)
      }
  }

  private def checkPartitionReplacements(workers: Int): Unit = {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, p INT) PARTITIONED BY (p) TBLPROPERTIES (
            |'data-evolution.enabled'='true', 'row-tracking.enabled'='true',
            |'deletion-vectors.enabled'='true', 'write-only'='true')""".stripMargin)
      sql(
        "INSERT INTO T SELECT /*+ COALESCE(1) */ * FROM VALUES (4,0),(2,0),(3,1),(1,1),(100,2),(101,2) AS s(id,p)")
      sql("DELETE FROM T WHERE id >= 100")
      assert(sql("SELECT * FROM `T$row_ranges` WHERE partition = '{2}'").collect().nonEmpty)
      val snapshot = loadTable("T").latestSnapshot().get().id()
      withSQLConf("spark.sql.shuffle.partitions" -> workers.toString) {
        checkAnswer(sql("CALL sys.sort_data_evolution('T', '[0,5]', 'id')"), Row(3) :: Nil)
      }
      assert(loadTable("T").latestSnapshot().get().id() == snapshot + 1)
      assert(
        sql("SELECT p, id FROM T ORDER BY p, _ROW_ID").collect().toList ==
          List(Row(0, 2), Row(0, 4), Row(1, 1), Row(1, 3)))
      assert(
        sql("SELECT * FROM `T$row_ranges` WHERE start_row_id < 6 OR partition = '{2}'")
          .collect()
          .isEmpty)
    }
  }

  private def checkAlignedRewrite(workers: Int): Unit = {
    withTable("T") {
      sql(
        """
          |CREATE TABLE T (id INT, k INT, priority INT, payload BINARY, embedding ARRAY<FLOAT>, p INT)
          |PARTITIONED BY (p) TBLPROPERTIES (
          | 'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
          | 'deletion-vectors.enabled'='true', 'blob-field'='payload',
          | 'vector-field'='embedding', 'field.embedding.vector-dim'='2',
          | 'vector.file.format'='parquet', 'write-only'='true')
          |""".stripMargin)
      def insert(start: Int, end: Int, p: Int): Unit = {
        sql(s"""INSERT INTO T SELECT /*+ COALESCE(1) */
               | CAST(id AS INT), IF(id = 2, NULL, CAST(id % 2 AS INT)), IF(id = 9, 0, 1),
               | IF(id = 8, NULL, CAST(concat('blob-', id) AS BINARY)),
               | IF(id = 9, NULL, array(CAST(id AS FLOAT), CAST(-id AS FLOAT))), $p
               | FROM range($start, $end, 1, 1)
               |""".stripMargin)
      }
      insert(0, 4, 0)
      insert(4, 7, 1)
      insert(7, 10, 0)
      insert(10, 12, 0)
      sql("UPDATE T SET k = 7 WHERE id = 1 AND p = 0")
      sql("DELETE FROM T WHERE id = 3 AND p = 0")

      val columns = "id, k, priority, cast(payload AS STRING), embedding[0], embedding[1], p"
      val target = "p = 0 AND _ROW_ID BETWEEN 0 AND 9"
      def rows(query: String): Seq[Row] = sql(query).collect().toSeq
      val expected = rows(s"SELECT $columns FROM T WHERE $target ORDER BY k, priority, _ROW_ID")
      assert(expected.map(_.getInt(0)) == Seq(2, 0, 8, 9, 7, 1))
      val untouched = rows(s"SELECT _ROW_ID, $columns FROM T WHERE NOT ($target) ORDER BY _ROW_ID")
      def files = loadTable("T")
        .newSnapshotReader()
        .read()
        .dataSplits()
        .asScala
        .flatMap(s => s.dataFiles().asScala.map(f => (s.partition().getInt(0), f)))
        .toVector
      val (inside, outside) = files.partition {
        case (p, f) => p == 0 && f.nonNullFirstRowId() < 10
      }
      val oldNames = inside.map(_._2.fileName()).toSet
      assert(oldNames.exists(_.endsWith(".blob")) && oldNames.exists(_.contains(".vector.")))
      val before = loadTable("T").latestSnapshot().get().id()
      val error = intercept[Exception] {
        sql("CALL sys.sort_data_evolution('T', '[1,9]', 'k', 'p = 0')").collect()
      }
      assert(
        Iterator
          .iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .exists(e => Option(e.getMessage).exists(_.contains("complete file groups"))))
      assert(loadTable("T").latestSnapshot().get().id() == before)

      val ranges = rows(
        "SELECT start_row_id, end_row_id FROM `T$row_ranges` " +
          "WHERE partition = '{0}' AND end_row_id < 10 ORDER BY start_row_id")
      assert(ranges == Seq(Row(0L, 3L), Row(7L, 9L)))
      val start = ranges.head.getLong(0)
      val end = ranges.last.getLong(1)
      val adaptiveInitialPartitions = if (workers == 1) "2" else "1"
      withSQLConf(
        "spark.sql.shuffle.partitions" -> workers.toString,
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> adaptiveInitialPartitions
      ) {
        checkAnswer(
          sql(s"CALL sys.sort_data_evolution('T', '[$start,$end]', 'k,priority', 'p = 0')"),
          Row(1) :: Nil)
      }
      // Six tiny rows fit in a single file per reducer.
      assert(
        rows("SELECT start_row_id FROM `T$row_ranges` WHERE start_row_id >= 12").size == workers)
      assert(rows(s"SELECT $columns FROM T WHERE _ROW_ID >= 12 ORDER BY _ROW_ID") == expected)
      assert(
        rows(s"SELECT _ROW_ID, $columns FROM T WHERE _ROW_ID < 12 ORDER BY _ROW_ID") == untouched)
      val now = files.map(_._2.fileName()).toSet
      assert(now.intersect(oldNames).isEmpty)
      assert(outside.forall(f => now.contains(f._2.fileName())))
      assert(now.diff(outside.map(_._2.fileName()).toSet).exists(_.endsWith(".blob")))
      assert(now.diff(outside.map(_._2.fileName()).toSet).exists(_.contains(".vector.")))
      checkAnswer(sql("CALL sys.sort_data_evolution('T', '[0,9]', 'k', 'p = 0')"), Row(0) :: Nil)
      assert(loadTable("T").latestSnapshot().get().id() == before + 1)
    }
  }
}

class FailingRangeSortCallback extends CommitCallback {

  private var table: FileStoreTable = _

  override def setTable(table: FileStoreTable): Unit = this.table = table

  override def call(context: CommitCallback.Context): Unit = {
    if (context.snapshot.commitKind() == CommitKind.COMPACT) {
      val previous = table.snapshotManager().snapshot(context.snapshot.id() - 1)
      val input = DataEvolutionRangeSortPlan
        .plan(table, previous, new Range(0, 1), null, Seq("id").asJava)
        .get(0)
        .inputFiles()
        .asInstanceOf[CommitMessageImpl]
        .withCheckFromSnapshot(previous.id())
      // Reuse the outer commit user and expose the inner rejection to its post-commit callback.
      val nested = table.newCommit(context.snapshot.commitUser())
      try {
        nested.dataEvolutionRangeSortCheck().commit(Seq[CommitMessage](input).asJava)
      } finally {
        nested.close()
      }
    }
  }

  override def retry(committable: ManifestCommittable): Unit = {}

  override def close(): Unit = {}
}
