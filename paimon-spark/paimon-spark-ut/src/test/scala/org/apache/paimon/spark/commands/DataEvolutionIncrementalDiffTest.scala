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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class DataEvolutionIncrementalDiffTest extends PaimonSparkTestBase {

  test("DE incremental DIFF rejects metadata-only vector search") {
    withTable("t_vector_diff") {
      sql("""CREATE TABLE t_vector_diff (id INT, v ARRAY<FLOAT>) TBLPROPERTIES (
            |'bucket'='-1','row-tracking.enabled'='true',
            |'data-evolution.enabled'='true','write-only'='true')""".stripMargin)
      sql("INSERT INTO t_vector_diff VALUES (10, array(0f, 1f))")
      val start = loadTable("t_vector_diff").snapshotManager().latestSnapshot().id()
      sql("INSERT INTO t_vector_diff VALUES (20, array(-1f, 0f))")
      val end = loadTable("t_vector_diff").snapshotManager().latestSnapshot().id()
      sql("INSERT INTO t_vector_diff VALUES (30, array(1f, 0f))")
      sql("CALL sys.create_global_index(table => 'test.t_vector_diff', index_column => 'v', " +
        "index_type => 'ivf-flat', options => 'ivf-flat.dimension=2,ivf-flat.nlist=1,ivf-flat.metric=cosine')")
      val search = "vector_search('t_vector_diff', 'v', array(1f, 0f), 1)"
      checkAnswer(sql(s"SELECT id, __paimon_search_score FROM $search"), Row(30, 1.0f) :: Nil)
      checkAnswer(sql(s"SELECT __paimon_search_score FROM $search"), Row(1.0f) :: Nil)
      val indexed = loadTable("t_vector_diff").snapshotManager().latestSnapshot().id()
      sql("INSERT INTO t_vector_diff VALUES (40, array(0f, -1f))")
      sql(s"ALTER TABLE t_vector_diff SET TBLPROPERTIES ('scan.snapshot-id'='$indexed')")
      checkAnswer(sql(s"SELECT __paimon_search_score FROM $search"), Row(1.0f) :: Nil)
      sql("ALTER TABLE t_vector_diff UNSET TBLPROPERTIES ('scan.snapshot-id')")
      checkAnswer(
        spark.read
          .option("incremental-between", s"$start,$end")
          .option("incremental-between-scan-mode", "diff")
          .table("t_vector_diff")
          .select("id"),
        Row(20) :: Nil
      )
      sql(
        s"ALTER TABLE t_vector_diff SET TBLPROPERTIES ('incremental-between'='$start,$end', " +
          "'incremental-between-scan-mode'='diff')")
      Seq("diff", "delta").foreach {
        mode =>
          sql(
            s"ALTER TABLE t_vector_diff SET TBLPROPERTIES ('incremental-between-scan-mode'='$mode')")
          val error = intercept[Exception] {
            sql(s"SELECT __paimon_search_score FROM $search").collect()
          }
          assert(error.toString.contains("requires a single snapshot"))
      }
    }
  }

  test("DE diff preserves file path and physical row index through deletion and filtering") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT) TBLPROPERTIES (
            |'bucket'='-1','row-tracking.enabled'='true',
            |'data-evolution.enabled'='true','deletion-vectors.enabled'='true',
            |'write-only'='true')""".stripMargin)
      sql("INSERT INTO t VALUES (0)")
      val before = loadTable("t").snapshotManager().latestSnapshot().id()
      sql("INSERT INTO t SELECT CAST(id AS INT) FROM range(10, 16, 1, 1)")
      val table = loadTable("t")
      val files = table
        .store()
        .newScan()
        .plan()
        .files()
        .asScala
        .map(_.file())
        .filter(_.nonNullFirstRowId() == 1L)
      assert(files.size == 1)
      assert(files.head.rowCount() == 6L)
      val filePath = table
        .store()
        .pathFactory()
        .createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0)
        .toPath(files.head)
        .toString

      sql("DELETE FROM t WHERE id=11")
      val after = loadTable("t").snapshotManager().latestSnapshot().id()
      val diff = spark.read
        .option("incremental-between", s"$before,$after")
        .option("incremental-between-scan-mode", "diff")
        .table("t")

      checkAnswer(
        diff.select("id", "__paimon_file_path", "__paimon_row_index"),
        Seq(
          Row(10, filePath, 0L),
          Row(12, filePath, 2L),
          Row(13, filePath, 3L),
          Row(14, filePath, 4L),
          Row(15, filePath, 5L))
      )
      checkAnswer(
        diff.where("id IN (12,15)").select("__paimon_file_path", "__paimon_row_index"),
        Seq(Row(filePath, 2L), Row(filePath, 5L)))
    }
  }

  test("DE diff preserves physical metadata for an untouched column after a partial update") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, a INT, b INT) TBLPROPERTIES (
            |'bucket'='-1','row-tracking.enabled'='true',
            |'data-evolution.enabled'='true','deletion-vectors.enabled'='true',
            |'write-only'='true')""".stripMargin)
      sql(
        "INSERT INTO t SELECT CAST(id AS INT), CAST(id * 10 AS INT), " +
          "CAST(id * 100 AS INT) FROM range(1, 4, 1, 1)")
      val before = loadTable("t").snapshotManager().latestSnapshot().id()
      val latest = spark.table("t").select("a", "__paimon_file_path", "__paimon_row_index")
      val original = latest.orderBy("a").collect()
      assert(original.map(_.getLong(2)).toSeq == Seq(0L, 1L, 2L))
      val filePath = original.head.getString(1)
      assert(original.forall(_.getString(1) == filePath))
      sql("UPDATE t SET b=b+1")
      sql("DELETE FROM t WHERE id=2")
      val after = loadTable("t").snapshotManager().latestSnapshot().id()
      val diff = spark.read
        .option("incremental-between", s"$before,$after")
        .option("incremental-between-scan-mode", "diff")
        .table("t")

      checkAnswer(
        diff.select("a", "__paimon_file_path", "__paimon_row_index"),
        Seq(Row(10, filePath, 0L), Row(30, filePath, 2L)))
      val survivors = Seq(Row(1, 10, 101), Row(3, 30, 301))
      checkAnswer(diff, survivors)
      checkAnswer(diff.where("b=101").select("a"), Seq(Row(10)))
      checkAnswer(diff.orderBy("id").limit(1), survivors.take(1))
      assert(diff.limit(1).collect().length == 1)
      checkAnswer(sql("SELECT * FROM t"), survivors)
    }
  }
}
