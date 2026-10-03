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

import org.apache.paimon.CoreOptions
import org.apache.paimon.schema.TableSchema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.utils.ExceptionUtils

import org.apache.spark.sql.Row

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

class EnableDataEvolutionProcedureTest extends PaimonSparkTestBase {

  import testImplicits._

  test("Paimon Procedure: enable data evolution on an append table") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v INT, w INT)")
      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, 10, 100), (2, 20, 200)")
      sql("INSERT INTO t VALUES (3, 30, 300)")
      val snapshotBefore = loadTable("t").snapshotManager().latestSnapshotId()

      // dry run reports the work and changes nothing
      val dryRun = sql("CALL sys.enable_data_evolution(table => 't', dry_run => true)").collect()
      assert(dryRun.length == 1)
      assert(dryRun(0).getString(0).startsWith("Dry run."), dryRun(0).getString(0))
      assert(dryRun(0).getString(0).contains("2 file(s) with 3 row(s)"), dryRun(0).getString(0))
      assert(loadTable("t").snapshotManager().latestSnapshotId() == snapshotBefore)
      assert(!loadTable("t").coreOptions().dataEvolutionEnabled())

      val result = sql("CALL sys.enable_data_evolution(table => 't')").collect()
      assert(result.length == 1)
      assert(result(0).getString(0).startsWith("Success."), result(0).getString(0))
      assert(
        result(0).getString(0).contains("2 file(s) with 3 row(s) assigned row ids, nextRowId=3"),
        result(0).getString(0))

      val table = loadTable("t")
      assert(table.coreOptions().rowTrackingEnabled())
      assert(table.coreOptions().dataEvolutionEnabled())
      // one snapshot assigning the row ids, one fencing writers that predate the schema change
      assert(table.snapshotManager().latestSnapshotId() == snapshotBefore + 2)
      checkAnswer(
        sql("SELECT id, v, w, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(1, 10, 100, 0), Row(2, 20, 200, 1), Row(3, 30, 300, 2)))

      // a second call has nothing to do
      val again = sql("CALL sys.enable_data_evolution(table => 't')").collect()
      assert(again(0).getString(0).startsWith("Skipped."), again(0).getString(0))

      // the converted table behaves like a data-evolution table: a merge on _ROW_ID writes only
      // the assigned column, new rows continue the row id sequence
      Seq((0L, 11), (2L, 33)).toDF("rid", "v").createOrReplaceTempView("s")
      sql("""
            |MERGE INTO t USING s ON t._ROW_ID = s.rid
            |WHEN MATCHED THEN UPDATE SET v = s.v
            |""".stripMargin)
      checkAnswer(
        sql("SELECT id, v, w, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(1, 11, 100, 0), Row(2, 20, 200, 1), Row(3, 33, 300, 2)))
      checkAnswer(
        sql("SELECT write_cols FROM `t$files` ORDER BY max_sequence_number DESC LIMIT 1"),
        Seq(Row(Seq("v"))))

      // UPDATE takes the data-evolution path too: only the assigned column is written
      sql("UPDATE t SET w = w + 1 WHERE id = 2")
      checkAnswer(
        sql("SELECT id, v, w, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(1, 11, 100, 0), Row(2, 20, 201, 1), Row(3, 33, 300, 2)))
      checkAnswer(
        sql("SELECT write_cols FROM `t$files` ORDER BY max_sequence_number DESC LIMIT 1"),
        Seq(Row(Seq("w"))))

      sql("INSERT INTO t VALUES (4, 40, 400)")
      checkAnswer(sql("SELECT id, _ROW_ID FROM t WHERE id = 4"), Seq(Row(4, 3)))
    }
  }

  test("Paimon Procedure: updates after the conversion win over rows of one large commit") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v INT)")
      // a plain append writer numbers the 100 rows of this commit: its file had sequence numbers
      // up to 99, far above the snapshot ids of the updates below
      sql(
        "INSERT INTO t SELECT /*+ REPARTITION(1) */ CAST(id AS INT), CAST(id AS INT) * 10 " +
          "FROM range(0, 100)")

      sql("CALL sys.enable_data_evolution(table => 't')")

      sql("UPDATE t SET v = -5 WHERE id = 5")
      sql("""
            |MERGE INTO t USING (SELECT _ROW_ID AS rid FROM t WHERE id = 7) s
            |ON t._ROW_ID = s.rid
            |WHEN MATCHED THEN UPDATE SET v = -7
            |""".stripMargin)
      checkAnswer(sql("SELECT id, v FROM t WHERE v < 0 ORDER BY id"), Seq(Row(5, -5), Row(7, -7)))
      // every other row keeps its value: 49500 - 50 - 70 - 5 - 7
      checkAnswer(sql("SELECT count(*), sum(v) FROM t"), Seq(Row(100L, 49368L)))
    }
  }

  test("Paimon Procedure: enable data evolution assigns partition-contiguous row ids") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, pt STRING) PARTITIONED BY (pt)")
      sql("INSERT INTO t VALUES (0, 'a')")
      sql("INSERT INTO t VALUES (1, 'b')")
      sql("INSERT INTO t VALUES (2, 'a')")
      sql("INSERT INTO t VALUES (3, 'b')")

      sql("CALL sys.enable_data_evolution(table => 't')")

      checkAnswer(
        sql("SELECT id, pt, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(0, "a", 0), Row(1, "b", 2), Row(2, "a", 1), Row(3, "b", 3)))
      val reassign = sql("CALL sys.reassign_row_id(table => 't')").collect()
      assert(reassign(0).getString(0).startsWith("Skipped."), reassign(0).getString(0))
    }
  }

  test("Paimon Procedure: enable data evolution keeps deletion vectors") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v INT) TBLPROPERTIES ('deletion-vectors.enabled' = 'true')")
      sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
      sql("DELETE FROM t WHERE id = 2")

      sql("CALL sys.enable_data_evolution(table => 't')")

      checkAnswer(
        sql("SELECT id, v, _ROW_ID FROM t ORDER BY id"),
        Seq(Row(1, 10, 0), Row(3, 30, 2)))
      // deletes and merge-deletes keep working on the converted files
      sql("DELETE FROM t WHERE id = 3")
      checkAnswer(sql("SELECT id, v, _ROW_ID FROM t ORDER BY id"), Seq(Row(1, 10, 0)))
      Seq(0L).toDF("rid").createOrReplaceTempView("s")
      sql("MERGE INTO t USING s ON t._ROW_ID = s.rid WHEN MATCHED THEN DELETE")
      checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(0)))
    }
  }

  test("Paimon Procedure: converted table supports sub-field data evolution") {
    withTable("t", "s") {
      sql("CREATE TABLE t (id INT, nest STRUCT<a: INT, b: STRING>)")
      sql("INSERT INTO t VALUES (1, named_struct('a', 10, 'b', 'x'))")

      sql("CALL sys.enable_data_evolution(table => 't')")
      sql("ALTER TABLE t SET TBLPROPERTIES ('data-evolution.nested-field.enabled' = 'true')")

      Seq((0L, 100)).toDF("rid", "newa").createOrReplaceTempView("s")
      sql("""
            |MERGE INTO t USING s ON t._ROW_ID = s.rid
            |WHEN MATCHED THEN UPDATE SET t.nest.a = s.newa
            |""".stripMargin)
      checkAnswer(sql("SELECT id, nest.a, nest.b FROM t"), Seq(Row(1, 100, "x")))
      checkAnswer(
        sql("SELECT write_cols FROM `t$files` ORDER BY max_sequence_number DESC LIMIT 1"),
        Seq(Row(Seq("nest.a"))))
    }
  }

  test("Paimon Procedure: snapshots before the conversion stay readable with NULL row ids") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v INT)")
      sql("INSERT INTO t VALUES (1, 10)")
      val before = loadTable("t").snapshotManager().latestSnapshotId()
      sql("CALL sys.create_tag(table => 't', tag => 'before')")

      sql("CALL sys.enable_data_evolution(table => 't')")
      sql("INSERT INTO t VALUES (2, 20)")

      checkAnswer(sql("SELECT id, _ROW_ID FROM t ORDER BY id"), Seq(Row(1, 0), Row(2, 1)))
      checkAnswer(sql(s"SELECT id, _ROW_ID FROM t VERSION AS OF $before"), Seq(Row(1, null)))
      checkAnswer(sql("SELECT id, _ROW_ID FROM t VERSION AS OF 'before'"), Seq(Row(1, null)))
      checkAnswer(sql(s"SELECT id FROM t VERSION AS OF $before WHERE v = 10"), Seq(Row(1)))
    }
  }

  test("Paimon Procedure: a converted table can build and use a global index") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING)")
      sql("INSERT INTO t VALUES (1, 'name-1'), (2, 'name-2')")
      sql("INSERT INTO t VALUES (3, 'name-3')")

      // A plain append table cannot have a global index at all.
      val refused = intercept[Exception] {
        sql(
          "CALL sys.create_global_index(table => 'test.t', index_column => 'name', " +
            "index_type => 'btree')")
      }
      assert(
        ExceptionUtils.stringifyException(refused).contains("row-tracking.enabled"),
        ExceptionUtils.stringifyException(refused))

      sql("CALL sys.enable_data_evolution(table => 't')")

      // The historical rows are indexed by the first call.
      sql(
        "CALL sys.create_global_index(table => 'test.t', index_column => 'name', " +
          "index_type => 'btree')")
      checkAnswer(sql("SELECT id FROM t WHERE name = 'name-2'"), Row(2) :: Nil)
      checkAnswer(
        sql("SELECT id FROM t WHERE name IN ('name-1', 'name-3') ORDER BY id"),
        Row(1) :: Row(3) :: Nil)

      // Rows appended after the conversion are picked up by a later incremental call.
      sql("INSERT INTO t VALUES (4, 'name-4')")
      sql(
        "CALL sys.create_global_index(table => 'test.t', index_column => 'name', " +
          "index_type => 'btree')")
      checkAnswer(sql("SELECT id FROM t WHERE name = 'name-4'"), Row(4) :: Nil)
      checkAnswer(sql("SELECT id FROM t WHERE name = 'name-2'"), Row(2) :: Nil)
      checkAnswer(sql("SELECT count(*) FROM t"), Row(4) :: Nil)
    }
  }

  test("Paimon Procedure: enable data evolution refuses unsupported tables") {
    withTable("pk", "bucketed") {
      sql("CREATE TABLE pk (id INT, v INT) TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')")
      val pkError = intercept[Exception] {
        sql("CALL sys.enable_data_evolution(table => 'pk')")
      }.getMessage
      assert(pkError.contains("Cannot enable data evolution on table test.pk"), pkError)
      assert(pkError.contains("primary-key"), pkError)

      sql(
        "CREATE TABLE bucketed (id INT, v INT) TBLPROPERTIES ('bucket' = '2', 'bucket-key' = 'id')")
      val bucketError = intercept[Exception] {
        sql("CALL sys.enable_data_evolution(table => 'bucketed')")
      }.getMessage
      assert(bucketError.contains("bucket = -1"), bucketError)

      val alterError = intercept[Exception] {
        withTable("plain") {
          sql("CREATE TABLE plain (id INT)")
          sql("INSERT INTO plain VALUES (1)")
          sql("ALTER TABLE plain SET TBLPROPERTIES ('row-tracking.enabled' = 'true')")
        }
      }.getMessage
      assert(alterError.contains("Change 'row-tracking.enabled' is not supported yet"), alterError)

      // not even before the first write: a writer may be committing at the same time
      val emptyAlterError = intercept[Exception] {
        withTable("empty") {
          sql("CREATE TABLE empty (id INT)")
          sql("ALTER TABLE empty SET TBLPROPERTIES ('data-evolution.enabled' = 'true')")
        }
      }.getMessage
      assert(
        emptyAlterError.contains("Cannot enable 'data-evolution.enabled' on an existing table"),
        emptyAlterError)
    }
  }

  test("Paimon Procedure: MERGE INTO refuses files that still have no row id") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, v INT)")
      sql("INSERT INTO t VALUES (1, 10)")

      // a schema with data evolution enabled but files that were never converted
      val table = loadTable("t")
      val latest = table.schemaManager().latest().get()
      val options = new JHashMap[String, String](latest.options())
      options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
      options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
      assert(
        table
          .schemaManager()
          .commit(
            new TableSchema(
              latest.id() + 1,
              latest.fields(),
              latest.highestFieldId(),
              latest.partitionKeys(),
              latest.primaryKeys(),
              options,
              latest.comment())))
      sql("REFRESH TABLE t")

      // the rows read fine, with a NULL row id
      checkAnswer(sql("SELECT id, _ROW_ID FROM t"), Seq(Row(1, null)))

      Seq((0L, 11)).toDF("rid", "v").createOrReplaceTempView("s")
      val error = intercept[Exception] {
        sql("MERGE INTO t USING s ON t._ROW_ID = s.rid WHEN MATCHED THEN UPDATE SET v = s.v")
      }.getMessage
      assert(error.contains("has no first row id"), error)
      assert(error.contains("sys.enable_data_evolution"), error)

      // the procedure repairs the table
      val result = sql("CALL sys.enable_data_evolution(table => 't')").collect()
      assert(result(0).getString(0).startsWith("Success."), result(0).getString(0))
      sql("MERGE INTO t USING s ON t._ROW_ID = s.rid WHEN MATCHED THEN UPDATE SET v = s.v")
      checkAnswer(sql("SELECT id, v, _ROW_ID FROM t"), Seq(Row(1, 11, 0)))
    }
  }
}
