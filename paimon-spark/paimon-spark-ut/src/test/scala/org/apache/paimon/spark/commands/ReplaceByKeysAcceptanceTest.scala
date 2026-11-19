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
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}

import org.apache.spark.sql.{AnalysisException, Row}

import scala.collection.JavaConverters._

/** Public CALL coverage plus deterministic interleavings using the existing preparation API. */
class ReplaceByKeysAcceptanceTest extends PaimonSparkTestBase {

  private val deOptions =
    "'bucket'='-1','data-evolution.enabled'='true','row-tracking.enabled'='true'," +
      "'deletion-vectors.enabled'='true','write-only'='true','snapshot.num-retained.min'='20'"

  private def createTarget(columns: String = "id INT, value STRING"): Unit = {
    sql(s"CREATE TABLE target ($columns) TBLPROPERTIES ($deOptions)")
  }

  private def sourceQuote(text: String): String =
    "\"" + text.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def replace(
      query: String,
      keys: Seq[String] = Seq("id"),
      filter: Option[String] = None): Unit = {
    val quoted = sourceQuote(query)
    val keyArray = keys.map(sourceQuote).mkString("ARRAY(", ",", ")")
    val predicate = filter.map(f => ", target_filter => " + sourceQuote(f)).getOrElse("")
    checkAnswer(
      sql(s"""CALL sys.replace_by_keys(table => 'target',
             |source_query => $quoted, key_columns => $keyArray $predicate)""".stripMargin),
      Seq(Row(true))
    )
  }

  private def snapshotId(name: String = "target"): Long =
    Option(loadTable(name).snapshotManager().latestSnapshot()).map(_.id()).getOrElse(-1L)

  private def command(
      query: String = "SELECT 1 AS id, 'replacement' AS value"): ReplaceByKeysCommand =
    ReplaceByKeysCommand(loadTable("target"), createRelationV2("target"), query, Seq("id"), None)

  private def commit(messages: Seq[CommitMessage], baseline: Long): Unit = {
    val checked: Seq[CommitMessage] =
      if (baseline < 0) messages
      else messages.map(_.asInstanceOf[CommitMessageImpl].withCheckFromSnapshot(baseline))
    val tableCommit = loadTable("target").newBatchWriteBuilder().newCommit()
    try tableCommit.withOperation(Snapshot.Operation.MERGE).commit(checked.asJava)
    finally tableCommit.close()
  }

  private def assertRejectedWithoutChanges(body: => Unit): Exception = {
    val targetRows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
    val sourceRows = sql("SELECT * FROM source").collect().toSeq
    val targetSnapshot = snapshotId()
    val sourceSnapshot = snapshotId("source")
    val failure = intercept[Exception](body)
    assert(snapshotId() == targetSnapshot)
    assert(snapshotId("source") == sourceSnapshot)
    checkAnswer(sql("SELECT *,_ROW_ID FROM target"), targetRows)
    checkAnswer(sql("SELECT * FROM source"), sourceRows)
    failure
  }

  Seq("unwritten", "written-empty", "pruned", "populated").foreach {
    state =>
      test(s"membership types are validated before deletion shortcuts: $state") {
        withSparkSQLConf(
          "spark.sql.ansi.enabled" -> "false",
          "spark.sql.storeAssignmentPolicy" -> "LEGACY") {
          withTable("target", "source", "assignment") {
            createTarget("id STRING, value STRING")
            if (state != "unwritten") sql("INSERT INTO target VALUES ('old','keep')")
            if (state == "written-empty") sql("DELETE FROM target WHERE id='old'")
            sql("CREATE TABLE source (id ARRAY<STRING>, value STRING)")
            sql("INSERT INTO source VALUES (array('new'),'payload')")
            sql("CREATE TABLE assignment (id STRING, value STRING)")
            // Assignment permits this cast; IN membership must reject the original key type.
            sql("INSERT INTO assignment SELECT * FROM source")
            assert(sql("SELECT * FROM assignment").count() == 1L)
            val filter = if (state == "pruned") Some("id='absent'") else None
            val restriction = filter.map(f => " AND (" + f + ")").getOrElse("")
            intercept[AnalysisException] {
              sql("DELETE FROM target WHERE id IN (SELECT id FROM source)" + restriction).collect()
            }
            val failure = assertRejectedWithoutChanges {
              val argument = filter.map(f => ", target_filter => " + sourceQuote(f)).getOrElse("")
              sql(s"""CALL sys.replace_by_keys(table => 'target',
                     |source_query => 'SELECT * FROM source',
                     |key_columns => ARRAY('id') $argument)""".stripMargin).collect()
            }
            assert(failure.isInstanceOf[AnalysisException])
          }
        }
      }
  }

  test("target filter restricts deletion only and composite keys do not cross match") {
    withTable("target", "source") {
      createTarget("tenant INT, entity STRING, value STRING")
      sql("""INSERT INTO target VALUES (7,'a','old'),(7,'b','cross'),
            |(8,'b','outside'),(7,NULL,'old-null')""".stripMargin)
      sql("CREATE TABLE source (tenant INT, entity STRING, value STRING)")
      sql("""INSERT INTO source VALUES (7,'a','new'),(7,'a','new'),
            |(8,'b','outside-new'),(7,NULL,'new-null')""".stripMargin)
      val retained = sql("SELECT *,_ROW_ID FROM target WHERE value <> 'old'").collect().toSeq
      replace("SELECT value,entity,tenant FROM source", Seq("tenant", "entity"), Some("tenant=7"))
      checkAnswer(
        sql("SELECT * FROM target"),
        Seq(
          Row(7, "a", "new"),
          Row(7, "a", "new"),
          Row(7, "b", "cross"),
          Row(8, "b", "outside"),
          Row(8, "b", "outside-new"),
          Row(7, null, "old-null"),
          Row(7, null, "new-null"))
      )
      checkAnswer(
        sql("SELECT *,_ROW_ID FROM target WHERE value IN ('cross','outside','old-null')"),
        retained)
    }
  }

  Seq(
    "ordinary primary key" -> "'primary-key'='id','bucket'='1'",
    "postpone merge on read" ->
      ("'primary-key'='id','bucket'='-2','postpone.batch-write-fixed-bucket'='false'," +
        "'postpone.merge-on-read'='true'")
  ).foreach {
    case (mode, options) =>
      test(s"replacement reads logical rows from a nonempty $mode source") {
        withTable("target", "source") {
          createTarget()
          sql("INSERT INTO target VALUES (1,'old-target'),(9,'keep')")
          sql(s"""CREATE TABLE source (id INT, value STRING, seq BIGINT)
                 |TBLPROPERTIES ($options,'sequence.field'='seq')""".stripMargin)
          sql("INSERT INTO source VALUES (1,'old-source',1),(2,'second',1)")
          sql("INSERT INTO source VALUES (1,'latest',2)")
          checkAnswer(sql("SELECT id,value FROM source"), Seq(Row(1, "latest"), Row(2, "second")))
          replace("SELECT id,value FROM source")
          checkAnswer(
            sql("SELECT * FROM target"),
            Seq(Row(1, "latest"), Row(2, "second"), Row(9, "keep")))
        }
      }
  }

  test("replacement reads current DE column values while retaining untouched source columns") {
    withTable("target", "source") {
      createTarget("id INT, value STRING, payload STRING")
      sql("INSERT INTO target VALUES (1,'old-target','old-payload')")
      sql(s"CREATE TABLE source (id INT, value STRING, payload STRING) TBLPROPERTIES ($deOptions)")
      sql("INSERT INTO source VALUES (1,'old-source','retain'),(2,'second','other')")
      sql("UPDATE source SET value='updated' WHERE id=1")
      replace("SELECT payload,id,value FROM source")
      checkAnswer(
        sql("SELECT * FROM target"),
        Seq(Row(1, "updated", "retain"), Row(2, "second", "other")))
    }
  }

  test("self source with deterministic CTE aliases and UNION ALL uses the original rows") {
    withTable("target") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'one'),(2,'two')")
      val before = snapshotId()
      replace("""WITH batch AS (SELECT id,value FROM target)
                |SELECT l.id,concat(l.value,'-new') AS value FROM batch l
                |JOIN batch r ON l.id=r.id WHERE l.id=1
                |UNION ALL SELECT id,concat(value,'-new') AS value FROM batch WHERE id=2
                |""".stripMargin)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "one-new"), Row(2, "two-new")))
      assert(snapshotId() == before + 1)
    }
  }

  Seq("snapshot", "timestamp").foreach {
    selector =>
      test(s"public replacement respects a historical source $selector selector") {
        withTable("target", "source") {
          createTarget()
          sql("INSERT INTO target VALUES (1,'old-target')")
          sql("CREATE TABLE source (id INT, value STRING)")
          sql("INSERT INTO source VALUES (1,'historical')")
          val selected = loadTable("source").snapshotManager().latestSnapshot()
          sql("INSERT INTO source VALUES (2,'later')")
          val reader = spark.read
          if (selector == "snapshot") reader.option("scan.snapshot-id", selected.id().toString)
          else {
            assert(
              loadTable("source").snapshotManager().latestSnapshot().timeMillis() >
                selected.timeMillis())
            reader.option("scan.timestamp-millis", selected.timeMillis().toString)
          }
          reader.table("source").createOrReplaceTempView("acceptance_history")
          try {
            checkAnswer(sql("SELECT * FROM acceptance_history"), Seq(Row(1, "historical")))
            replace("SELECT * FROM acceptance_history")
          } finally spark.catalog.dropTempView("acceptance_history")
          checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "historical")))
        }
      }
  }

  test("fallback with both branches populated keeps current partition precedence") {
    withTable("target", "source") {
      createTarget("id INT, value STRING, pt STRING")
      sql("INSERT INTO target VALUES (1,'old','a')")
      sql("CREATE TABLE source (id INT, value STRING, pt STRING) PARTITIONED BY (pt)")
      sql("CALL sys.create_branch('test.source','backup')")
      val backup = "`source$branch_backup`"
      sql(s"INSERT INTO $backup VALUES (8,'shadowed','a'),(2,'backup-only','b')")
      sql("INSERT INTO source VALUES (1,'current','a'),(3,'current-only','c')")
      sql("ALTER TABLE source SET TBLPROPERTIES ('scan.fallback-branch'='backup')")
      val expected =
        Seq(Row(1, "current", "a"), Row(2, "backup-only", "b"), Row(3, "current-only", "c"))
      checkAnswer(sql("SELECT * FROM source"), expected)
      replace("SELECT * FROM source")
      checkAnswer(sql("SELECT * FROM target"), expected)
    }
  }

  test("invalid keys and source schemas are rejected without row or snapshot changes") {
    withTable("target", "source") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old')")
      sql("CREATE TABLE source (id INT, value STRING)")
      sql("INSERT INTO source VALUES (1,'new')")
      Seq(
        ("SELECT * FROM source", Seq.empty[String]),
        ("SELECT * FROM source", Seq("id", "id")),
        ("SELECT * FROM source", Seq("missing")),
        ("SELECT id FROM source", Seq("id")),
        ("SELECT *,id AS extra FROM source", Seq("id")),
        ("SELECT id,id AS ID,value FROM source", Seq("id"))
      ).foreach {
        case (query, keys) =>
          withSparkSQLConf("spark.sql.caseSensitive" -> "false") {
            assertRejectedWithoutChanges(replace(query, keys))
          }
      }
      withSparkSQLConf("spark.sql.caseSensitive" -> "true") {
        assertRejectedWithoutChanges(replace("SELECT id AS ID,value FROM source"))
      }
      Seq(
        "source_query => '', key_columns => ARRAY('id')",
        "source_query => CAST(NULL AS STRING), key_columns => ARRAY('id')",
        "source_query => 'SELECT * FROM source', key_columns => CAST(NULL AS ARRAY<STRING>)",
        "source_query => 'SELECT * FROM source', key_columns => ARRAY(CAST(NULL AS STRING))"
      ).foreach {
        arguments =>
          assertRejectedWithoutChanges {
            sql(s"CALL sys.replace_by_keys(table => 'target', $arguments)")
          }
      }
    }
  }

  test("unstable sources and invalid target filters fail before publishing any changes") {
    withTable("target", "source") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old')")
      sql("CREATE TABLE source (id INT, value STRING)")
      sql("INSERT INTO source VALUES (1,'new')")
      Seq(
        "SELECT id,CAST(rand() AS STRING) AS value FROM source",
        "SELECT id,max(value) AS value FROM source GROUP BY id",
        "SELECT id,CAST(row_number() OVER (ORDER BY id) AS STRING) AS value FROM source",
        "SELECT * FROM source LIMIT 1",
        "DELETE FROM source",
        "INSERT INTO source VALUES (2,'mutation')"
      ).foreach(query => assertRejectedWithoutChanges(replace(query)))
      Seq("missing=1", "rand() > 0.5", "id IN (SELECT id FROM source)").foreach {
        filter =>
          assertRejectedWithoutChanges(replace("SELECT * FROM source", filter = Some(filter)))
      }
    }
  }

  test("unsupported full scan alternatives are not silently changed to latest full") {
    withTable("target", "source") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old')")
      sql("CREATE TABLE source (id INT, value STRING)")
      sql("INSERT INTO source VALUES (1,'new')")
      Seq(
        "scan.mode" -> "latest-delta",
        "scan.mode" -> "compacted-full",
        "batch-scan-mode" -> "compact").foreach {
        case (key, value) =>
          spark.read
            .option(key, value)
            .table("source")
            .createOrReplaceTempView("acceptance_scan")
          try {
            assertRejectedWithoutChanges(replace("SELECT * FROM acceptance_scan"))
          } finally spark.catalog.dropTempView("acceptance_scan")
      }
    }
  }

  test("a target filter execution failure cannot publish any replacement files or deletions") {
    withTable("target", "source") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old'),(2,'keep')")
      sql("CREATE TABLE source (id INT, value STRING)")
      sql("INSERT INTO source VALUES (1,'new')")
      val before = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
      val baseline = snapshotId()
      val failure = intercept[Exception] {
        replace(
          "SELECT * FROM source",
          filter = Some(
            "CASE WHEN id=1 THEN CAST(raise_error('acceptance-delete-failure') AS BOOLEAN) " +
              "ELSE true END"))
      }
      val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
      assert(
        causes.exists(e => Option(e.getMessage).exists(_.contains("acceptance-delete-failure"))))
      assert(snapshotId() == baseline)
      checkAnswer(sql("SELECT *,_ROW_ID FROM target"), before)
    }
  }

  test("independent readers see old rows before commit and complete new rows after one snapshot") {
    withTable("target") {
      createTarget()
      sql("INSERT INTO target VALUES (1,'old'),(2,'keep')")
      val beforeRows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
      val (messages, baseline) = command().prepare(spark)
      assert(messages.nonEmpty)
      val reader = spark.newSession()
      checkAnswer(
        reader.sql("SELECT * FROM paimon.test.target"),
        Seq(Row(1, "old"), Row(2, "keep")))
      assert(snapshotId() == baseline)
      commit(messages, baseline)
      assert(snapshotId() == baseline + 1)
      checkAnswer(
        reader.sql("SELECT * FROM paimon.test.target"),
        Seq(Row(1, "replacement"), Row(2, "keep")))
      checkAnswer(
        reader.read
          .option("scan.snapshot-id", baseline.toString)
          .table("paimon.test.target")
          .selectExpr("*", "_ROW_ID"),
        beforeRows)
    }
  }

  test("empty target preparation can commit after a concurrent first append without deleting it") {
    withTable("target") {
      createTarget()
      val (messages, baseline) = command().prepare(spark)
      assert(baseline == -1L && messages.nonEmpty)
      assert(snapshotId() == -1L)
      sql("INSERT INTO target VALUES (1,'concurrent')")
      val afterAppend = snapshotId()
      val concurrent = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
      commit(messages, baseline)
      assert(snapshotId() == afterAppend + 1)
      checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "concurrent"), Row(1, "replacement")))
      checkAnswer(sql("SELECT *,_ROW_ID FROM target WHERE value='concurrent'"), concurrent)
      val ids = sql("SELECT _ROW_ID FROM target").collect().map(_.getLong(0))
      assert(ids.distinct.length == 2)
    }
  }

  Seq("delete", "replacement").foreach {
    concurrent =>
      test(s"a stale replacement cannot overwrite a concurrent same-anchor $concurrent") {
        withTable("target") {
          createTarget()
          sql("""INSERT INTO target SELECT /*+ COALESCE(1) */ * FROM
                |VALUES (1,'old'),(2,'keep') AS t(id,value)""".stripMargin)
          val (messages, baseline) = command().prepare(spark)
          if (concurrent == "delete") sql("DELETE FROM target WHERE id=1")
          else replace("SELECT 1 AS id,'winner' AS value")
          val winnerRows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
          val winnerSnapshot = snapshotId()
          intercept[Exception](commit(messages, baseline))
          assert(snapshotId() == winnerSnapshot)
          checkAnswer(sql("SELECT *,_ROW_ID FROM target"), winnerRows)
          checkAnswer(
            sql("SELECT * FROM target"),
            if (concurrent == "delete") Seq(Row(2, "keep"))
            else Seq(Row(1, "winner"), Row(2, "keep")))
        }
      }
  }

  Seq("compact", "materialize_deletion_vectors").foreach {
    maintenance =>
      test(s"stale replacement is rejected after $maintenance removes its physical anchor") {
        withTable("target") {
          createTarget()
          sql("""INSERT INTO target SELECT /*+ COALESCE(1) */ * FROM
                |VALUES (1,'old'),(3,'deleted') AS t(id,value)""".stripMargin)
          sql("INSERT INTO target VALUES (2,'keep')")
          sql("DELETE FROM target WHERE id=3")
          def files(): Set[String] =
            loadTable("target")
              .newSnapshotReader()
              .read()
              .dataSplits()
              .asScala
              .flatMap(_.dataFiles().asScala.map(_.fileName()))
              .toSet
          val rowId = sql("SELECT _ROW_ID FROM target WHERE id=1").head().getLong(0)
          val anchors = loadTable("target")
            .newSnapshotReader()
            .read()
            .dataSplits()
            .asScala
            .flatMap(_.dataFiles().asScala)
            .filter(
              f =>
                f.firstRowId() != null && f.firstRowId().longValue() <= rowId &&
                  rowId < f.firstRowId().longValue() + f.rowCount())
            .map(_.fileName())
            .toSet
          assert(anchors.nonEmpty)
          val (messages, baseline) = command().prepare(spark)
          sql(s"""CALL sys.$maintenance(table => 'target',
                 |options => 'compaction.min.file-num=2')""".stripMargin)
          assert((anchors.intersect(files())).isEmpty, "Maintenance must remove the matched anchor")
          checkAnswer(sql("SELECT * FROM target"), Seq(Row(1, "old"), Row(2, "keep")))
          val afterMaintenance = snapshotId()
          val winnerRows = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
          intercept[Exception](commit(messages, baseline))
          assert(snapshotId() == afterMaintenance)
          checkAnswer(sql("SELECT *,_ROW_ID FROM target"), winnerRows)
        }
      }
  }

  test("ordinary append primary key and DE without DV targets reject CALL without changes") {
    Seq(
      ("append", "'bucket'='-1'", "requires a Data Evolution table"),
      ("primary key", "'primary-key'='id','bucket'='1'", "requires a Data Evolution table"),
      (
        "DE without DV",
        deOptions.replace(
          "'deletion-vectors.enabled'='true'",
          "'deletion-vectors.enabled'='false'"),
        "requires deletion-vectors.enabled=true")
    ).foreach {
      case (mode, options, expectedError) =>
        withTable("target") {
          sql(s"CREATE TABLE target (id INT, value STRING) TBLPROPERTIES ($options)")
          sql("INSERT INTO target VALUES (1,'old')")
          // PK targets need not expose the DE metadata column.
          val before = sql("SELECT id,value FROM target").collect().toSeq
          val baseline = snapshotId()
          val failure = intercept[Exception](replace("SELECT 1 AS id,'new' AS value"))
          val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
          assert(
            causes.exists(e => Option(e.getMessage).exists(_.contains(expectedError))),
            s"$mode must fail at the target-mode guard")
          assert(snapshotId() == baseline)
          checkAnswer(sql("SELECT id,value FROM target"), before)
        }
    }
  }

  Seq(false, true).foreach {
    ansi =>
      test(s"non-key malformed string follows ordinary INSERT and remains atomic with ansi=$ansi") {
        withSparkSQLConf(
          "spark.sql.ansi.enabled" -> ansi.toString,
          "spark.sql.storeAssignmentPolicy" -> "LEGACY") {
          withTable("target", "source", "assignment") {
            createTarget("id INT, value INT")
            sql("INSERT INTO target VALUES (1,10),(2,20)")
            sql("CREATE TABLE source (id INT, value STRING)")
            sql("INSERT INTO source VALUES (1,'bad')")
            sql(s"CREATE TABLE assignment (id INT, value INT) TBLPROPERTIES ($deOptions)")
            val before = sql("SELECT *,_ROW_ID FROM target").collect().toSeq
            val untouched = sql("SELECT *,_ROW_ID FROM target WHERE id=2").collect().toSeq
            val baseline = snapshotId()
            val oracleFailure: Option[Exception] =
              try {
                sql("INSERT INTO assignment SELECT * FROM source")
                None
              } catch {
                case failure: Exception => Some(failure)
              }
            if (oracleFailure.isEmpty) {
              val assigned = sql("SELECT id,value FROM assignment").collect().toSeq
              assert(assigned.size == 1)
              replace("SELECT * FROM source")
              checkAnswer(sql("SELECT id,value FROM target"), Seq(Row(2, 20)) ++ assigned)
              checkAnswer(sql("SELECT *,_ROW_ID FROM target WHERE id=2"), untouched)
              assert(snapshotId() == baseline + 1)
            } else {
              intercept[Exception](replace("SELECT * FROM source"))
              assert(snapshotId() == baseline)
              checkAnswer(sql("SELECT *,_ROW_ID FROM target"), before)
            }
          }
        }
      }
  }

  // Publication failures have an unknown outcome. No lightweight commit-failure hook exists here;
  // do not add a production hook or assert rollback after a publication exception.
}
