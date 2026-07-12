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

package org.apache.paimon.spark.sql

import org.apache.paimon.data.{BinaryString, GenericRow, Timestamp}
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.spark.catalyst.plans.logical.{LateralVectorSearch, PaimonTableValuedFunctions}
import org.apache.paimon.utils.DateTimeUtils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.Filter

import java.time.LocalDateTime
import java.util.Collections

class TableValuedFunctionsTest extends PaimonHiveTestBase {

  test("parse positive limit rejects overflowing long") {
    val longValue: Long = 4294967297L
    assert(longValue.toInt > 0)

    val error = intercept[IllegalArgumentException] {
      PaimonTableValuedFunctions.parsePositiveLimit(longValue)
    }
    assert(error.getMessage.contains("Limit must be no greater than"))
  }

  test("lateral vector search preserves subquery alias qualifiers") {
    withTable("vector_search_source", "vector_search_result") {
      spark.sql("""
                  |CREATE TABLE vector_search_source (gid BIGINT, embs ARRAY<FLOAT>, dt STRING)
                  |USING paimon
                  |TBLPROPERTIES (
                  |  'vector.file.format' = 'lance',
                  |  'vector-field' = 'embs',
                  |  'field.embs.vector-dim' = '3',
                  |  'row-tracking.enabled' = 'true',
                  |  'data-evolution.enabled' = 'true')
                  |PARTITIONED BY (dt)
                  |""".stripMargin)
      spark.sql("""
                  |CREATE TABLE vector_search_result (
                  |  query_gid BIGINT,
                  |  query_embs ARRAY<FLOAT>,
                  |  result_gid BIGINT,
                  |  result_embs ARRAY<FLOAT>,
                  |  score FLOAT,
                  |  dt STRING)
                  |USING paimon
                  |PARTITIONED BY (dt)
                  |""".stripMargin)

      val insertOptimizedPlan = spark
        .sql("""
               |SELECT q.gid AS query_gid, q.embs AS query_embs,
               |       r.gid AS result_gid, r.embs AS result_embs,
               |       r.__paimon_search_score AS score
               |FROM vector_search_source AS q,
               |LATERAL (
               |  SELECT gid, embs, __paimon_search_score
               |  FROM vector_search('vector_search_source', 'embs', q.embs, 5)
               |) AS r
               |WHERE q.dt = '20260608'
               |""".stripMargin)
        .queryExecution
        .optimizedPlan
      val lateralVectorSearches = insertOptimizedPlan.collect {
        case lvs: LateralVectorSearch => lvs
      }
      assert(lateralVectorSearches.size == 1, insertOptimizedPlan.toString)

      val optimizedPlanWithoutScore = spark
        .sql("""
               |SELECT q.gid AS query_gid, r.embs AS result_embs
               |FROM vector_search_source AS q,
               |LATERAL (
               |  SELECT embs
               |  FROM vector_search('vector_search_source', 'embs', q.embs, 5)
               |) AS r
               |""".stripMargin)
        .queryExecution
        .optimizedPlan
      assert(
        optimizedPlanWithoutScore.exists(_.isInstanceOf[LateralVectorSearch]),
        optimizedPlanWithoutScore.toString)

      val analyzedPlanWithJoinCondition = spark
        .sql("""
               |SELECT q.gid AS query_gid, r.result_gid, r.score
               |FROM vector_search_source AS q,
               |LATERAL (
               |  SELECT gid AS result_gid, __paimon_search_score AS score
               |  FROM vector_search('vector_search_source', 'embs', q.embs, 5)
               |) AS r
               |WHERE q.gid = r.result_gid AND r.score >= 0.0
               |""".stripMargin)
        .queryExecution
        .analyzed
      val lateralVectorSearchFilters = analyzedPlanWithJoinCondition.collect {
        case filter @ Filter(_, _: LateralVectorSearch) => filter
      }
      assert(lateralVectorSearchFilters.size == 1, analyzedPlanWithJoinCondition.toString)
      assert(
        lateralVectorSearchFilters.head.condition.references
          .subsetOf(lateralVectorSearchFilters.head.child.outputSet),
        analyzedPlanWithJoinCondition.toString
      )

      val optimizedPlanWithSearchFilter = spark
        .sql("""
               |SELECT q.gid AS query_gid, r.result_gid, r.dt
               |FROM vector_search_source AS q,
               |LATERAL (
               |  SELECT gid AS result_gid, dt
               |  FROM vector_search('vector_search_source', 'embs', q.embs, 5)
               |) AS r
               |WHERE r.dt = '20260608'
               |""".stripMargin)
        .queryExecution
        .optimizedPlan
      val lateralVectorSearchesWithSearchFilter = optimizedPlanWithSearchFilter.collect {
        case lvs: LateralVectorSearch => lvs
      }
      assert(
        lateralVectorSearchesWithSearchFilter.size == 1,
        optimizedPlanWithSearchFilter.toString)
      assert(
        lateralVectorSearchesWithSearchFilter.head.searchFilters.nonEmpty,
        optimizedPlanWithSearchFilter.toString)

      val optimizedPlanWithSubqueryFilter = spark
        .sql("""
               |SELECT q.gid AS query_gid, r.result_gid
               |FROM vector_search_source AS q,
               |LATERAL (
               |  SELECT gid AS result_gid
               |  FROM vector_search('vector_search_source', 'embs', q.embs, 5)
               |  WHERE dt = '20260608'
               |) AS r
               |""".stripMargin)
        .queryExecution
        .optimizedPlan
      val lateralVectorSearchesWithSubqueryFilter = optimizedPlanWithSubqueryFilter.collect {
        case lvs: LateralVectorSearch => lvs
      }
      assert(
        lateralVectorSearchesWithSubqueryFilter.size == 1,
        optimizedPlanWithSubqueryFilter.toString)
      assert(
        lateralVectorSearchesWithSubqueryFilter.head.searchFilters.nonEmpty,
        optimizedPlanWithSubqueryFilter.toString)

      val optimizedPlanWithoutQueryVector = spark
        .sql("""
               |SELECT q.gid AS query_gid, r.gid AS result_gid
               |FROM vector_search_source AS q,
               |LATERAL (
               |  SELECT gid
               |  FROM vector_search('vector_search_source', 'embs', q.embs, 5)
               |) AS r
               |""".stripMargin)
        .queryExecution
        .optimizedPlan
      val lateralVectorSearchesWithoutQueryVector =
        optimizedPlanWithoutQueryVector.collect { case lvs: LateralVectorSearch => lvs }
      assert(
        lateralVectorSearchesWithoutQueryVector.size == 1,
        "Query vector column not in outer SELECT should not break lateral vector search: " +
          optimizedPlanWithoutQueryVector.toString
      )

      val constantVectorPlan = spark
        .sql("""
               |SELECT gid
               |FROM vector_search(
               |  'vector_search_source', 'embs', array(1.0f, 2.0f, 3.0f), 5)
               |""".stripMargin)
        .queryExecution
        .optimizedPlan
      assert(
        !constantVectorPlan.exists(_.isInstanceOf[LateralVectorSearch]),
        constantVectorPlan.toString)
    }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"incremental query: hasPk: $hasPk, bucket: $bucket") {
            Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
              catalogName =>
                sql(s"use $catalogName")

                withTable("t") {
                  val prop = if (hasPk) {
                    s"'primary-key'='a,b', 'bucket' = '$bucket' "
                  } else if (bucket != -1) {
                    s"'bucket-key'='b', 'bucket' = '$bucket' "
                  } else {
                    "'write-only'='true'"
                  }

                  spark.sql(s"""
                               |CREATE TABLE t (a INT, b INT, c STRING)
                               |USING paimon
                               |TBLPROPERTIES ($prop)
                               |PARTITIONED BY (a)
                               |""".stripMargin)

                  spark.sql("INSERT INTO t values (1, 1, '1'), (2, 2, '2')")
                  spark.sql("INSERT INTO t VALUES (1, 3, '3'), (2, 4, '4')")
                  spark.sql("INSERT INTO t VALUES (1, 5, '5'), (1, 7, '7')")

                  checkAnswer(
                    incrementalDF("t", 0, 1).orderBy("a", "b"),
                    Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '0', '1') ORDER BY a, b"),
                    Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

                  checkAnswer(
                    incrementalDF("t", 1, 2).orderBy("a", "b"),
                    Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '1', '2') ORDER BY a, b"),
                    Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)

                  checkAnswer(
                    incrementalDF("t", 2, 3).orderBy("a", "b"),
                    Row(1, 5, "5") :: Row(1, 7, "7") :: Nil)
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '2', '3') ORDER BY a, b"),
                    Row(1, 5, "5") :: Row(1, 7, "7") :: Nil)

                  checkAnswer(
                    incrementalDF("t", 1, 3).orderBy("a", "b"),
                    Row(1, 3, "3") :: Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil
                  )
                  checkAnswer(
                    spark.sql(
                      "SELECT * FROM paimon_incremental_query('t', '1', '3') ORDER BY a, b"),
                    Row(1, 3, "3") :: Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil)
                }
            }
          }
      }
  }

  test("Table Valued Functions: paimon_incremental_between_timestamp") {
    Seq("paimon", sparkCatalogName, paimonHiveCatalogName).foreach {
      catalogName =>
        sql(s"USE $catalogName")
        val dbName = "test_tvf_db"
        withDatabase(dbName) {
          sql(s"CREATE DATABASE $dbName")
          withTable("t") {
            sql(s"USE $dbName")
            sql("CREATE TABLE t (id INT) USING paimon")

            sql("INSERT INTO t VALUES 1")
            Thread.sleep(100)
            val t1 = System.currentTimeMillis()
            sql("INSERT INTO t VALUES 2")
            Thread.sleep(100)
            val t2 = System.currentTimeMillis()
            sql("INSERT INTO t VALUES 3")
            sql("INSERT INTO t VALUES 4")
            Thread.sleep(100)
            val t3 = System.currentTimeMillis()
            sql("INSERT INTO t VALUES 5")

            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('t', '$t1', '$t2') ORDER BY id"),
              Seq(Row(2)))
            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('$dbName.t', '$t2', '$t3') ORDER BY id"),
              Seq(Row(3), Row(4)))
            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('$catalogName.$dbName.t', '$t1', '$t3') ORDER BY id"),
              Seq(Row(2), Row(3), Row(4)))
            val t1String = DateTimeUtils.formatLocalDateTime(DateTimeUtils.toLocalDateTime(t1), 3)
            val t3String = DateTimeUtils.formatLocalDateTime(DateTimeUtils.toLocalDateTime(t3), 3)
            checkAnswer(
              sql(
                s"SELECT * FROM paimon_incremental_between_timestamp('$catalogName.$dbName.t', '$t1String', '$t3String') ORDER BY id"),
              Seq(Row(2), Row(3), Row(4))
            )
          }
        }
    }
  }

  test("Table Valued Functions: paimon_incremental_to_auto_tag") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (a INT, b STRING) USING paimon
            |TBLPROPERTIES ('primary-key' = 'a', 'bucket' = '1', 'tag.automatic-creation'='watermark', 'tag.creation-period'='daily')
            |""".stripMargin)

      val table = loadTable("t")
      val write = table.newWrite(commitUser)
      val commit = table.newCommit(commitUser).ignoreEmptyCommit(false)

      write.write(GenericRow.of(1, BinaryString.fromString("a")))
      var commitMessages = write.prepareCommit(false, 0)
      commit.commit(new ManifestCommittable(0, utcMills("2024-12-02T10:00:00"), commitMessages))

      write.write(GenericRow.of(2, BinaryString.fromString("b")))
      commitMessages = write.prepareCommit(false, 1)
      commit.commit(new ManifestCommittable(1, utcMills("2024-12-03T10:00:00"), commitMessages))

      write.write(GenericRow.of(3, BinaryString.fromString("c")))
      commitMessages = write.prepareCommit(false, 2)
      commit.commit(new ManifestCommittable(2, utcMills("2024-12-05T10:00:00"), commitMessages))

      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-01') ORDER BY a"),
        Seq())
      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-02') ORDER BY a"),
        Seq(Row(2, "b")))
      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-03') ORDER BY a"),
        Seq())
      checkAnswer(
        sql(s"SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-12-04') ORDER BY a"),
        Seq(Row(3, "c")))
    }
  }

  test("Table Valued Functions: incremental query with inconsistent tag bucket") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (a INT, b INT) USING paimon
            |TBLPROPERTIES ('primary-key'='a', 'bucket' = '1')
            |""".stripMargin)

      val table = loadTable("t")

      sql("INSERT INTO t VALUES (1, 11), (2, 22)")
      table.createTag("2024-01-01", 1)

      sql("ALTER TABLE t SET TBLPROPERTIES ('bucket' = '2')")
      sql("INSERT OVERWRITE t SELECT * FROM t")

      sql("INSERT INTO t VALUES (3, 33)")
      table.createTag("2024-01-03", 3)

      sql("DELETE FROM t WHERE a = 1")
      table.createTag("2024-01-04", 4)

      sql("UPDATE t SET b = 222 WHERE a = 2")
      table.createTag("2024-01-05", 5)

      checkAnswer(
        sql(
          "SELECT * FROM paimon_incremental_query('t', '2024-01-01', '2024-01-03') ORDER BY a, b"),
        Seq(Row(3, 33)))

      checkAnswer(
        sql("SELECT * FROM paimon_incremental_to_auto_tag('t', '2024-01-03') ORDER BY a, b"),
        Seq(Row(3, 33)))

      checkAnswer(
        sql(
          "SELECT * FROM paimon_incremental_query('t', '2024-01-01', '2024-01-04') ORDER BY a, b"),
        Seq(Row(3, 33)))

      checkAnswer(
        sql(
          "SELECT * FROM paimon_incremental_query('t', '2024-01-01', '2024-01-05') ORDER BY a, b"),
        Seq(Row(2, 222), Row(3, 33)))

      checkAnswer(
        sql(
          "SELECT * FROM paimon_incremental_query('`t$audit_log`', '2024-01-01', '2024-01-04') ORDER BY a, b"),
        Seq(Row("-D", 1, 11), Row("+I", 3, 33)))

      checkAnswer(
        sql(
          "SELECT * FROM paimon_incremental_query('`t$audit_log`', '2024-01-01', '2024-01-05') ORDER BY a, b"),
        Seq(Row("-D", 1, 11), Row("+U", 2, 222), Row("+I", 3, 33))
      )
    }
  }

  test("Table Valued Functions: incremental query with delete after minor compact") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT) USING paimon
            |TBLPROPERTIES ('primary-key'='id', 'bucket' = '1', 'write-only' = 'true')
            |""".stripMargin)

      sql("INSERT INTO t SELECT /*+ REPARTITION(1) */ id FROM range (1, 100001)")
      sql("CALL sys.compact(table => 't')")
      sql("INSERT INTO t VALUES 100001")
      sql("INSERT INTO t VALUES 100002")
      sql("CALL sys.create_tag('t', 'tag1')")

      sql(
        "CALL sys.compact(table => 't', compact_strategy => 'minor', options => 'num-sorted-run.compaction-trigger=2')")
      sql("DELETE FROM t WHERE id = 999")
      sql("CALL sys.create_tag('t', 'tag2')")

      //            tag1                          tag2
      // l0         f(+I 10001),f(+I 10002)       f(-D 999)
      // l1
      // l2
      // l3
      // l4                                       f(+I 10001,10002)
      // l5         f(+I 1-10000)                 f(+I 1-10000)
      checkAnswer(
        sql("SELECT level FROM `t$files` VERSION AS OF 'tag1' ORDER BY level"),
        Seq(Row(0), Row(0), Row(5)))
      checkAnswer(
        sql("SELECT level FROM `t$files` VERSION AS OF 'tag2' ORDER BY level"),
        Seq(Row(0), Row(4), Row(5)))

      // before files: f(+I 10001), f(+I 10002)
      // after files:  f(-D 999),   f(+I 10001,10002)
      checkAnswer(
        sql("SELECT * FROM paimon_incremental_query('`t$audit_log`', 'tag1', 'tag2') ORDER BY id"),
        Seq(Row("-D", 999)))
    }
  }

  test("Table Valued Functions: incremental query with delete after compact") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT) USING paimon
            |TBLPROPERTIES ('primary-key'='id', 'bucket' = '1', 'write-only' = 'true')
            |""".stripMargin)

      sql("INSERT INTO t VALUES 1")
      sql("INSERT INTO t VALUES 2")
      sql("CALL sys.create_tag('t', 'tag1')")

      sql("CALL sys.compact(table => 't')")
      sql("DELETE FROM t WHERE id = 1")
      sql("CALL sys.create_tag('t', 'tag2')")

      //         tag1                    tag2
      // l0      f(+I 1),f(+I 2)         f(-D 1)
      // l1
      // l2
      // l3
      // l4
      // l5                              f(+I 1,2)
      checkAnswer(
        sql("SELECT level FROM `t$files` VERSION AS OF 'tag1' ORDER BY level"),
        Seq(Row(0), Row(0)))
      checkAnswer(
        sql("SELECT level FROM `t$files` VERSION AS OF 'tag2' ORDER BY level"),
        Seq(Row(0), Row(5)))

      checkAnswer(
        sql("SELECT * FROM paimon_incremental_query('`t$audit_log`', 'tag1', 'tag2') ORDER BY id"),
        Seq(Row("-D", 1)))
    }
  }

  test("Table Valued Functions: incremental query with delete after compact2") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT) USING paimon
            |TBLPROPERTIES ('primary-key'='id', 'bucket' = '1', 'write-only' = 'true')
            |""".stripMargin)

      sql("INSERT INTO t VALUES 1")
      sql("DELETE FROM t WHERE id = 1")
      sql("CALL sys.create_tag('t', 'tag1')")

      sql("CALL sys.compact(table => 't')")
      sql("INSERT INTO t VALUES 1")
      sql("DELETE FROM t WHERE id = 1")
      sql("CALL sys.create_tag('t', 'tag2')")

      //         tag1                    tag2
      // l0      f(+I 1),f(-D 1)         f(+I 1),f(-D 1)
      checkAnswer(
        sql("SELECT level FROM `t$files` VERSION AS OF 'tag1' ORDER BY level"),
        Seq(Row(0), Row(0)))
      checkAnswer(
        sql("SELECT level FROM `t$files` VERSION AS OF 'tag2' ORDER BY level"),
        Seq(Row(0), Row(0)))

      checkAnswer(
        sql("SELECT * FROM paimon_incremental_query('`t$audit_log`', 'tag1', 'tag2') ORDER BY id"),
        Seq())
    }
  }

  test("incremental query by tag with LIMIT") {
    sql("use paimon")
    withTable("t") {
      spark.sql("""
                  |CREATE TABLE t (a INT, b INT, c STRING)
                  |USING paimon
                  |TBLPROPERTIES ('primary-key'='a,b', 'bucket' = '2')
                  |PARTITIONED BY (a)
                  |""".stripMargin)
      spark.sql("INSERT INTO t VALUES (1, 1, '1'), (2, 2, '2')")
      sql("CALL sys.create_tag('t', 'tag1')")
      spark.sql("INSERT INTO t VALUES (1, 3, '3'), (2, 4, '4')")
      sql("CALL sys.create_tag('t', 'tag2')")

      checkAnswer(
        spark.sql(
          "SELECT * FROM paimon_incremental_query('t', 'tag1', 'tag2') ORDER BY a, b LIMIT 5"),
        Seq(Row(1, 3, "3"), Row(2, 4, "4")))
    }
  }

  private def incrementalDF(tableIdent: String, start: Int, end: Int): DataFrame = {
    spark.read
      .format("paimon")
      .option("incremental-between", s"$start,$end")
      .table(tableIdent)
  }

  private def utcMills(timestamp: String) =
    Timestamp.fromLocalDateTime(LocalDateTime.parse(timestamp)).getMillisecond
}
