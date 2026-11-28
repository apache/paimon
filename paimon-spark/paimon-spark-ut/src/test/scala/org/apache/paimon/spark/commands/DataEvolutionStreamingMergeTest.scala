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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.read.PaimonSplitScan
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

class DataEvolutionStreamingMergeTest extends PaimonSparkTestBase {

  Seq("off", "lightweight", "streaming").foreach {
    mode =>
      test(s"MERGE preparation $mode preserves versioned columns, partition pruning and RowIDs") {
        withTable("t", "s") {
          sql(s"""
                 |CREATE TABLE t (id INT, p INT, b INT, c INT) PARTITIONED BY (p)
                 |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
                 | 'data-evolution.enabled'='true', 'write-only'='true',
                 | 'data-evolution.metadata-memory-optimization.mode'='$mode')
                 |""".stripMargin)
          sql("INSERT INTO t VALUES (1, 1, 10, 100)")
          sql("INSERT INTO t VALUES (2, 1, 20, 200)")
          sql("INSERT INTO t VALUES (3, 2, 30, 300)")
          val original = sql("SELECT id, _ROW_ID FROM t ORDER BY id").collect().toSeq
          sql(
            "MERGE INTO t USING (SELECT 1 AS id, 11 AS b UNION ALL SELECT 3, 31) u " +
              "ON t.id = u.id WHEN MATCHED THEN UPDATE SET b = u.b")
          sql("CREATE TABLE s (id INT, p INT, b INT, c INT)")
          sql(
            "INSERT INTO s VALUES (1, 1, 111, 1111), (2, 1, 222, 2222), " +
              "(3, 2, 333, 3333), (4, 1, 444, 4444)")
          val before = loadTable("t").latestSnapshot().get().id()
          val storedSplits = new ConcurrentLinkedQueue[Boolean]()
          val listener = new QueryExecutionListener {
            override def onSuccess(name: String, qe: QueryExecution, nanos: Long): Unit = {
              qe.optimizedPlan.foreach {
                case relation: DataSourceV2ScanRelation =>
                  relation.scan match {
                    case known: PaimonSplitScan =>
                      known.inputSplits.collect { case split: DataSplit => split }.foreach {
                        split =>
                          storedSplits.add(
                            split
                              .dataFiles()
                              .getClass
                              .getName
                              .contains("DataFileMetaStore$Slice"))
                      }
                    case _ =>
                  }
                case _ =>
              }
            }
            override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {}
          }
          spark.listenerManager.register(listener)
          try {
            sql("""
                  |MERGE INTO t USING s
                  |ON t.id = s.id AND t.p = s.p AND t.p = 1 AND t.b >= 11
                  |WHEN MATCHED THEN UPDATE SET b = s.b
                  |WHEN NOT MATCHED THEN INSERT (id, p, b, c) VALUES (s.id, s.p, s.b, s.c)
                  |""".stripMargin)
            Utils.waitUntilEventEmpty(spark)
            assert(!storedSplits.isEmpty, "Expected an actual KnownSplits consumer during MERGE")
            assert(storedSplits.asScala.forall(_ == (mode == "streaming")))
          } finally {
            spark.listenerManager.unregister(listener)
          }
          assert(loadTable("t").latestSnapshot().get().id() == before + 1)
          checkAnswer(
            sql("SELECT id, p, b, c FROM t"),
            Seq(
              Row(1, 1, 111, 100),
              Row(2, 1, 222, 200),
              Row(3, 2, 31, 300),
              Row(3, 2, 333, 3333),
              Row(4, 1, 444, 4444)))
          checkAnswer(sql("SELECT id, _ROW_ID FROM t WHERE c < 1000"), original)
          val inserted = sql("SELECT _ROW_ID FROM t WHERE c >= 1000").collect().map(_.getLong(0))
          assert(inserted.toSet == Set(3L, 4L) && inserted.length == 2)
          checkAnswer(
            sql("SELECT * FROM s"),
            Seq(
              Row(1, 1, 111, 1111),
              Row(2, 1, 222, 2222),
              Row(3, 2, 333, 3333),
              Row(4, 1, 444, 4444)))
        }
      }
  }

  test("streaming MERGE keeps NMBS independent of ON partition pruning across repeated deletes") {
    withTable("t") {
      withTempView("s") {
        sql("""
              |CREATE TABLE t (id INT, p INT, b INT, c INT) PARTITIONED BY (p)
              |TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',
              | 'data-evolution.enabled'='true', 'write-only'='true',
              | 'deletion-vectors.enabled'='true',
              | 'data-evolution.metadata-memory-optimization.mode'='streaming')
              |""".stripMargin)
        sql(
          "INSERT INTO t VALUES (1, 1, 10, 100), (2, 1, 20, 200), " +
            "(3, 2, 30, 300), (4, 2, 40, 400)")
        val original = sql("SELECT id, _ROW_ID FROM t WHERE id IN (1, 4)").collect().toSeq
        sql(
          "CREATE OR REPLACE TEMP VIEW s AS SELECT * FROM VALUES " +
            "(1, 1, 111), (5, 1, 555) AS s(id, p, b)")
        val firstSnapshot = loadTable("t").latestSnapshot().get().id()
        sql("""
              |MERGE INTO t USING s ON t.id = s.id AND t.p = s.p AND t.p = 1
              |WHEN MATCHED THEN UPDATE SET b = s.b
              |WHEN NOT MATCHED THEN INSERT (id, p, b, c) VALUES (s.id, s.p, s.b, s.b + 1000)
              |WHEN NOT MATCHED BY SOURCE AND t.id IN (2, 3) THEN DELETE
              |""".stripMargin)
        assert(loadTable("t").latestSnapshot().get().id() == firstSnapshot + 1)
        checkAnswer(
          sql("SELECT id, p, b, c FROM t"),
          Seq(Row(1, 1, 111, 100), Row(4, 2, 40, 400), Row(5, 1, 555, 1555)))
        checkAnswer(sql("SELECT id, _ROW_ID FROM t WHERE id IN (1, 4)"), original)
        sql(
          "CREATE OR REPLACE TEMP VIEW s AS SELECT * FROM VALUES " +
            "(1, 1, 112), (2, 1, 222), (6, 1, 666) AS s(id, p, b)")
        val secondSnapshot = loadTable("t").latestSnapshot().get().id()
        sql("""
              |MERGE INTO t USING s ON t.id = s.id AND t.p = s.p AND t.p = 1
              |WHEN MATCHED THEN UPDATE SET b = s.b
              |WHEN NOT MATCHED THEN INSERT (id, p, b, c) VALUES (s.id, s.p, s.b, s.b + 1000)
              |WHEN NOT MATCHED BY SOURCE AND t.id IN (4, 5) THEN DELETE
              |""".stripMargin)
        assert(loadTable("t").latestSnapshot().get().id() == secondSnapshot + 1)
        checkAnswer(
          sql("SELECT id, p, b, c FROM t"),
          Seq(Row(1, 1, 112, 100), Row(2, 1, 222, 1222), Row(6, 1, 666, 1666)))
        checkAnswer(
          sql("SELECT id, _ROW_ID FROM t WHERE id = 1"),
          original.filter(_.getInt(0) == 1))
        val fresh = sql("SELECT _ROW_ID FROM t WHERE id IN (2, 6)").collect().map(_.getLong(0))
        assert(fresh.length == 2 && fresh.distinct.length == 2 && fresh.forall(_ >= 5L))
      }
    }
  }

  test("streaming MERGE enforces its initial planning disk budget before publication") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES (
            | 'bucket'='-1', 'row-tracking.enabled'='true',
            | 'data-evolution.enabled'='true', 'write-only'='true',
            | 'data-evolution.merge-into.file-pruning'='false',
            | 'data-evolution.metadata-memory-optimization.mode'='streaming',
            | 'data-evolution.metadata-memory-optimization.max-disk-size'='8 b')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 10, 100)")
      val before = loadTable("t").latestSnapshot().get().id()
      val failure = intercept[Exception] {
        sql(
          "MERGE INTO t USING (SELECT 1 AS id, 111 AS b) s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET b = s.b")
      }
      assert(
        Iterator
          .iterate[Throwable](failure)(_.getCause)
          .takeWhile(_ != null)
          .exists(
            error => Option(error.getMessage).exists(_.contains("Metadata store disk limit"))))
      assert(loadTable("t").latestSnapshot().get().id() == before)
      sql(
        "ALTER TABLE t SET TBLPROPERTIES ('data-evolution.metadata-memory-optimization.mode'='off')")
      checkAnswer(sql("SELECT id, b, c, _ROW_ID FROM t"), Seq(Row(1, 10, 100, 0L)))
    }
  }
}
