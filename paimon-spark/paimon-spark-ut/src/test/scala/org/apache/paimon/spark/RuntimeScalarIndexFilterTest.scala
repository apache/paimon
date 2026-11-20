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

package org.apache.paimon.spark

import org.apache.paimon.data.GenericRow
import org.apache.paimon.fs.Path
import org.apache.paimon.globalindex.{IndexedSplit, IndexQuerySplit}
import org.apache.paimon.table.AppendOnlyFileStoreTable
import org.apache.paimon.table.sink.CommitMessageImpl

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.mockito.Mockito.{atLeastOnce, clearInvocations, mockingDetails, never, spy, verify}

import scala.collection.JavaConverters._

class RuntimeScalarIndexFilterTest extends PaimonSparkTestBase {

  test("ordinary fallback reads preserve a branch snapshot newer than main") {
    withTable("runtime_fact") {
      sql("""CREATE TABLE runtime_fact (id INT, p STRING) PARTITIONED BY (p)
            |TBLPROPERTIES ('row-tracking.enabled'='true',
            |'data-evolution.enabled'='true', 'bucket'='-1')""".stripMargin)
      sql("CALL sys.create_branch('test.runtime_fact', 'fallback')")
      sql("INSERT INTO runtime_fact VALUES (1, 'main')")
      sql("INSERT INTO `runtime_fact$branch_fallback` VALUES (2, 'fallback')")
      sql("ALTER TABLE runtime_fact SET TBLPROPERTIES ('scan.fallback-branch'='fallback')")
      checkAnswer(sql("SELECT id, p FROM runtime_fact"), Seq(Row(1, "main"), Row(2, "fallback")))
    }
  }

  Seq(false, true).foreach {
    queryInReader =>
      test(s"runtime keys cannot select an incomplete composite index, reader: $queryInReader") {
        withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
          withTable("runtime_fact", "runtime_dim") {
            sql(s"""CREATE TABLE runtime_fact (id INT, k INT, c INT) TBLPROPERTIES (
                   |'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
                   |'global-index.query-in-reader.enabled'='$queryInReader')""".stripMargin)
            sql("""INSERT INTO runtime_fact SELECT /*+ COALESCE(1) */
                  |CAST(id AS INT), CAST(id % 10 AS INT), 10 FROM range(100)""".stripMargin)
            sql(
              "CALL sys.create_global_index(table => 'test.runtime_fact', " +
                "index_column => 'k,c', index_type => 'btree')")
            sql("INSERT INTO runtime_fact VALUES (100, 2, 10)")
            sql(
              "CALL sys.create_global_index(table => 'test.runtime_fact', " +
                "index_column => 'k', index_type => 'btree')")
            sql("CREATE TABLE runtime_dim (k INT, keep INT)")
            sql("INSERT INTO runtime_dim VALUES (2, 1), (7, 0)")
            val query = """SELECT /*+ BROADCAST(d) */ f.id
                          |FROM runtime_fact f JOIN runtime_dim d ON f.k = d.k
                          |WHERE d.keep = 1 AND f.c = 10""".stripMargin
            Seq(false, true).foreach {
              enabled =>
                withSparkSQLConf(
                  "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> enabled.toString) {
                  checkAnswer(sql(query), (2 until 100 by 10).map(Row(_)) :+ Row(100))
                }
            }
          }
        }
      }
  }

  test("capability discovery reads one index manifest and defers scoped freshness scans") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact") {
        sql(
          """CREATE TABLE runtime_fact (id INT, k INT, c INT, p INT) PARTITIONED BY (p)
            |TBLPROPERTIES ('row-tracking.enabled'='true', 'data-evolution.enabled'='true')""".stripMargin)
        (0 until 3).foreach(p => sql(s"INSERT INTO runtime_fact VALUES ($p, 2, 3, $p)"))
        Seq("k", "c").foreach {
          field =>
            sql(
              s"CALL sys.create_global_index(table => 'test.runtime_fact', " +
                s"index_column => '$field', index_type => 'btree')")
        }
        val original = loadTable("runtime_fact")
        val fileIO = spy(original.fileIO())
        val table = new AppendOnlyFileStoreTable(
          fileIO,
          original.location(),
          original.schema(),
          original.catalogEnvironment())
        val scan = factScan("SELECT id, k, c, p FROM runtime_fact WHERE p = 1").copy(table = table)
        def reads: Seq[String] = mockingDetails(fileIO).getInvocations.asScala
          .filter(_.getMethod.getName == "newInputStream")
          .map(_.getArgument[Path](0).getName)
          .toSeq
        def dataReads: Seq[String] = reads.filter(
          n =>
            n.startsWith("manifest-") &&
              !n.startsWith("manifest-list-"))
        assert(scan.filterAttributes().map(_.fieldNames().head).toSet == Set("p", "k", "c"))
        assert(reads.count(_.startsWith("index-manifest-")) == 1)
        assert(dataReads.isEmpty)
        clearInvocations(fileIO)
        scan.filter(Array(in(2)))
        assert(dataReads.nonEmpty)
        assert(dataReads.distinct.size == 1)
        assert(!reads.exists(_.startsWith("index-manifest-")))
        assert(readIds(scan.toBatch, scan.toBatch.createReaderFactory()) == Seq(1))
      }
    }
  }

  test("a complete bitmap cannot hide incomplete coverage of the same column's btree") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact") {
        createFact()
        sql("INSERT INTO runtime_fact VALUES (100, 2)")
        sql(
          "CALL sys.create_global_index(table => 'test.runtime_fact', " +
            "index_column => 'k', index_type => 'bitmap')")
        val scan = factScan("SELECT id, k FROM runtime_fact")
        assert(scan.filterAttributes().isEmpty)
        scan.filter(Array(in(2)))
        assert(readIds(scan.toBatch, scan.toBatch.createReaderFactory()).size == 31)
      }
    }
  }

  Seq(("btree", false), ("bitmap", false), ("btree", true), ("bitmap", true)).foreach {
    case (indexType, queryInReader) =>
      test(s"broadcast join uses non-partition $indexType index, query in reader: $queryInReader") {
        withSparkSQLConf(
          "spark.sql.adaptive.enabled" -> "false",
          "spark.paimon.write.use-v2-write" -> "false") {
          withTable("runtime_fact", "runtime_dim") {
            sql("""CREATE TABLE runtime_fact (id INT, k INT) TBLPROPERTIES (
                  |'row-tracking.enabled'='true', 'data-evolution.enabled'='true')""".stripMargin)
            sql(
              s"ALTER TABLE runtime_fact SET TBLPROPERTIES " +
                s"('global-index.query-in-reader.enabled'='$queryInReader')")
            sql("""INSERT INTO runtime_fact SELECT /*+ COALESCE(1) */
                  |CAST(id AS INT), CAST(id % 10 AS INT) FROM range(100)""".stripMargin)
            sql(
              s"CALL sys.create_global_index(table => 'test.runtime_fact', " +
                s"index_column => 'k', index_type => '$indexType')")
            sql("CREATE TABLE runtime_dim (k INT, keep INT)")
            sql("INSERT INTO runtime_dim VALUES (2, 1), (2, 1), (7, 0), (NULL, 1)")
            val query = """SELECT /*+ BROADCAST(d) */ f.id
                          |FROM runtime_fact f JOIN runtime_dim d ON f.k = d.k
                          |WHERE d.keep = 1 AND f.id >= 10""".stripMargin
            val expected = (12 until 100 by 10).flatMap(i => Seq(Row(i), Row(i)))
            var unfilteredRows = 0L
            Seq(false, true).foreach {
              enabled =>
                withSparkSQLConf(
                  "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> enabled.toString) {
                  val result = sql(query)
                  val scan = result.queryExecution.executedPlan.collectFirst {
                    case s: BatchScanExec if s.output.exists(_.name == "id") => s
                  }.get
                  if (enabled) {
                    assert(
                      scan.runtimeFilters.nonEmpty,
                      result.queryExecution.executedPlan.toString)
                  }
                  checkAnswer(result, expected)
                  if (enabled) {
                    assert(scan.metrics("numOutputRows").value < unfilteredRows)
                    val paimonScan = scan.scan.asInstanceOf[PaimonScan]
                    val indexedRows = paimonScan.inputSplits.map {
                      case split: IndexedSplit => split.rowCount()
                      case split: IndexQuerySplit =>
                        split.evaluate(loadTable("runtime_fact").fileIO()).rowCount()
                      case other => fail(s"Expected an index split, found $other")
                    }.sum
                    assert(indexedRows <= 10)
                  } else {
                    unfilteredRows = scan.metrics("numOutputRows").value
                  }
                }
            }
          }
        }
      }
  }

  test("runtime replanning keeps the original snapshot, static predicate and reader factory") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact") {
        createFact()
        val scan = factScan("SELECT id, k FROM runtime_fact WHERE k >= 2")
        assert(scan.filterAttributes().exists(_.fieldNames().toSeq == Seq("k")))
        val originalBatch = scan.toBatch
        val originalFactory = originalBatch.createReaderFactory()
        sql("INSERT INTO runtime_fact VALUES (200, 2)")

        scan.filter(Array(in(1, 2)))
        assert(readIds(scan.toBatch, originalFactory) == Seq(2, 12, 22))
        scan.filter(Array(in(7)))
        assert(readIds(scan.toBatch, originalFactory).isEmpty)
      }
    }
  }

  test("incomplete coverage and unsafe scan shapes keep ordinary scans") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact") {
        createFact()
        val scan = factScan("SELECT id, k FROM runtime_fact")
        assert(scan.filterAttributes().exists(_.fieldNames().toSeq == Seq("k")))
        assert(scan.copy(pushedLimit = Some(1)).filterAttributes().isEmpty)
        assert(scan.copy(preserveDataGrouping = true).filterAttributes().isEmpty)
        assert(
          factScan("SELECT id, k FROM runtime_fact ORDER BY k LIMIT 1")
            .filterAttributes()
            .isEmpty)
        val oversized = factScan("SELECT id, k FROM runtime_fact")
        oversized.filter(Array(in((100 until 10101): _*)))
        assert(readIds(oversized.toBatch, oversized.toBatch.createReaderFactory()).size == 30)
        val factory = scan.toBatch.createReaderFactory()
        scan.filter(Array(new Predicate("IN", Array(Expressions.column("k")))))
        assert(readIds(scan.toBatch, factory).isEmpty)

        sql("INSERT INTO runtime_fact VALUES (100, 2)")
        val incomplete = factScan("SELECT id, k FROM runtime_fact")
        assert(incomplete.filterAttributes().isEmpty)
        incomplete.filter(Array(in(2)))
        assert(readIds(incomplete.toBatch, incomplete.toBatch.createReaderFactory()).size == 31)

        sql(
          "CALL sys.create_global_index(table => 'test.runtime_fact', " +
            "index_column => 'k', index_type => 'btree')")
        sql(
          "ALTER TABLE runtime_fact SET TBLPROPERTIES ('global-index.column-update-action'='IGNORE')")
        assert(factScan("SELECT id, k FROM runtime_fact").filterAttributes().isEmpty)
      }
    }
  }

  test("string index keys are filtered and oversized literal payloads are ignored") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact") {
        sql("""CREATE TABLE runtime_fact (id INT, k STRING) TBLPROPERTIES (
              |'row-tracking.enabled'='true', 'data-evolution.enabled'='true')""".stripMargin)
        sql("INSERT INTO runtime_fact VALUES (1, 'alpha'), (2, 'beta'), (3, NULL)")
        sql(
          "CALL sys.create_global_index(table => 'test.runtime_fact', " +
            "index_column => 'k', index_type => 'btree')")
        val scan = factScan("SELECT id, k FROM runtime_fact")
        assert(scan.filterAttributes().exists(_.fieldNames().toSeq == Seq("k")))
        val factory = scan.toBatch.createReaderFactory()
        scan.filter(
          Array(
            new Predicate(
              "IN",
              Array(Expressions.column("k"), Expressions.literal("x" * (1024 * 1024 + 1))))))
        assert(readIds(scan.toBatch, factory) == Seq(1, 2, 3))
        scan.filter(
          Array(new Predicate("IN", Array(Expressions.column("k"), Expressions.literal("beta")))))
        assert(readIds(scan.toBatch, factory) == Seq(2))
      }
    }
  }

  test("runtime filters reject an index made stale by a writer-only IGNORE option") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact", "runtime_dim") {
        createFact()
        val writerTable = loadTable("runtime_fact").copy(
          Map("global-index.column-update-action" -> "IGNORE").asJava)
        val builder = writerTable.newBatchWriteBuilder()
        val writer = builder.newWrite().withWriteType(writerTable.rowType().project("k"))
        val commit = builder.newCommit()
        try {
          (0 until 30).foreach(
            i => writer.write(GenericRow.of(Int.box(if (i == 0) 200 else i % 10))))
          val messages = writer.prepareCommit()
          messages.asScala.foreach {
            message =>
              val files = message.asInstanceOf[CommitMessageImpl].newFilesIncrement().newFiles()
              val updatedFiles = files.asScala.map(_.assignFirstRowId(0L)).toList
              files.clear()
              files.addAll(updatedFiles.asJava)
          }
          commit.commit(messages)
        } finally {
          writer.close()
          commit.close()
        }
        assert(!loadTable("runtime_fact").coreOptions().ignoreIndexColumnUpdate())
        assert(factScan("SELECT id, k FROM runtime_fact").filterAttributes().nonEmpty)
        sql("CREATE TABLE runtime_dim (k INT, keep INT)")
        sql("INSERT INTO runtime_dim VALUES (200, 1), (7, 0)")
        val query = """SELECT /*+ BROADCAST(d) */ f.id
                      |FROM runtime_fact f JOIN runtime_dim d ON f.k = d.k
                      |WHERE d.keep = 1""".stripMargin
        Seq(false, true).foreach {
          enabled =>
            withSparkSQLConf(
              "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> enabled.toString) {
              val result = sql(query)
              val scan = result.queryExecution.executedPlan.collectFirst {
                case s: BatchScanExec if s.output.exists(_.name == "id") => s
              }.get
              assert(scan.runtimeFilters.nonEmpty == enabled)
              checkAnswer(result, Seq(Row(0)))
            }
        }
      }
    }
  }

  test("ordinary scans defer index capability discovery until Spark requests runtime filters") {
    withSparkSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("runtime_fact") {
        createFact()
        val original = loadTable("runtime_fact")
        val fileIO = spy(original.fileIO())
        val table = new AppendOnlyFileStoreTable(
          fileIO,
          original.location(),
          original.schema(),
          original.catalogEnvironment())
        val manifest = table
          .store()
          .newIndexFileHandler()
          .indexManifestFilePath(table.snapshotManager().latestSnapshot().indexManifest())
        val scan = factScan("SELECT id, k FROM runtime_fact").copy(table = table)
        assert(readIds(scan.toBatch, scan.toBatch.createReaderFactory()).size == 30)
        verify(fileIO, never()).newInputStream(manifest)
        assert(scan.filterAttributes().exists(_.fieldNames().toSeq == Seq("k")))
        verify(fileIO, atLeastOnce()).newInputStream(manifest)
      }
    }
  }

  private def createFact(): Unit = {
    sql("""CREATE TABLE runtime_fact (id INT, k INT) TBLPROPERTIES (
          |'row-tracking.enabled'='true', 'data-evolution.enabled'='true')""".stripMargin)
    sql("""INSERT INTO runtime_fact SELECT /*+ COALESCE(1) */
          |CAST(id AS INT), CAST(id % 10 AS INT) FROM range(30)""".stripMargin)
    sql(
      "CALL sys.create_global_index(table => 'test.runtime_fact', " +
        "index_column => 'k', index_type => 'btree')")
  }

  private def factScan(query: String): PaimonScan =
    sql(query).queryExecution.executedPlan.collectFirst {
      case scan: BatchScanExec => scan.scan.asInstanceOf[PaimonScan]
    }.get

  private def in(values: Int*): Predicate =
    new Predicate(
      "IN",
      Array(Expressions.column("k")) ++
        values.map(value => Expressions.literal(Int.box(value))))

  private def readIds(batch: Batch, factory: PartitionReaderFactory): Seq[Int] = {
    batch
      .planInputPartitions()
      .toSeq
      .flatMap {
        partition =>
          val reader = factory.createReader(partition)
          val result = scala.collection.mutable.ArrayBuffer.empty[Int]
          try {
            while (reader.next()) {
              result += reader.get().getInt(0)
            }
          } finally {
            reader.close()
          }
          result
      }
      .sorted
  }
}
