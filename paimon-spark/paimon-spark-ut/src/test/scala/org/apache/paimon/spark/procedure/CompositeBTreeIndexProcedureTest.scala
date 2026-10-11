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

import org.apache.paimon.globalindex.{IndexedSplit, IndexQuerySplit}
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils
import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.globalindex.sorted.SortedIndexTopoBuilder
import org.apache.paimon.types.DataField
import org.apache.paimon.utils.ExceptionUtils

import org.apache.spark.sql.Row

import java.io.FileNotFoundException
import java.util.Collections

import scala.collection.JavaConverters._

class CompositeBTreeIndexProcedureTest extends PaimonSparkTestBase {

  test("nullable extra fields preserve single column empty builds") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT)
            |TBLPROPERTIES ('bucket' = '-1', 'row-tracking.enabled' = 'true',
            |'data-evolution.enabled' = 'true')
            |""".stripMargin)
      val table = loadTable("T")
      for (extras <- Seq(null, Collections.emptyList[DataField]())) {
        assert(
          new SortedIndexTopoBuilder()
            .buildIndex(
              null,
              null,
              null,
              table,
              "btree",
              table.rowType(),
              table.rowType().getField("id"),
              extras,
              new Options())
            .isEmpty)
      }
    }
  }

  test("composite btree creation, incremental build and component refresh") {
    withTable("T", "S", "C") {
      sql("""CREATE TABLE T (id INT, category STRING, item_number INT)
            |TBLPROPERTIES ('bucket' = '-1', 'row-tracking.enabled' = 'true',
            |'data-evolution.enabled' = 'true', 'global-index.column-update-action' = 'IGNORE',
            |'sorted-index.records-per-file' = '13', 'btree-index.bloom-filter.enabled' = 'true')
            |""".stripMargin)
      def insert(from: Int, to: Int): Unit = {
        val values = (from until to)
          .map {
            i =>
              val categoryValue = if ((i / 10) % 2 == 0) "category-a" else "category-b"
              s"($i, '$categoryValue', ${i % 10})"
          }
          .mkString(",")
        sql(s"INSERT INTO T VALUES $values")
      }
      def build(columns: String): Unit = {
        sql(
          s"CALL sys.create_global_index(table => 'test.T', index_column => '$columns', index_type => 'btree')")
      }
      insert(0, 40)
      sql("INSERT INTO T VALUES (100, NULL, 7), (101, 'category-a', NULL)")
      build("item_number")
      build("category,item_number")
      val indexes = loadTable("T").store().newIndexFileHandler().scanEntries().asScala
      val composite =
        indexes.filter(_.indexFile().globalIndexMeta().getIndexedFieldIds().size() == 2)
      assert(composite.nonEmpty)
      assert(
        composite.forall(
          _.indexFile().globalIndexMeta().getIndexedFieldIds().asScala.toSeq == Seq(1, 2)))
      assert(composite.map(_.indexFile().rowCount()).sum == 42L)
      // Keep the trailing scalar definition, but make its postings unavailable.
      val scalarFiles = SortedGlobalIndexTestUtils.deleteIndexFiles(loadTable("T"), 1)
      try {
        for (inReader <- Seq(false, true)) {
          sql(
            s"ALTER TABLE T SET TBLPROPERTIES ('global-index.query-in-reader.enabled' = '$inReader', 'scalar-index.search-mode' = 'fast')")
          def checkCompositeAnswer(query: String, expected: Seq[Row]): Unit = {
            val splits =
              try {
                getPaimonScan(query).inputSplits
              } catch {
                case failure: Exception =>
                  throw new AssertionError(s"Failed to plan $query, inReader=$inReader", failure)
              }
            if (expected.nonEmpty) {
              assert(splits.nonEmpty)
              assert(
                splits.forall(
                  split =>
                    if (inReader) split.isInstanceOf[IndexQuerySplit]
                    else split.isInstanceOf[IndexedSplit]))
            } else if (!inReader) {
              assert(splits.isEmpty)
            }
            try {
              checkAnswer(sql(query), expected)
            } catch {
              case failure: Exception =>
                throw new AssertionError(s"Failed to read $query, inReader=$inReader", failure)
            }
            if (expected.nonEmpty) {
              val compositeFiles = loadTable("T")
                .store()
                .newIndexFileHandler()
                .scanEntries()
                .asScala
                .map(_.indexFile())
                .filter(_.globalIndexMeta().getIndexedFieldIds().size() == 2)
                .map(_.fileName())
              val removed = SortedGlobalIndexTestUtils.deleteIndexFiles(loadTable("T"), 2)
              try {
                val failure = intercept[Exception] {
                  sql(query).collect()
                }
                val missing = ExceptionUtils.findThrowable(failure, classOf[FileNotFoundException])
                assert(missing.isPresent)
                assert(compositeFiles.exists(missing.get().getMessage.contains))
              } finally {
                removed.close()
              }
            }
          }
          checkCompositeAnswer(
            "SELECT id FROM T WHERE item_number = 7 AND category = 'category-a'",
            Seq(Row(7), Row(27)))
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category = 'absent' AND item_number = 7",
            Seq.empty)
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category = 'category-a'",
            ((0 until 10) ++ (20 until 30) :+ 101).map(Row(_)))
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category = 'category-a' AND item_number > 7",
            Seq(Row(8), Row(9), Row(28), Row(29)))
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category = 'category-a' AND item_number BETWEEN 6 AND 8",
            Seq(Row(6), Row(7), Row(8), Row(26), Row(27), Row(28)))
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category IN ('category-a', 'category-b') AND item_number IN (7, 8)",
            Seq(Row(7), Row(8), Row(17), Row(18), Row(27), Row(28), Row(37), Row(38))
          )
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category IN ('category-a', 'category-b') AND item_number > 7",
            Seq(Row(8), Row(9), Row(18), Row(19), Row(28), Row(29), Row(38), Row(39))
          )
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category IS NULL AND item_number = 7",
            Seq(Row(100)))
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category = 'category-a' AND item_number IS NULL",
            Seq(Row(101)))
          checkCompositeAnswer(
            "SELECT id FROM T WHERE category IN ('category-a', NULL) AND item_number IN (7, 8)",
            Seq(Row(7), Row(8), Row(27), Row(28)))
        }
      } finally {
        scalarFiles.close()
      }
      build("category")
      insert(40, 60)
      build("category,item_number")
      checkAnswer(
        sql("SELECT id FROM T WHERE item_number = 7 AND category = 'category-a'"),
        Seq(Row(7), Row(27), Row(47)))
      sql("CREATE TABLE S (id INT, item_number INT)")
      sql("INSERT INTO S VALUES (7, 107)")
      sql(
        "MERGE INTO T USING S ON T.id = S.id WHEN MATCHED THEN UPDATE SET T.item_number = S.item_number")
      build("category,item_number")
      checkAnswer(
        sql("SELECT id FROM T WHERE category = 'category-a' AND item_number = 107"),
        Seq(Row(7)))
      checkAnswer(
        sql("SELECT id FROM T WHERE category = 'category-a' AND item_number = 7"),
        Seq(Row(27), Row(47)))
      sql("CREATE TABLE C (id INT, category STRING)")
      sql("INSERT INTO C VALUES (27, 'category-c')")
      sql(
        "MERGE INTO T USING C ON T.id = C.id WHEN MATCHED THEN UPDATE SET T.category = C.category")
      build("category,item_number")
      checkAnswer(
        sql("SELECT id FROM T WHERE category = 'category-a' AND item_number = 7"),
        Seq(Row(47)))
      checkAnswer(
        sql("SELECT id FROM T WHERE category = 'category-c' AND item_number = 7"),
        Seq(Row(27)))
      val snapshot = loadTable("T").snapshotManager().latestSnapshot().id()
      build("category,item_number")
      assert(loadTable("T").snapshotManager().latestSnapshot().id() == snapshot)
      sql(
        "CALL sys.drop_global_index(table => 'test.T', index_column => 'category,item_number', index_type => 'btree')")
      assert(
        loadTable("T")
          .store()
          .newIndexFileHandler()
          .scanEntries()
          .asScala
          .forall(_.indexFile().globalIndexMeta().getIndexedFieldIds().size() == 1))
      assert(
        loadTable("T")
          .store()
          .newIndexFileHandler()
          .scanEntries()
          .asScala
          .map(_.indexFile().globalIndexMeta().indexFieldId())
          .toSet == Set(1, 2))
    }
  }
}
