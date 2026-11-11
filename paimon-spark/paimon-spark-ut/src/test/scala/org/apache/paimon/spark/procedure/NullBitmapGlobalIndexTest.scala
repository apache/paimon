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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class NullBitmapGlobalIndexTest extends PaimonSparkTestBase {

  for {
    indexType <- Seq("btree", "bitmap")
    compressNulls <- Seq(false, true)
  } {
    test(
      s"null $indexType index survives incremental builds and refresh, compression=$compressNulls") {
      withTable("T", "S") {
        spark.sql("""
                    |CREATE TABLE T (id BIGINT, idx INT, p INT) PARTITIONED BY (p)
                    |TBLPROPERTIES (
                    |  'bucket' = '-1',
                    |  'row-tracking.enabled' = 'true',
                    |  'data-evolution.enabled' = 'true',
                    |  'global-index.enabled' = 'true',
                    |  'global-index.column-update-action' = 'IGNORE',
                    |  'scalar-index.search-mode' = 'fast')
                    |""".stripMargin)
        spark.sql("""
                    |INSERT INTO T SELECT id,
                    |  CASE WHEN id % 10 = 0 THEN CAST(id AS INT) ELSE NULL END,
                    |  CAST(id % 2 AS INT) FROM range(10000)
                    |""".stripMargin)

        def build(): Unit = {
          spark
            .sql(
              s"CALL sys.create_global_index(table => 'test.T', " +
                s"index_column => 'idx', index_type => '$indexType', " +
                "options => 'sorted-index.records-per-file=2000," +
                s"sorted-index.null-bitmap-shuffle.enabled=$compressNulls')")
            .collect()
        }

        def checkQueries(): Unit = {
          checkAnswer(sql("SELECT COUNT(*) FROM T WHERE idx IS NULL"), Seq(Row(9000L)))
          checkAnswer(sql("SELECT COUNT(*) FROM T WHERE idx IS NOT NULL"), Seq(Row(1000L)))
          checkAnswer(sql("SELECT id FROM T WHERE idx = 20"), Seq(Row(20L)))
          checkAnswer(
            sql("SELECT id FROM T WHERE idx BETWEEN 10 AND 30"),
            Seq(Row(10L), Row(20L), Row(30L)))
        }

        build()
        checkQueries()
        val table = loadTable("T")
        val files = table.store().newIndexFileHandler().scanEntries().asScala
        assert(files.map(_.indexFile().rowCount()).sum == 10000L)
        val snapshot = table.snapshotManager().latestSnapshot().id()
        build()
        assert(loadTable("T").snapshotManager().latestSnapshot().id() == snapshot)

        spark.sql("INSERT INTO T SELECT id, CAST(NULL AS INT), 2 FROM range(10000, 11000)")
        build()
        checkAnswer(sql("SELECT COUNT(*) FROM T WHERE idx IS NULL"), Seq(Row(10000L)))
        assert(
          loadTable("T")
            .store()
            .newIndexFileHandler()
            .scanEntries()
            .asScala
            .map(_.indexFile().rowCount())
            .sum == 11000L)

        spark.sql("CREATE TABLE S (id BIGINT, idx INT)")
        spark.sql("INSERT INTO S VALUES (1, 42)")
        spark.sql("""
                    |MERGE INTO T USING S ON T.id = S.id
                    |WHEN MATCHED THEN UPDATE SET T.idx = S.idx
                    |""".stripMargin)
        build()
        checkAnswer(sql("SELECT id FROM T WHERE idx = 42"), Seq(Row(1L)))
        checkAnswer(sql("SELECT COUNT(*) FROM T WHERE idx IS NULL"), Seq(Row(9999L)))
      }
    }
  }
}
