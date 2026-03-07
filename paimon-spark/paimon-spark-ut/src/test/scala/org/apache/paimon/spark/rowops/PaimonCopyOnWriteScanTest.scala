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

package org.apache.paimon.spark.rowops

import org.apache.paimon.spark.PaimonSparkTestBase

class PaimonCopyOnWriteScanTest extends PaimonSparkTestBase {

  test("getInputSplits should cache dataSplits and not trigger redundant loadSplits") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, name STRING) USING PAIMON
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
            |""".stripMargin)
      sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      val table = loadTable("T")
      val schema = spark.table("T").schema

      // Create COW scan and load initial splits (snapshot 1)
      val cowScan = PaimonCopyOnWriteScan(table, schema)
      cowScan.inputSplits
      assert(cowScan.dataSplits.nonEmpty, "Initial dataSplits should not be empty")

      val initialSplitCount = cowScan.dataSplits.length
      val initialScannedFiles = cowScan.scannedFiles

      // Insert more data to create a new snapshot (simulates concurrent commit)
      sql("INSERT INTO T VALUES (4, 'd'), (5, 'e')")

      // Call inputSplits again (e.g., from reportDriverMetrics) - this should NOT trigger
      // loadSplits() and should NOT overwrite dataSplits due to caching in getInputSplits
      val metrics = cowScan.reportDriverMetrics()

      // Verify dataSplits was NOT overwritten by the new snapshot
      assert(
        cowScan.dataSplits.length == initialSplitCount,
        s"getInputSplits cache should prevent overwriting dataSplits. " +
          s"Expected $initialSplitCount splits, got ${cowScan.dataSplits.length}"
      )

      // Verify scannedFiles still returns the original files
      assert(
        cowScan.scannedFiles == initialScannedFiles,
        "scannedFiles should return the same files after reportDriverMetrics()"
      )

      // Verify metrics were computed correctly based on cached dataSplits
      assert(metrics.nonEmpty, "reportDriverMetrics should return non-empty metrics")
    }
  }

  test("getInputSplits cache should preserve dataSplits when filter applied and snapshot changed") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, name STRING) USING PAIMON
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
            |""".stripMargin)
      sql("INSERT INTO T VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      val table = loadTable("T")
      val schema = spark.table("T").schema

      // Create COW scan and load initial splits (snapshot 1)
      val cowScan = PaimonCopyOnWriteScan(table, schema)
      cowScan.inputSplits
      assert(cowScan.dataSplits.nonEmpty, "Initial dataSplits should not be empty")

      val initialScannedFiles = cowScan.scannedFiles
      assert(initialScannedFiles.nonEmpty, "Initial scannedFiles should not be empty")

      // Simulate a concurrent MERGE INTO that replaces the data files in a new snapshot.
      // After this, the original files from snapshot 1 no longer exist in the latest snapshot.
      sql("""MERGE INTO T USING (SELECT 1 as id, 'x' as name) AS s
            |ON T.id = s.id
            |WHEN MATCHED THEN UPDATE SET name = s.name
            |""".stripMargin)

      // Call inputSplits again (e.g., via reportDriverMetrics). With the getInputSplits
      // cache fix, this must NOT trigger loadSplits(). Without the fix, loadSplits()
      // would read the latest snapshot and overwrite dataSplits.
      cowScan.reportDriverMetrics()

      // Key assertion: dataSplits must remain non-empty (from original snapshot)
      assert(
        cowScan.dataSplits.nonEmpty,
        "dataSplits should remain non-empty after reportDriverMetrics() even when snapshot changed"
      )

      // scannedFiles should still return the original files for correct deletedCommitMessage
      assert(
        cowScan.scannedFiles == initialScannedFiles,
        "scannedFiles should still return original files for correct COW commit"
      )
    }
  }

  test("concurrent MERGE INTO should produce correct row count") {
    import java.util.concurrent.Executors

    withTable("s", "t") {
      sql("CREATE TABLE s (id INT, b INT, c INT) USING PAIMON")
      sql(
        "INSERT INTO s VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), " +
          "(4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9)")

      sql("""CREATE TABLE t (id INT, b INT, c INT) USING PAIMON
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
            |""".stripMargin)
      sql(
        "INSERT INTO t VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), " +
          "(4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9)")

      def doMergeInto(): Unit = {
        for (i <- 1 to 9) {
          try {
            sql(s"""
                   |MERGE INTO t
                   |USING (SELECT * FROM s WHERE id = $i) AS src
                   |ON t.id = src.id
                   |WHEN MATCHED THEN
                   |UPDATE SET t.b = src.b + t.b, t.c = src.c + t.c
                   |""".stripMargin)
          } catch {
            case e: Throwable =>
              assert(
                e.getMessage.contains("Conflicts during commits") ||
                  e.getMessage.contains("Missing file"),
                s"Unexpected error: ${e.getMessage}"
              )
          }
          // Key assertion: row count must always be 9 after each MERGE INTO
          checkAnswer(sql("SELECT count(*) FROM t"), Seq(org.apache.spark.sql.Row(9)))
        }
      }

      val executor = Executors.newFixedThreadPool(2)
      val runnable = new Runnable {
        override def run(): Unit = doMergeInto()
      }

      val future1 = executor.submit(runnable)
      val future2 = executor.submit(runnable)

      future1.get()
      future2.get()

      executor.shutdown()
    }
  }
}
