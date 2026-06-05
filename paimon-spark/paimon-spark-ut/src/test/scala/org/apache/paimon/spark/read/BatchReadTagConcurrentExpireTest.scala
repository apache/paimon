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

package org.apache.paimon.spark.read

import org.apache.paimon.options.ExpireConfig
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.source.DataTableBatchScan

import java.time.Duration
import java.util

import scala.collection.JavaConverters._

/**
 * Tests that simulate concurrent snapshot expiration during a batch read.
 *
 * Uses an append-only table with INSERT OVERWRITE to ensure old data files become unreferenced by
 * later snapshots. Without protection: data files are deleted during expiration, read fails. With
 * protection (auto-tag): data files are preserved, read succeeds.
 */
class BatchReadTagConcurrentExpireTest extends PaimonSparkTestBase {

  test("Paimon: without protection, read fails after concurrent expiration") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |""".stripMargin)

    // write snapshot 1
    spark.sql("INSERT INTO T VALUES (1, 'v1'), (2, 'v2')")

    val table = loadTable("T")

    // Step 1: plan the scan (get splits referencing snapshot 1 data files)
    val scan = table.newScan()
    val plan = scan.plan()
    val splits = plan.splits()
    assert(!splits.isEmpty)

    // Step 2: OVERWRITE makes snapshot 1's files unreferenced by the new snapshot
    spark.sql("INSERT OVERWRITE T VALUES (3, 'v3'), (4, 'v4')")
    spark.sql("INSERT INTO T VALUES (5, 'v5')")
    spark.sql("INSERT INTO T VALUES (6, 'v6')")

    // Step 3: aggressively expire snapshots (keep only 1)
    val reloadedTable = loadTable("T")
    reloadedTable
      .newExpireSnapshots()
      .config(
        ExpireConfig
          .builder()
          .snapshotMaxDeletes(Integer.MAX_VALUE)
          .snapshotRetainMax(1)
          .snapshotRetainMin(1)
          .snapshotTimeRetain(Duration.ZERO)
          .build())
      .expire()

    // Step 4: try to read from the old splits - data files have been deleted
    val read = table.newReadBuilder().newRead()
    var readFailed = false
    try {
      val readers = splits.asScala.map(split => read.createReader(split))
      readers.foreach {
        reader =>
          val iter = reader.toCloseableIterator
          while (iter.hasNext) iter.next()
          iter.close()
      }
    } catch {
      case _: Exception =>
        readFailed = true
    }

    assert(readFailed, "Read should fail because data files were deleted by expiration")
  }

  test("Paimon: with protection, read succeeds after concurrent expiration") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES (
                 |  'scan.plan-auto-tag-for-read.time-retained' = '1 h'
                 |)
                 |""".stripMargin)

    // write snapshot 1
    spark.sql("INSERT INTO T VALUES (1, 'v1'), (2, 'v2')")

    val table = loadTable("T")

    // Step 1: plan the scan - this creates a protection tag automatically
    val scan = table.newScan()
    val plan = scan.plan()
    val splits = plan.splits()
    assert(!splits.isEmpty)

    // Verify protection tag was created
    val batchScan = scan.asInstanceOf[DataTableBatchScan]
    val tagName = batchScan.readProtectionTagName
    assert(tagName != null, "Protection tag should be created during scan planning")
    assert(table.tagManager().tagExists(tagName))

    // Step 2: OVERWRITE makes snapshot 1's files unreferenced by new snapshot
    spark.sql("INSERT OVERWRITE T VALUES (3, 'v3'), (4, 'v4')")
    spark.sql("INSERT INTO T VALUES (5, 'v5')")
    spark.sql("INSERT INTO T VALUES (6, 'v6')")

    // Step 3: aggressively expire snapshots (keep only 1)
    val reloadedTable = loadTable("T")
    reloadedTable
      .newExpireSnapshots()
      .config(
        ExpireConfig
          .builder()
          .snapshotMaxDeletes(Integer.MAX_VALUE)
          .snapshotRetainMax(1)
          .snapshotRetainMin(1)
          .snapshotTimeRetain(Duration.ZERO)
          .build())
      .expire()

    // Step 4: read from old splits - should succeed because tag protects data files
    val read = table.newReadBuilder().newRead()
    val results = new util.ArrayList[String]()
    splits.asScala.foreach {
      split =>
        val reader = read.createReader(split)
        val iter = reader.toCloseableIterator
        while (iter.hasNext) {
          val row = iter.next()
          results.add(s"${row.getInt(0)},${row.getString(1).toString}")
        }
        iter.close()
    }

    assert(results.size() == 2, s"Should read 2 rows but got ${results.size()}")
    assert(results.contains("1,v1"))
    assert(results.contains("2,v2"))

    // Cleanup: delete the protection tag
    val snapshotManager = table.snapshotManager()
    val creator = new org.apache.paimon.tag.BatchReadTagCreator(
      table.tagManager(),
      snapshotManager,
      Duration.ofHours(1))
    creator.deleteReadTag(tagName)
  }
}
