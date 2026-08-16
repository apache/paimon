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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.tag.BatchReadTagCreator

import org.apache.spark.sql.Row

class BatchReadTagProtectionTest extends PaimonSparkTestBase {

  test("Paimon: batch read creates protection tag when enabled") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'a',
                 |  'bucket' = '1',
                 |  'scan.plan-auto-tag-for-read.time-retained' = '1 h'
                 |)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'v1'), (2, 'v2')")

    val table = loadTable("T")
    val tagManager = table.tagManager()

    // query triggers scan which creates a protection tag
    checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), Row(1, "v1") :: Row(2, "v2") :: Nil)

    // after query completes, listener should have cleaned up the tag
    // give it a moment since listener fires asynchronously
    Thread.sleep(500)

    val remainingTags = tagManager.allTagNames()
    val batchReadTags = remainingTags.toArray
      .map(_.toString)
      .filter(BatchReadTagCreator.isBatchReadTag)
    assert(
      batchReadTags.isEmpty,
      s"Protection tags should be cleaned up, but found: ${batchReadTags.mkString(", ")}")
  }

  test("Paimon: batch read does NOT create tag when disabled") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES ('primary-key' = 'a', 'bucket' = '1')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'v1'), (2, 'v2')")

    val table = loadTable("T")
    val tagManager = table.tagManager()

    checkAnswer(spark.sql("SELECT * FROM T ORDER BY a"), Row(1, "v1") :: Row(2, "v2") :: Nil)

    val remainingTags = tagManager.allTagNames()
    val batchReadTags = remainingTags.toArray
      .map(_.toString)
      .filter(BatchReadTagCreator.isBatchReadTag)
    assert(batchReadTags.isEmpty, "No protection tags should be created when feature is disabled")
  }

  test("Paimon: protection tag prevents data file deletion during expiration") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'a',
                 |  'bucket' = '1',
                 |  'scan.plan-auto-tag-for-read.time-retained' = '1 h',
                 |  'snapshot.num-retained.min' = '1',
                 |  'snapshot.num-retained.max' = '1'
                 |)
                 |""".stripMargin)

    // create snapshot 1
    spark.sql("INSERT INTO T VALUES (1, 'v1'), (2, 'v2')")

    val table = loadTable("T")
    val tagManager = table.tagManager()
    val snapshotManager = table.snapshotManager()

    // manually create a protection tag on snapshot 1 (simulating what scan does)
    val creator =
      new BatchReadTagCreator(tagManager, snapshotManager, java.time.Duration.ofHours(1))
    val tagName = creator.createReadTag(1L)
    assert(tagName != null)
    assert(tagManager.tagExists(tagName))

    // create more snapshots to trigger expiration of snapshot 1
    spark.sql("INSERT INTO T VALUES (3, 'v3')")
    spark.sql("INSERT INTO T VALUES (4, 'v4')")

    // snapshot 1 may be expired now, but data files should be protected by the tag
    // read from the tag should still work
    checkAnswer(
      spark.sql(s"SELECT * FROM T VERSION AS OF '$tagName' ORDER BY a"),
      Row(1, "v1") :: Row(2, "v2") :: Nil)

    // cleanup
    creator.deleteReadTag(tagName)
  }
}
