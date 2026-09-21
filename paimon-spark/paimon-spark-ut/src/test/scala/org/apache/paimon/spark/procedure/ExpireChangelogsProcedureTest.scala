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
import org.apache.paimon.utils.{ChangelogManager, SnapshotManager}

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.sql.Timestamp

class ExpireChangelogsProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: expire changelogs") {
    createInputChangelogTable()

    val table = loadTable("T")
    val snapshotManager = table.snapshotManager
    val changelogManager = table.changelogManager

    for (i <- 0 until 10) {
      spark.sql(s"INSERT INTO T VALUES ('$i', $i)")
    }
    checkSnapshots(snapshotManager, 7, 10)
    checkChangelogs(changelogManager, 1, 6)

    checkAnswer(
      spark.sql("CALL sys.expire_changelogs(table => 'test.T', retain_max => 8)"),
      Row(2) :: Nil)
    checkChangelogs(changelogManager, 3, 6)

    val ts = new Timestamp(snapshotManager.latestSnapshot().timeMillis)
    checkAnswer(
      spark.sql(
        s"CALL sys.expire_changelogs(table => 'test.T', older_than => '${ts.toString}', max_deletes => 1)"),
      Row(1) :: Nil)
    checkChangelogs(changelogManager, 4, 6)

    checkAnswer(
      spark.sql(
        s"CALL sys.expire_changelogs(table => 'test.T', older_than => '${ts.toString}', retain_min => 6)"),
      Row(1) :: Nil)
    checkChangelogs(changelogManager, 5, 6)

    checkAnswer(
      spark.sql(s"CALL sys.expire_changelogs(table => 'test.T', older_than => '${ts.toString}')"),
      Row(1) :: Nil)
    checkChangelogs(changelogManager, 6, 6)

    for (i <- 10 until 12) {
      spark.sql(s"INSERT INTO T VALUES ('$i', $i)")
    }
    checkSnapshots(snapshotManager, 9, 12)
    checkChangelogs(changelogManager, 6, 8)

    checkAnswer(
      spark.sql("CALL sys.expire_changelogs(table => 'test.T', retain_max => 4)"),
      Row(2) :: Nil)
    checkChangelogs(changelogManager, 8, 8)

    checkAnswer(
      spark.sql("CALL sys.expire_changelogs(table => 'test.T', delete_all => true)"),
      Row(1) :: Nil)
    checkAllDeleted(changelogManager)
  }

  test("Paimon Procedure: expire changelogs load table property first") {
    createInputChangelogTable()

    val table = loadTable("T")
    val snapshotManager = table.snapshotManager
    val changelogManager = table.changelogManager

    for (i <- 0 until 10) {
      spark.sql(s"INSERT INTO T VALUES ('$i', $i)")
    }
    checkSnapshots(snapshotManager, 7, 10)
    checkChangelogs(changelogManager, 1, 6)

    // Commits already honor changelog.num-retained.max=10, so empty CALL would delete 0.
    // Tighten table properties and verify the procedure reads them.
    spark.sql("""
                |ALTER TABLE T SET TBLPROPERTIES (
                |  'changelog.num-retained.max' = '8',
                |  'changelog.num-retained.min' = '4'
                |)
                |""".stripMargin)

    checkAnswer(spark.sql("CALL sys.expire_changelogs(table => 'test.T')"), Row(2) :: Nil)
    checkChangelogs(changelogManager, 3, 6)
  }

  test("Paimon Procedure: expire changelogs add options parameter") {
    createInputChangelogTable()

    val table = loadTable("T")
    val snapshotManager = table.snapshotManager
    val changelogManager = table.changelogManager

    for (i <- 0 until 10) {
      spark.sql(s"INSERT INTO T VALUES ('$i', $i)")
    }
    checkSnapshots(snapshotManager, 7, 10)
    checkChangelogs(changelogManager, 1, 6)

    checkAnswer(spark.sql("CALL sys.expire_changelogs(table => 'test.T')"), Row(0) :: Nil)
    checkChangelogs(changelogManager, 1, 6)

    checkAnswer(
      spark.sql(
        "CALL sys.expire_changelogs(table => 'test.T', options => 'changelog.num-retained.max=8, changelog.num-retained.min=4')"),
      Row(2) :: Nil)
    checkChangelogs(changelogManager, 3, 6)
  }

  test("Paimon Procedure: expire changelogs without separated changelogs") {
    spark.sql("CREATE TABLE T (word STRING, cnt INT)")
    spark.sql("INSERT INTO T VALUES ('a', 1)")

    checkAnswer(spark.sql("CALL sys.expire_changelogs(table => 'test.T')"), Row(0) :: Nil)
    checkAnswer(
      spark.sql("CALL sys.expire_changelogs(table => 'test.T', delete_all => true)"),
      Row(0) :: Nil)
  }

  test("Paimon Procedure: expire changelogs retainMax retainMin value check") {
    createInputChangelogTable()

    for (i <- 0 until 10) {
      spark.sql(s"INSERT INTO T VALUES ('$i', $i)")
    }

    assertThrows[IllegalArgumentException] {
      spark.sql("CALL sys.expire_changelogs(table => 'test.T', retain_max => 2, retain_min => 3)")
    }
  }

  test("Paimon Procedure: expire changelogs delete_all skips missing changelog") {
    createInputChangelogTable()

    val table = loadTable("T")
    val changelogManager = table.changelogManager

    for (i <- 0 until 10) {
      spark.sql(s"INSERT INTO T VALUES ('$i', $i)")
    }
    checkChangelogs(changelogManager, 1, 6)

    val missingId = 3L
    assertThat(changelogManager.longLivedChangelogExists(missingId)).isTrue()
    changelogManager.fileIO.deleteQuietly(changelogManager.longLivedChangelogPath(missingId))

    checkAnswer(
      spark.sql("CALL sys.expire_changelogs(table => 'test.T', delete_all => true)"),
      Row(5) :: Nil)
    checkAllDeleted(changelogManager)
  }

  test("Paimon Procedure: expire changelogs delete_all cannot mix with other arguments") {
    createInputChangelogTable()
    spark.sql("INSERT INTO T VALUES ('0', 0)")

    assertThrows[IllegalArgumentException] {
      spark.sql(
        "CALL sys.expire_changelogs(table => 'test.T', delete_all => true, retain_max => 8)")
    }
    assertThrows[IllegalArgumentException] {
      spark.sql(
        "CALL sys.expire_changelogs(table => 'test.T', delete_all => true, options => 'changelog.num-retained.max=8')")
    }
  }

  private def createInputChangelogTable(): Unit = {
    spark.sql("""
                |CREATE TABLE T (word STRING, cnt INT)
                |TBLPROPERTIES (
                |  'primary-key' = 'word',
                |  'bucket' = '1',
                |  'num-sorted-run.compaction-trigger' = '9999',
                |  'changelog-producer' = 'input',
                |  'snapshot.num-retained.min' = '4',
                |  'snapshot.num-retained.max' = '4',
                |  'changelog.num-retained.min' = '4',
                |  'changelog.num-retained.max' = '10'
                |)
                |""".stripMargin)
  }

  private def checkSnapshots(sm: SnapshotManager, earliest: Int, latest: Int): Unit = {
    assertThat(sm.snapshotCount).isEqualTo(latest - earliest + 1)
    assertThat(sm.earliestSnapshotId).isEqualTo(earliest)
    assertThat(sm.latestSnapshotId).isEqualTo(latest)
  }

  private def checkChangelogs(cm: ChangelogManager, earliest: Int, latest: Int): Unit = {
    assertThat(cm.earliestLongLivedChangelogId).isEqualTo(earliest)
    assertThat(cm.latestLongLivedChangelogId).isEqualTo(latest)
  }

  private def checkAllDeleted(cm: ChangelogManager): Unit = {
    assertThat(cm.latestLongLivedChangelogId).isNull()
    assertThat(cm.earliestLongLivedChangelogId).isNull()
  }
}
