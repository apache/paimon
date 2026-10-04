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

import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.manifest.ManifestCommittable
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.{ExpireSnapshotsImpl, FileStoreTable}
import org.apache.paimon.table.sink.CommitCallback

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThatThrownBy

class RollbackToAsLatestProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: rollback to a snapshot as the latest snapshot") {
    spark.sql("CREATE TABLE T (id INT, name STRING)")
    spark.sql("INSERT INTO T VALUES (1, 'a')")
    spark.sql("INSERT INTO T VALUES (2, 'b')")
    spark.sql("INSERT INTO T VALUES (3, 'c')")

    val snapshotManager = loadTable("T").snapshotManager

    // Non-destructive: roll snapshot 1 forward as the new latest (snapshot 4);
    // snapshots 2 and 3 stay, unlike the destructive `rollback`.
    checkAnswer(
      spark.sql("CALL paimon.sys.rollback_to_as_latest(table => 'test.T', snapshot_id => 1)"),
      Row(3L, 1L, 4L) :: Nil)
    assert(snapshotManager.snapshotExists(2))
    assert(snapshotManager.snapshotExists(3))
    checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Nil)

    // Roll forward to snapshot 3 as the latest (snapshot 5).
    checkAnswer(
      spark.sql("CALL paimon.sys.rollback_to_as_latest(table => 'test.T', snapshot_id => 3)"),
      Row(4L, 3L, 5L) :: Nil)
    checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
  }

  test("Paimon Procedure: rollback keeps the protection tag after a post-commit failure") {
    Seq(false, true).foreach {
      concurrentCommit =>
        withTable("T") {
          createTableWithFailingCallback()
          spark.sql("INSERT INTO T VALUES (1, 'original')")
          // The restored file must not also belong to the snapshots retained before the rollback.
          spark.sql("INSERT OVERWRITE T VALUES (2, 'replacement')")
          spark.sql("INSERT INTO T VALUES (3, 'retained')")

          FailingRollbackCallback.concurrentCommit = concurrentCommit
          FailingRollbackCallback.failRollbackCommit = true
          try {
            assertThatThrownBy(() => rollbackToSnapshot1())
              .hasStackTraceContaining("Injected post-commit callback failure")
          } finally {
            FailingRollbackCallback.reset()
          }

          val table = loadTable("T")
          val latest = table.snapshotManager().latestSnapshot()
          assert(latest.id() == (if (concurrentCommit) 5L else 4L))
          assert(latest.commitUser().startsWith("rollback-to-as-latest-"))
          val tags = table.tagManager().allTagNames()
          assert(tags.size() == 1 && tags.get(0).startsWith("rollback-to-as-latest-1-"))
          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "original") :: Nil)
          val expire = table.newExpireSnapshots().asInstanceOf[ExpireSnapshotsImpl]
          expire.expireUntil(1, latest.id())
          checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "original") :: Nil)
        }
    }
  }

  test("Paimon Procedure: rollback removes the protection tag when it never started") {
    createTableWithFailingCallback()
    spark.sql("INSERT INTO T VALUES (1, 'original')")

    FailingRollbackCallback.failCommitCreation = true
    try {
      assertThatThrownBy(() => rollbackToSnapshot1())
        .hasStackTraceContaining("Injected commit creation failure")
    } finally {
      FailingRollbackCallback.reset()
    }

    val table = loadTable("T")
    assert(table.tagManager().allTagNames().isEmpty)
    assert(table.snapshotManager().latestSnapshotId() == 1L)
  }

  test("Paimon Procedure: rollback refreshes a cached table after a post-commit failure") {
    createTableWithFailingCallback()
    spark.sql("INSERT INTO T VALUES (1, 'original')")
    spark.sql("INSERT OVERWRITE T VALUES (2, 'replacement')")
    spark.sql("CACHE TABLE T")
    checkAnswer(spark.sql("SELECT * FROM T"), Row(2, "replacement") :: Nil)

    FailingRollbackCallback.failRollbackCommit = true
    try {
      assertThatThrownBy(() => rollbackToSnapshot1())
        .hasStackTraceContaining("Injected post-commit callback failure")
    } finally {
      FailingRollbackCallback.reset()
    }

    // Snapshot 3 (the rollback) is durable despite the callback failure, so the cached read must
    // reflect snapshot 1's data rather than the stale pre-rollback replacement row.
    checkAnswer(spark.sql("SELECT * FROM T"), Row(1, "original") :: Nil)
  }

  private def createTableWithFailingCallback(): Unit = {
    val callback = classOf[FailingRollbackCallback].getName
    spark.sql(
      s"CREATE TABLE T (id INT, name STRING) TBLPROPERTIES ('commit.callbacks' = '$callback')")
  }

  private def rollbackToSnapshot1(): Unit = {
    spark
      .sql("CALL paimon.sys.rollback_to_as_latest(table => 'test.T', snapshot_id => 1)")
      .collect()
  }
}

/** Injects rollback failures, optionally after another writer commits a snapshot first. */
class FailingRollbackCallback extends CommitCallback {

  override def setTable(table: FileStoreTable): Unit = {
    if (FailingRollbackCallback.failCommitCreation) {
      throw new RuntimeException("Injected commit creation failure")
    }
    if (FailingRollbackCallback.concurrentCommit) {
      // The procedure already read latest; the core rollback has not read it yet.
      FailingRollbackCallback.concurrentCommit = false
      val builder = table.newBatchWriteBuilder()
      val write = builder.newWrite()
      val commit = builder.newCommit()
      try {
        write.write(GenericRow.of(Int.box(4), BinaryString.fromString("concurrent")))
        commit.commit(write.prepareCommit())
      } finally {
        write.close()
        commit.close()
      }
    }
  }

  override def call(context: CommitCallback.Context): Unit = {
    if (
      FailingRollbackCallback.failRollbackCommit &&
      context.snapshot.commitUser().startsWith("rollback-to-as-latest-")
    ) {
      throw new RuntimeException("Injected post-commit callback failure")
    }
  }

  override def retry(committable: ManifestCommittable): Unit = {}

  override def close(): Unit = {}
}

object FailingRollbackCallback {
  @volatile var failRollbackCommit = false
  @volatile var concurrentCommit = false
  @volatile var failCommitCreation = false

  def reset(): Unit = {
    failRollbackCommit = false
    concurrentCommit = false
    failCommitCreation = false
  }
}
