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

package org.apache.paimon.spark.write

import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl, InnerTableCommit}

import org.apache.spark.internal.Logging

import java.io.FileNotFoundException
import java.util.Collections

import scala.collection.JavaConverters._

/**
 * The committer of one run of a streaming query. It is created with the first micro-batch of the
 * run and kept until the query terminates, the way a Flink committer lives across checkpoints: the
 * maintenance a commit starts (tag creation, partition and snapshot expiration) can run on while
 * the next micro-batch is written, and only a micro-batch that may be a replay has to look up what
 * a previous run committed.
 *
 * @param commitUser
 *   the commit user of the query, stable across its runs, under which every micro-batch is
 *   committed so that a replayed one can be recognised.
 * @param marker
 *   where the query records what it is about to commit, when it has a checkpoint location to keep
 *   it in.
 */
class StreamingWriteContext(val commitUser: String, @transient marker: Option[CommitMarker])
  extends AutoCloseable
  with Serializable
  with Logging {

  /** Driver-side state; the context itself travels with the write closures. */
  @transient private var committer: InnerTableCommit = _

  /** The identifier of the last micro-batch this run committed, none before the first one. */
  @transient private var lastCommitted: Option[Long] = None

  /** The committer of the run, created by `create` on first use and kept afterwards. */
  def openCommitter(create: => InnerTableCommit): InnerTableCommit =
    synchronized {
      if (committer == null) {
        committer = create
      }
      committer
    }

  /**
   * Commits a micro-batch under its identifier.
   *
   * Structured Streaming replays a micro-batch with its original batch id when a query is restarted
   * after failing between the sink returning and Spark recording the batch as completed, so the
   * first micro-batch of a run may already have been committed by a previous run. A batch that
   * follows one this run committed cannot be a replay of it, and is committed without looking the
   * history of the commit user up.
   *
   * An overwrite, the commit of a query in complete mode, publishes the complete result of its
   * micro-batch: committing a replay of it again leaves the table as its first commit did, and runs
   * the callbacks of an overwrite again, as the cleanup of the snapshot branch of a chain table
   * needs. Every micro-batch of such a query is committed, as it always was.
   */
  def commit(
      write: StreamingWrite,
      messages: Seq[CommitMessage],
      table: FileStoreTable,
      overwrite: Boolean): Unit =
    synchronized {
      require(committer != null, "The committer of the streaming query has not been created.")
      val identifier = write.commitIdentifier
      if (overwrite) {
        committer.commit(identifier, messages.asJava)
      } else if (lastCommitted.exists(identifier > _)) {
        writeMarker(write, table)
        committer.commit(identifier, messages.asJava)
      } else {
        commitFirstOfRun(write, messages, table)
      }
      lastCommitted = Some(lastCommitted.fold(identifier)(Math.max(_, identifier)))
    }

  private def writeMarker(write: StreamingWrite, table: FileStoreTable): Unit =
    marker.foreach(
      _.write(
        commitUser,
        write.batchId,
        Option(table.snapshotManager().latestSnapshotId()).map(_.longValue()).getOrElse(0L)))

  /**
   * The first micro-batch of a run, which may be a replay. Every snapshot after the boundary its
   * previous attempt recorded is checked, and the result of that scan decides whether the batch is
   * skipped, refused, or committed:
   *   - If a snapshot of this commit user carries `identifier` or a later one, the micro-batch was
   *     already committed. It is not committed again, but is handed to the commit callbacks,
   *     through the committer's filtered commit, to retry what may have failed after its first
   *     commit, such as registering its partitions. It is handed over without its files: the
   *     filtered commit only compares with the latest snapshot of the commit user, and should that
   *     one not show the first commit, what gets published is an empty commit, not the batch a
   *     second time.
   *   - If the scan hits missing metadata before reaching the boundary, it may have missed an
   *     expired commit, so the query fails closed instead of risking a duplicate commit.
   *   - Only when there was no previous attempt or the scan read every snapshot since the boundary
   *     without finding the commit is the micro-batch committed directly.
   */
  private def commitFirstOfRun(
      write: StreamingWrite,
      messages: Seq[CommitMessage],
      table: FileStoreTable): Unit = {
    val identifier = write.commitIdentifier
    val previousAttempt = marker.flatMap(_.latestSnapshotIdBefore(commitUser, write.batchId))
    writeMarker(write, table)
    previousAttempt.map(lookupAfter(_, identifier, table)) match {
      case Some(StreamingWriteContext.ReplayLookup.Committed) =>
        committer.filterAndCommit(
          Collections.singletonMap(Long.box(identifier), withoutFiles(messages).asJava))
        // Expected of a replay; the only trace of a misconfigured identity otherwise.
        logInfo(
          s"Micro-batch $identifier was already committed to ${table.name()} under commit user " +
            s"'$commitUser' and is skipped as a replay.")
      case Some(StreamingWriteContext.ReplayLookup.HistoryIncomplete(before, missingSnapshotId)) =>
        throw new IllegalStateException(
          s"Cannot tell whether micro-batch $identifier of this query was committed to " +
            s"${table.name()}. A previous attempt of it was about to commit when the latest " +
            s"snapshot was $before, but snapshot $missingSnapshotId is no longer retained, so " +
            s"the lookup of commit user '$commitUser' may have missed its commit. " +
            "Committing it again could write it twice. Retain snapshots for longer than a " +
            "query may be down. If the table shows that the micro-batch was not committed, " +
            s"delete ${marker.get.path} and restart the query to commit it; otherwise restart " +
            "the query from a new checkpoint.")
      case None | Some(StreamingWriteContext.ReplayLookup.NotCommitted) =>
        committer.commit(identifier, messages.asJava)
    }
  }

  /**
   * Looks for a commit of `identifier`, or a later one, by this commit user in every snapshot after
   * `before`, the latest snapshot when the previous attempt of the micro-batch was about to commit.
   *
   * Every snapshot since `before` is looked at, not only the latest one of the commit user: the
   * maintenance a commit starts, such as the expiration of partitions, publishes under the
   * identifier of the micro-batch that started it, and when it runs asynchronously it can do so
   * after the next micro-batch committed.
   *
   * A negative lookup is trusted only when every snapshot in `(before, latest]` was read without
   * encountering missing metadata. Checking whether `before + 1` still exists is not enough:
   * `SnapshotManager.deleteSnapshot` swallows deletion failures quietly, so `before + 1` can
   * survive even after expiration has removed later snapshots and advanced `EARLIEST`.
   */
  private def lookupAfter(
      before: Long,
      identifier: Long,
      table: FileStoreTable): StreamingWriteContext.ReplayLookup = {
    val snapshots = table.snapshotManager()
    val latest = Option(snapshots.latestSnapshotId()).map(_.longValue()).getOrElse(0L)
    if (latest < before) {
      return StreamingWriteContext.ReplayLookup.HistoryIncomplete(before, before)
    }
    var id = latest
    while (id > before) {
      try {
        val snapshot = snapshots.tryGetSnapshot(id)
        if (snapshot.commitUser() == commitUser && snapshot.commitIdentifier() >= identifier) {
          return StreamingWriteContext.ReplayLookup.Committed
        }
      } catch {
        case _: FileNotFoundException =>
          return StreamingWriteContext.ReplayLookup.HistoryIncomplete(before, id)
      }
      id -= 1
    }
    StreamingWriteContext.ReplayLookup.NotCommitted
  }

  /** The partitions and buckets a micro-batch wrote to, without anything it wrote there. */
  private def withoutFiles(messages: Seq[CommitMessage]): Seq[CommitMessage] =
    messages.map(
      message =>
        new CommitMessageImpl(
          message.partition(),
          message.bucket(),
          message.totalBuckets(),
          DataIncrement.emptyIncrement(),
          CompactIncrement.emptyIncrement()))

  override def close(): Unit =
    synchronized {
      if (committer != null) {
        val toClose = committer
        committer = null
        toClose.close()
      }
    }
}

private object StreamingWriteContext {

  sealed trait ReplayLookup
  object ReplayLookup {
    case object Committed extends ReplayLookup
    case object NotCommitted extends ReplayLookup
    case class HistoryIncomplete(before: Long, missingSnapshotId: Long) extends ReplayLookup
  }
}

/**
 * One micro-batch of a streaming query, written and committed through the context of the query's
 * current run.
 *
 * @param batchId
 *   the id Spark gives the micro-batch, counted from 0.
 */
case class StreamingWrite(context: StreamingWriteContext, batchId: Long) {

  /**
   * Paimon numbers the commits of a stream from 1, the way Flink numbers its checkpoints, and
   * [[org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner]] recognises a full
   * compaction by an identifier that is a multiple of 'full-compaction.delta-commits'. Spark
   * numbers its micro-batches from 0, so the third one is batch 2; both the full compaction
   * schedule and the identifier it is published under have to count it as 3.
   */
  val commitIdentifier: Long = batchId + 1
}
