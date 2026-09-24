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

import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, InnerTableCommit}

import org.apache.spark.internal.Logging

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
   */
  def commit(write: StreamingWrite, messages: Seq[CommitMessage], table: FileStoreTable): Unit =
    synchronized {
      require(committer != null, "The committer of the streaming query has not been created.")
      val identifier = write.commitIdentifier
      val mayBeReplay = !lastCommitted.exists(identifier > _)
      if (mayBeReplay) {
        checkReplayIsRecognisable(write.batchId, identifier, table)
      }
      marker.foreach(
        _.write(
          commitUser,
          write.batchId,
          Option(table.snapshotManager().latestSnapshotId()).map(_.longValue()).getOrElse(0L)))
      if (mayBeReplay) {
        val commits =
          committer.filterAndCommit(Collections.singletonMap(Long.box(identifier), messages.asJava))
        if (commits == 0) {
          // Expected of a replay; the only trace of a misconfigured identity otherwise.
          logInfo(
            s"Micro-batch $identifier was already committed to ${table.name()} under commit " +
              s"user '$commitUser' and is skipped as a replay.")
        }
      } else {
        committer.commit(identifier, messages.asJava)
      }
      lastCommitted = Some(lastCommitted.fold(identifier)(Math.max(_, identifier)))
    }

  /**
   * A replay is recognised by the latest snapshot the commit user left behind. While any snapshot
   * of the user is retained, that lookup is exact: expiration removes the oldest snapshots first,
   * so a commit of this batch, being newer, would be retained too. Without one, the marker the
   * previous attempt of this batch left before committing tells whether expiration has gone past
   * the snapshots that commit could have produced; if it has, whether the batch was committed can
   * no longer be told, and committing it again could write it twice.
   */
  private def checkReplayIsRecognisable(
      batchId: Long,
      identifier: Long,
      table: FileStoreTable): Unit = {
    val snapshotManager = table.snapshotManager()
    if (snapshotManager.latestSnapshotOfUser(commitUser).isPresent) {
      return
    }
    for {
      m <- marker
      before <- m.latestSnapshotIdBefore(commitUser, batchId)
      earliest <- Option(snapshotManager.earliestSnapshotId()).map(_.longValue())
      if earliest > before + 1
    } {
      throw new IllegalStateException(
        s"Cannot tell whether micro-batch $identifier of this query was committed to " +
          s"${table.name()}. A previous attempt of it was about to commit when the latest " +
          s"snapshot was $before, but snapshot expiration has since removed every snapshot up " +
          s"to ${earliest - 1}, including any it may have committed under commit user " +
          s"'$commitUser'. Committing it again could write it twice. Retain snapshots for " +
          s"longer than a query may be down. If the table shows that the micro-batch was not " +
          s"committed, delete ${m.path} and restart the query to commit it; otherwise restart " +
          s"the query from a new checkpoint.")
    }
  }

  override def close(): Unit =
    synchronized {
      if (committer != null) {
        val toClose = committer
        committer = null
        toClose.close()
      }
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
