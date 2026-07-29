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

import org.apache.paimon.table.sink.{BatchTableCommit, BatchWriteBuilder, CommitMessage}

import org.apache.spark.TaskContext
import org.apache.spark.TaskKilledException
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

/**
 * Task-side cleanup helper for Spark speculative execution and task interruption.
 *
 * <p>When a task attempt is killed by Spark (e.g. speculative execution loser), this helper aborts
 * unprepared or prepared files before the attempt returns a successful result.
 */
final class SparkAttemptCleanup(
    tableName: String,
    commitUser: String,
    writeBuilder: BatchWriteBuilder,
    closeUnprepared: () => Unit)
  extends Logging {

  private val INTERRUPT_CHECK_INTERVAL = 1024

  sealed private trait State
  private object State {
    case object Writing extends State
    case object Prepared extends State
    case object Aborted extends State
    case object Returned extends State
    case object CloseFailed extends State
    case object Closed extends State
  }

  private val state = new AtomicReference[State](State.Writing)

  /**
   * Incrementally accumulated prepared messages. Uses a mutable ListBuffer so each {@link
   * #addPrepared} call is O(1) amortized; the list is only materialized once in {@link
   * #abortIfNeeded} when a kill or close actually needs to delete the files. This avoids the O(n²)
   * cost of repeatedly exporting an immutable Seq on every incremental prepareCommit.
   */
  private val preparedMessages = scala.collection.mutable.ListBuffer.empty[CommitMessage]
  @volatile private var preparedSnapshot: Option[Seq[CommitMessage]] = None
  private var recordCount: Long = 0L

  registerCompletionListener()

  def checkInterrupted(stage: String): Unit = {
    if (isTaskInterrupted) {
      abortIfNeeded(s"interrupted at $stage")
      throw taskKilledException()
    }
  }

  def checkInterruptedPeriodically(): Unit = {
    recordCount += 1
    if (recordCount % INTERRUPT_CHECK_INTERVAL == 0) {
      checkInterrupted("write")
    }
  }

  /**
   * Replace the full prepared list. Convenience for one-shot prepareCommit callers; multi-file
   * writers should prefer {@link #addPrepared} to avoid O(n²) copying.
   */
  def setPrepared(messages: Seq[CommitMessage]): Unit = {
    preparedMessages.clear()
    preparedMessages ++= Option(messages).getOrElse(Seq.empty)
    preparedSnapshot = None
    transitionToPrepared()
  }

  /**
   * Incrementally register prepared messages. O(1) amortized; call this after each incremental
   * prepareCommit so a mid-attempt kill can abort already-finished files.
   */
  def addPrepared(messages: Seq[CommitMessage]): Unit = {
    preparedMessages ++= Option(messages).getOrElse(Seq.empty)
    preparedSnapshot = None
    transitionToPrepared()
  }

  private def transitionToPrepared(): Unit = {
    while (true) {
      val current = state.get()
      current match {
        case State.Writing | State.Prepared =>
          if (state.compareAndSet(current, State.Prepared)) {
            return
          }
        case State.Aborted | State.Returned | State.CloseFailed | State.Closed =>
          logWarning(
            s"Ignoring prepared registration for table $tableName because cleanup is already " +
              s"in state $current.")
          return
      }
    }
  }

  /** Java-friendly wrapper for procedure callers. */
  def setPreparedJava(messages: java.util.List[CommitMessage]): Unit = {
    setPrepared(Option(messages).map(_.asScala.toSeq).getOrElse(Seq.empty))
  }

  /** Java-friendly incremental registration for procedure callers. */
  def addPreparedJava(messages: java.util.List[CommitMessage]): Unit = {
    addPrepared(Option(messages).map(_.asScala.toSeq).getOrElse(Seq.empty))
  }

  def markReturned(): Unit = {
    // Force Returned from Writing or Prepared so close() does not treat the attempt as
    // "close without return" and abort. Files remain reclaimable: Spark may still kill
    // the attempt after commit() returns but before DataWritingSparkTaskResult is
    // accepted (see abortIfNeeded / completion listener). Snapshot-published callers
    // must use {@link #markCommitted} instead so a later abort cannot delete live files.
    while (true) {
      val current = state.get()
      current match {
        case State.Prepared | State.Writing =>
          if (state.compareAndSet(current, State.Returned)) {
            return
          }
        case State.Returned | State.CloseFailed | State.Aborted | State.Closed =>
          return
      }
    }
  }

  /**
   * Mark that prepared files are already published into a snapshot (procedure / driver-local
   * commit). Clears prepared messages so a later abort or interrupted completion listener cannot
   * delete live snapshot files, then behaves like {@link #markReturned} for close().
   */
  def markCommitted(): Unit = {
    preparedMessages.clear()
    preparedSnapshot = Some(Seq.empty)
    markReturned()
  }

  def abortPrepared(): Unit = abortIfNeeded("manual abort")

  def close(): Unit = {
    state.get() match {
      case State.Closed =>
        // Already closed: a previous close() ran the writer/ioManager close. Spark always
        // calls DataWriter#close() again after commit()/abort(), so this must be a no-op
        // rather than a double-close.
        ()
      case State.Returned =>
        // Normal success path: a writer/ioManager close failure must propagate so the
        // driver does not commit an incomplete result. Only mark the attempt Closed after
        // close succeeds. On failure for Spark result-handoff paths, abort prepared files
        // immediately — V1 / SparkAttemptWrite only close once and never call abort().
        try {
          runWithClearedThreadInterrupt {
            closeUnprepared()
          }
          state.compareAndSet(State.Returned, State.Closed)
        } catch {
          case e: Exception =>
            state.compareAndSet(State.Returned, State.CloseFailed)
            abortIfNeeded("returned writer close failure")
            throw e
        }
      case State.CloseFailed =>
        // Follow-up close after a Returned-path close failure. Abort if still needed
        // (idempotent when the failure path already aborted).
        abortIfNeeded("close after returned writer close failure")
      case State.Aborted =>
        // Already aborted: best-effort residual close, suppress failures (the task is
        // already failing).
        runWithClearedThreadInterrupt {
          safeCloseUnprepared()
        }
      case State.Prepared =>
        abortIfNeeded("close without return")
        runWithClearedThreadInterrupt {
          safeCloseUnprepared()
        }
      case State.Writing =>
        if (state.compareAndSet(State.Writing, State.Closed)) {
          runWithClearedThreadInterrupt {
            safeCloseUnprepared()
          }
        } else {
          // State advanced concurrently (e.g. setPrepared/abort); handle the new state.
          close()
        }
    }
  }

  private def registerCompletionListener(): Unit = {
    val context = TaskContext.get()
    if (context != null) {
      context.addTaskCompletionListener[Unit](
        _ => {
          val current = state.get()
          // Spark kill: reclaim even from Returned/Closed — markReturned/close run before
          // Spark accepts DataWritingSparkTaskResult, so a speculative kill in that window
          // would otherwise leave prepared files. Only Spark's explicit kill
          // (context.isInterrupted) triggers this, NOT a stray thread interrupt flag:
          // a successful winner attempt must never see its committed files deleted by a
          // false-positive thread interrupt. CloseFailed without interrupt: reclaim when
          // the caller never invokes abort()/close again.
          if (current != State.Aborted && (context.isInterrupted() || current == State.CloseFailed)) {
            abortIfNeeded("task completion listener")
          }
        })
    }
  }

  private def abortIfNeeded(reason: String): Unit = {
    val current = state.get()
    if (current == State.Aborted) {
      return
    }
    // Returned/Closed still abortable: commit() calls markReturned()+close() before Spark
    // constructs/delivers DataWritingSparkTaskResult. A loser kill in that window sets
    // interrupted and/or invokes DataWriter.abort(); both must reclaim prepared files.
    // Snapshot-published callers use markCommitted() which clears prepared messages first.
    if (!state.compareAndSet(current, State.Aborted)) {
      return
    }

    val context = TaskContext.get()
    val stageId = if (context != null) context.stageId() else -1
    val partitionId = if (context != null) context.partitionId() else -1
    val attemptNumber = if (context != null) context.attemptNumber() else -1

    // Spark kill sets Thread.interrupt(); clear it around abort deletes so HDFS RPC is
    // not rejected with InterruptedIOException, then restore for Spark task teardown.
    // Always close the unprepared writer as well: multi-file writers (data evolution,
    // append compact) can have both prepared messages and an in-flight file open.
    runWithClearedThreadInterrupt {
      val messages = preparedSnapshot.getOrElse {
        val s = preparedMessages.toSeq
        preparedSnapshot = Some(s)
        s
      }
      try {
        if (messages.nonEmpty) {
          logInfo(
            s"Aborting ${messages.size} prepared commit messages for table $tableName " +
              s"(commitUser=$commitUser, stage=$stageId, partition=$partitionId, " +
              s"attempt=$attemptNumber) due to $reason.")
          var tableCommit: BatchTableCommit = null
          try {
            tableCommit = writeBuilder.newCommit()
            tableCommit.abort(messages.asJava)
            SparkAttemptCleanup.notifyAbortedMessagesProbe(messages)
          } catch {
            case e: Exception =>
              logWarning(
                s"Failed to abort prepared commit messages for table $tableName " +
                  s"(commitUser=$commitUser, stage=$stageId, partition=$partitionId, " +
                  s"attempt=$attemptNumber).",
                e
              )
          } finally {
            if (tableCommit != null) {
              try tableCommit.close()
              catch { case _: Exception => }
            }
          }
        } else {
          logInfo(
            s"Closing unprepared writer for table $tableName (commitUser=$commitUser, " +
              s"stage=$stageId, partition=$partitionId, attempt=$attemptNumber) due to $reason.")
        }
      } finally {
        // Writer cleanup must run even when creating the abort commit itself fails.
        safeCloseUnprepared()
      }
    }
  }

  private def safeCloseUnprepared(): Unit = {
    try closeUnprepared()
    catch {
      case e: Exception =>
        logWarning(s"Failed to close unprepared writer for table $tableName.", e)
    }
  }

  /**
   * Temporarily clears the thread interrupt flag for best-effort abort/cleanup IO, then restores
   * it. Hadoop RPC deletes fail with InterruptedIOException while the flag is set.
   */
  private def runWithClearedThreadInterrupt(block: => Unit): Unit = {
    val interrupted = Thread.interrupted()
    try {
      block
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt()
      }
    }
  }

  private def isTaskInterrupted: Boolean = {
    val context = TaskContext.get()
    (context != null && context.isInterrupted()) || Thread.currentThread().isInterrupted
  }

  private def taskKilledException(): TaskKilledException = {
    new TaskKilledException("Paimon writer interrupted for speculative execution cleanup")
  }
}

object SparkAttemptCleanup {

  /**
   * Best-effort commit user for logging. {@link BatchWriteBuilder#commitUser()} is an optional
   * extension point whose default implementation throws; builders that do not expose a commit
   * user (e.g. format tables or custom implementations) must not break writer construction. The
   * value is only used for abort logging — abort itself goes through {@link
   * BatchWriteBuilder#newCommit()}.
   */
  def commitUserOrUnknown(writeBuilder: BatchWriteBuilder): String = {
    try {
      writeBuilder.commitUser()
    } catch {
      case _: UnsupportedOperationException => "<unknown>"
    }
  }

  /**
   * Optional test probe invoked after prepared messages are aborted successfully. Not for
   * production use; IT cases install a probe to assert synchronous loser cleanup before orphan
   * cleaner runs. The probe is JVM-global: ITs that install it must not run concurrently in the
   * same JVM (each test must clear it afterwards, see {@code @AfterEach} usage).
   */
  @volatile private var abortedMessagesProbe
      : java.util.function.Consumer[java.util.List[CommitMessage]] = null

  /** Install or clear the abort probe used by speculative-write IT cases. */
  def setAbortedMessagesProbe(
      probe: java.util.function.Consumer[java.util.List[CommitMessage]]): Unit = {
    abortedMessagesProbe = probe
  }

  private def notifyAbortedMessagesProbe(messages: Seq[CommitMessage]): Unit = {
    val probe = abortedMessagesProbe
    if (probe != null) {
      probe.accept(messages.asJava)
    }
  }

  def forJava(
      tableName: String,
      commitUser: String,
      writeBuilder: BatchWriteBuilder,
      closeUnprepared: Runnable): SparkAttemptCleanup = {
    new SparkAttemptCleanup(tableName, commitUser, writeBuilder, () => closeUnprepared.run())
  }
}
