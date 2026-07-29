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
import org.apache.paimon.table.sink.{BatchWriteBuilder, CommitMessage}

import scala.collection.JavaConverters._

/**
 * Shared helper for Spark task-side write attempts used by procedures (compact / rescale / chain
 * compact) and other Java/Scala callers that prepare commit messages inside mapPartitions.
 *
 * <p>V1 Dataset writes ({@code PaimonDataWrite} / {@code PaimonV2DataWriter}) embed {@link
 * SparkAttemptCleanup} directly rather than going through this helper.
 */
object SparkAttemptWrite {

  def run[R](
      table: FileStoreTable,
      writeBuilder: BatchWriteBuilder,
      closeUnprepared: () => Unit,
      write: SparkAttemptCleanup => Unit,
      prepareCommit: SparkAttemptCleanup => Seq[CommitMessage],
      toResult: Seq[CommitMessage] => R): R = {
    val cleanup =
      new SparkAttemptCleanup(
        table.fullName(),
        SparkAttemptCleanup.commitUserOrUnknown(writeBuilder),
        writeBuilder,
        closeUnprepared)
    try {
      write(cleanup)
      cleanup.checkInterrupted("before prepareCommit")
      val messages = prepareCommit(cleanup)
      cleanup.checkInterrupted("after prepareCommit")
      val result = toResult(messages)
      cleanup.checkInterrupted("before return")
      cleanup.markReturned()
      result
    } finally {
      cleanup.close()
    }
  }

  /** Java-friendly entry point for procedures and other Java callers. */
  def runJava[R](
      table: FileStoreTable,
      writeBuilder: BatchWriteBuilder,
      closeUnprepared: Runnable,
      write: java.util.function.Consumer[SparkAttemptCleanup],
      prepareCommit: java.util.function.Function[SparkAttemptCleanup, java.util.List[CommitMessage]],
      toResult: java.util.function.Function[java.util.List[CommitMessage], R]): R = {
    run(
      table,
      writeBuilder,
      () => runUnchecked(closeUnprepared),
      cleanup => write.accept(cleanup),
      cleanup =>
        Option(prepareCommit.apply(cleanup)).map(_.asScala.toSeq).getOrElse(Seq.empty),
      messages => toResult.apply(messages.asJava)
    )
  }

  private def runUnchecked(action: Runnable): Unit = {
    try action.run()
    catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }
}
