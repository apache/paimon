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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.table.sink.{BatchTableWrite, SinkRecord}

import org.apache.spark.internal.Logging

import scala.collection.mutable

trait DataWriteHelper extends Logging {

  val write: BatchTableWrite

  val fullCompactionDeltaCommits: Option[Int]

  /** For batch write, batchId is -1, for streaming write, batchId is the current batch id (>= 0). */
  val batchId: Long

  private val needFullCompaction: Boolean = {
    fullCompactionDeltaCommits match {
      case Some(deltaCommits) =>
        deltaCommits > 0 && (batchId == -1 || (batchId + 1) % deltaCommits == 0)
      case None => false
    }
  }

  private val writtenBuckets = mutable.Set[(BinaryRow, Integer)]()

  def postWrite(record: SinkRecord): Unit = {
    if (record == null) {
      return
    }

    if (needFullCompaction && !writtenBuckets.contains((record.partition(), record.bucket()))) {
      writtenBuckets.add((record.partition().copy(), record.bucket()))
    }
  }

  def preFinish(): Unit = {
    if (needFullCompaction && writtenBuckets.nonEmpty) {
      logInfo("Start to compact buckets: " + writtenBuckets)
      writtenBuckets.foreach(
        (bucket: (BinaryRow, Integer)) => {
          write.compact(bucket._1, bucket._2, true)
        })
    }
  }
}
