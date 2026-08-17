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

package org.apache.paimon.spark

import org.apache.paimon.table.source.Split

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, SupportsReportPartitioning}

import java.util.{List => JList, Objects, Optional}

import scala.util.control.NonFatal

trait PaimonInputPartition extends InputPartition {
  def splits: Seq[Split]

  def rowCount(): Long = {
    splits.map(_.rowCount()).sum
  }

  // Used to avoid checking [[PaimonBucketedInputPartition]] to workaround for multi Spark version
  def bucketed = false
}

case class SimplePaimonInputPartition(splits: Seq[Split]) extends PaimonInputPartition

final private[spark] class PaimonMicroBatchMetadata private[spark] (
    val sourceId: String,
    val startOffset: String,
    val endOffset: String,
    val splitCount: Int,
    @transient private var writtenColumnIdsThunk: () => Optional[JList[Integer]])
  extends Serializable {

  @transient private lazy val cachedWrittenColumnIds: Optional[JList[Integer]] = {
    try {
      val thunk = writtenColumnIdsThunk
      writtenColumnIdsThunk = null
      val supplied = if (thunk == null) null else thunk()
      if (supplied == null) Optional.empty() else supplied
    } catch {
      case NonFatal(_) => Optional.empty()
      case _: LinkageError => Optional.empty()
    }
  }

  def writtenColumnIds: Optional[JList[Integer]] = cachedWrittenColumnIds

  override def equals(other: Any): Boolean =
    other match {
      case that: PaimonMicroBatchMetadata =>
        sourceId == that.sourceId &&
        startOffset == that.startOffset &&
        endOffset == that.endOffset &&
        splitCount == that.splitCount
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hash(sourceId, startOffset, endOffset, Integer.valueOf(splitCount))
}

private[spark] case class PaimonMicroBatchInputPartition(
    splits: Seq[Split],
    @transient metadata: PaimonMicroBatchMetadata)
  extends PaimonInputPartition

object PaimonInputPartition {
  def apply(split: Split): PaimonInputPartition = {
    SimplePaimonInputPartition(Seq(split))
  }

  def apply(splits: Seq[Split]): PaimonInputPartition = {
    SimplePaimonInputPartition(splits)
  }
}

/** Bucketed input partition should work with [[SupportsReportPartitioning]] together. */
case class PaimonBucketedInputPartition(splits: Seq[Split], bucket: Int)
  extends PaimonInputPartition
  with HasPartitionKey {
  override def partitionKey(): InternalRow = new GenericInternalRow(Array(bucket.asInstanceOf[Any]))
  override def bucketed: Boolean = true
}
