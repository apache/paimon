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

package org.apache.paimon.spark.sources

import org.apache.paimon.spark.util.JsonUtils
import org.apache.paimon.table.source.snapshot.StartingContext

import org.apache.spark.sql.connector.read.streaming.Offset

/**
 * The implementation of [[Offset]] for paimon spark streaming source.
 *
 * @param snapshotId
 *   the current snapshot id
 * @param index
 *   the index position of file in the current snapshot, start from 0
 * @param scanSnapshot
 *   whether to scan all files in the current snapshot
 */
case class PaimonSourceOffset(snapshotId: Long, index: Long, scanSnapshot: Boolean)
  extends Offset
  with Comparable[PaimonSourceOffset] {

  override def json(): String = {
    JsonUtils.toJson(this)
  }

  override def compareTo(o: PaimonSourceOffset): Int = {
    o match {
      case PaimonSourceOffset(snapshotId, index, scanSnapshot) =>
        val diff = this.snapshotId.compare(snapshotId)
        if (diff == 0) {
          this.index.compare(index)
        } else {
          diff
        }
    }
  }
}

object PaimonSourceOffset {
  //  index of the init offset, for we filter offset by (startOffset, endOffset]
  val INIT_OFFSET_INDEX: Long = -1L

  def apply(version: Long, index: Long, scanSnapshot: Boolean): PaimonSourceOffset = {
    new PaimonSourceOffset(
      version,
      index,
      scanSnapshot
    )
  }

  def apply(offset: Any): PaimonSourceOffset = {
    offset match {
      case o: PaimonSourceOffset => o
      case json: String => JsonUtils.fromJson[PaimonSourceOffset](json)
      case sc: StartingContext =>
        PaimonSourceOffset(sc.getSnapshotId, INIT_OFFSET_INDEX, sc.getScanFullSnapshot)
      case _ => throw new IllegalArgumentException(s"Can't parse $offset to PaimonSourceOffset.")
    }
  }

  def gt(indexedDataSplit: IndexedDataSplit, start: PaimonSourceOffset): Boolean = {
    indexedDataSplit.snapshotId > start.snapshotId ||
    (indexedDataSplit.snapshotId == start.snapshotId && indexedDataSplit.index > start.index)
  }

  def le(indexedDataSplit: IndexedDataSplit, end: PaimonSourceOffset): Boolean = {
    indexedDataSplit.snapshotId < end.snapshotId ||
    (indexedDataSplit.snapshotId == end.snapshotId && indexedDataSplit.index <= end.index)
  }
}
