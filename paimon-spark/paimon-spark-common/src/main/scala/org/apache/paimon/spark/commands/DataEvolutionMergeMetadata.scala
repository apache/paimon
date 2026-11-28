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

package org.apache.paimon.spark.commands

import org.apache.paimon.disk.IOManager
import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.table.source.{DataFileMetaStore, DataSplit}
import org.apache.paimon.table.source.snapshot.SnapshotReader
import org.apache.paimon.types.VectorType.isVectorStoreFile
import org.apache.paimon.utils.LongArrayList

import java.nio.file.Paths
import java.util.{Arrays, TreeMap}

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Owns replay files until all internal MERGE preparation jobs have finished. */
final private[spark] class DataEvolutionMergeMetadata(val ioManager: IOManager, maxDiskBytes: Long)
  extends AutoCloseable {

  private val stores = new ArrayBuffer[DataFileMetaStore]
  private var closed = false
  private var fullyClosed = false

  def newStore(): DataFileMetaStore = {
    checkOpen()
    val store = new DataFileMetaStore(Paths.get(ioManager.pickTempDir()), maxDiskBytes)
    try {
      stores += store
      store
    } catch {
      case failure: Throwable =>
        try store.close()
        catch {
          case cleanupFailure: Throwable =>
            if (cleanupFailure ne failure) failure.addSuppressed(cleanupFailure)
        }
        throw failure
    }
  }

  def read(reader: SnapshotReader): SnapshotReader.Plan =
    reader.readIncrementally(newStore(), ioManager)

  def prune(splits: Seq[DataSplit], keep: DataFileMeta => Boolean): Seq[DataSplit] =
    DataSplit.filterDataFiles(splits.asJava, file => keep(file), newStore()).asScala.toVector

  override def close(): Unit = {
    if (fullyClosed) return
    closed = true
    var failure: Throwable = null
    val resources = stores.reverseIterator.map(store => store: AutoCloseable) ++ Iterator(ioManager)
    resources.foreach {
      resource =>
        try resource.close()
        catch {
          case error: Throwable =>
            if (failure == null) failure = error
            else if (error ne failure) failure.addSuppressed(error)
        }
    }
    if (failure != null) throw failure
    fullyClosed = true
    stores.clear()
  }

  private def checkOpen(): Unit = {
    if (closed) throw new IllegalStateException("MERGE metadata owner is closed.")
  }
}

private[spark] object DataEvolutionMergeMetadata {

  case class RangeSummary(
      firstRowIds: immutable.IndexedSeq[Long],
      rowIdRangeEnds: immutable.IndexedSeq[Long],
      firstRowIdToBlobFirstRowIds: Map[Long, List[Long]])

  def summarize(splits: Seq[DataSplit]): RangeSummary = {
    val ranges = new TreeMap[java.lang.Long, java.lang.Long]()
    val blobs = new LongArrayList(16)
    splits.iterator.flatMap(_.dataFiles().asScala.iterator).foreach {
      file =>
        val first = file.firstRowId()
        if (isBlobFile(file.fileName())) {
          blobs.add(first.longValue())
        } else if (first != null && !isVectorStoreFile(file.fileName())) {
          val end = first.longValue() + file.rowCount()
          val previous = ranges.get(first)
          if (previous == null || end > previous.longValue()) ranges.put(first, end)
        }
    }
    val starts = new Array[Long](ranges.size())
    val ends = new Array[Long](ranges.size())
    val entries = ranges.entrySet().iterator()
    var index = 0
    while (entries.hasNext) {
      val entry = entries.next()
      starts(index) = entry.getKey.longValue()
      ends(index) = entry.getValue.longValue()
      index += 1
    }
    val blobIds = blobs.toArray
    Arrays.sort(blobIds)
    val mapping = new mutable.HashMap[Long, mutable.ListBuffer[Long]]
    blobIds.foreach {
      first =>
        if (starts.isEmpty) {
          throw new IllegalArgumentException("The input sorted sequence is empty.")
        }
        val found = Arrays.binarySearch(starts, first)
        val floor = if (found >= 0) found else -found - 2
        if (floor < 0) {
          throw new IllegalArgumentException(
            s"Value $first is less than the first element in the sorted sequence.")
        }
        mapping.getOrElseUpdate(starts(floor), mutable.ListBuffer.empty[Long]) += first
    }
    RangeSummary(
      new LongIndexedSeq(starts),
      new LongIndexedSeq(ends),
      mapping.iterator.map { case (first, values) => first -> values.toList }.toMap)
  }

  @SerialVersionUID(1L)
  final private class LongIndexedSeq(values: Array[Long])
    extends immutable.IndexedSeq[Long]
    with Serializable {
    override def length: Int = values.length
    override def apply(index: Int): Long = values(index)
  }
}
