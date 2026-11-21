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

import org.apache.paimon.data.{BinaryRow, Timestamp}
import org.apache.paimon.disk.IOManagerImpl
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.stats.SimpleStats
import org.apache.paimon.table.source.{DataSplit, SplitSerializer}
import org.apache.paimon.utils.InstantiationUtil

import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.{Files, Path}
import java.util.{AbstractList, Collections, Iterator => JIterator, List => JList}

import scala.collection.JavaConverters._

class DataEvolutionMergeMetadataTest extends AnyFunSuite {

  test("range summaries preserve equal starts, gaps and sorted duplicate blob IDs") {
    val files = Seq(
      file("late.parquet", 20L, 2),
      file("base.parquet", 0L, 10),
      file("partial.parquet", 0L, 12),
      file("null.parquet", null, 2),
      file("ignored.vector.bin", 100L, 50),
      file("a.blob", 7L, 1),
      file("b.blob", 3L, 1),
      file("c.blob", 7L, 1),
      file("d.blob", 23L, 1)
    )
    val summary = DataEvolutionMergeMetadata.summarize(Seq(split(files.asJava)))
    assert(summary.firstRowIds == Vector(0L, 20L))
    assert(summary.rowIdRangeEnds == Vector(12L, 22L))
    assert(summary.firstRowIdToBlobFirstRowIds == Map(0L -> List(3L, 7L, 7L), 20L -> List(23L)))
    val restored = InstantiationUtil.deserializeObject[scala.collection.immutable.IndexedSeq[Long]](
      InstantiationUtil.serializeObject(summary.firstRowIds),
      getClass.getClassLoader)
    assert(restored == summary.firstRowIds)
  }

  test("summary consumes each normal file before requesting the next metadata object") {
    var consumed = true
    val files = new AbstractList[DataFileMeta] {
      override def size(): Int = 4
      override def get(index: Int): DataFileMeta = throw new UnsupportedOperationException()
      override def iterator(): JIterator[DataFileMeta] = new JIterator[DataFileMeta] {
        private var index = 0
        override def hasNext: Boolean = index < size()
        override def next(): DataFileMeta = {
          assert(consumed, "Fetched another metadata object before consuming the current one")
          consumed = false
          val value = file(s"$index.parquet", index * 2L, 2)
          index += 1
          Proxy
            .newProxyInstance(
              getClass.getClassLoader,
              Array(classOf[DataFileMeta]),
              new InvocationHandler {
                override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
                  if (method.getName == "rowCount") consumed = true
                  method.invoke(value, Option(args).getOrElse(Array.empty[AnyRef]): _*)
                }
              }
            )
            .asInstanceOf[DataFileMeta]
        }
      }
    }
    val source = new DataSplit { override def dataFiles(): JList[DataFileMeta] = files }
    val summary = DataEvolutionMergeMetadata.summarize(Seq(source))
    assert(consumed)
    assert(summary.firstRowIds == Vector(0L, 2L, 4L, 6L))
    assert(summary.rowIdRangeEnds == Vector(2L, 4L, 6L, 8L))
  }

  test("range summaries serialize primitive arrays without a boxed per-range object graph") {
    val count = 20000
    val files = (0 until count).map(i => file(s"$i.parquet", i * 2L, 2))
    val summary = DataEvolutionMergeMetadata.summarize(Seq(split(files.asJava)))
    assert(summary.firstRowIds.size == count && summary.rowIdRangeEnds.size == count)
    val bytes = InstantiationUtil.serializeObject((summary.firstRowIds, summary.rowIdRangeEnds))
    assert(bytes.length < count * 17, s"Unexpected serialized range summary: ${bytes.length}")
  }

  test("preparation owner retains original and pruned stores until all consumers finish") {
    withOwner(1024 * 1024) {
      (owner, io, directory) =>
        val input = owner.newStore()
        (0 until 4).foreach(i => input.append(file(s"$i.parquet", i * 2L, 2)))
        input.seal()
        val source = split(input.slice(0, 4))
        val selected = owner.prune(Seq(source), value => value.firstRowId().longValue() >= 4L)
        assert(selected.size == 1)
        assert(
          selected.head.dataFiles().asScala.map(_.fileName()).toVector ==
            Vector("2.parquet", "3.parquet"))
        assert(selected.head.dataFiles().getClass.getName.contains("DataFileMetaStore$Slice"))
        assert(!io.closed)
        val transported = SplitSerializer.serialize(selected.head)
        owner.close()
        assert(io.closed)
        intercept[IllegalStateException](source.dataFiles().get(0))
        intercept[IllegalStateException](selected.head.dataFiles().get(0))
        assert(
          SplitSerializer
            .deserialize(transported)
            .asInstanceOf[DataSplit]
            .dataFiles()
            .asScala
            .map(_.fileName())
            .toVector == Vector("2.parquet", "3.parquet"))
        assertEmpty(directory)
    }
  }

  test("preparation owner cleans a disk failure and rejects new stores after closing") {
    withOwner(8) {
      (owner, io, directory) =>
        val output = owner.newStore()
        intercept[IOException](output.append(file("large.parquet", 0L, 2)))
        owner.close()
        assert(io.closed)
        intercept[IllegalStateException](owner.newStore())
        assertEmpty(directory)
    }
  }

  test("failed IO cleanup still closes stores and can be retried") {
    val directory = Files.createTempDirectory("paimon-merge-owner-error-")
    val stopped = new IOException("IO close failed")
    val io = new ObservedIO(directory) {
      private var failed = false
      override def close(): Unit = {
        super.close()
        if (!failed) {
          failed = true
          throw stopped
        }
      }
    }
    val owner = new DataEvolutionMergeMetadata(io, 1024 * 1024)
    try {
      owner.newStore().append(file("owned.parquet", 0L, 2))
      assert(intercept[IOException](owner.close()) eq stopped)
      assert(io.closeCalls == 1)
      assertEmpty(directory)
      owner.close()
      assert(io.closeCalls == 2)
      intercept[IllegalStateException](owner.newStore())
    } finally {
      owner.close()
      Files.delete(directory)
    }
  }

  private def withOwner(limit: Long)(
      action: (DataEvolutionMergeMetadata, ObservedIO, Path) => Unit): Unit = {
    val directory = Files.createTempDirectory("paimon-merge-owner-")
    val io = new ObservedIO(directory)
    val owner = new DataEvolutionMergeMetadata(io, limit)
    try action(owner, io, directory)
    finally {
      owner.close()
      assertEmpty(directory)
      Files.delete(directory)
    }
  }

  private def assertEmpty(directory: Path): Unit = {
    val files = Files.list(directory)
    try assert(files.count() == 0L)
    finally files.close()
  }

  private class ObservedIO(directory: Path) extends IOManagerImpl(directory.toString) {
    var closed = false
    var closeCalls = 0
    override def close(): Unit = { closeCalls += 1; closed = true; super.close() }
  }

  private def file(name: String, first: java.lang.Long, count: Long): DataFileMeta =
    DataFileMeta.create(
      name,
      100L,
      count,
      DataFileMeta.EMPTY_MIN_KEY,
      DataFileMeta.EMPTY_MAX_KEY,
      SimpleStats.EMPTY_STATS,
      SimpleStats.EMPTY_STATS,
      1L,
      1L,
      0L,
      0,
      Collections.emptyList[String](),
      Timestamp.fromEpochMillis(12345),
      0L,
      null,
      FileSource.APPEND,
      null,
      null,
      first,
      null,
      null
    )

  private def split(files: JList[DataFileMeta]): DataSplit = DataSplit
    .builder()
    .withSnapshot(17)
    .withPartition(BinaryRow.EMPTY_ROW)
    .withBucket(0)
    .withBucketPath("/unused/bucket-0")
    .withDataFiles(files)
    .rawConvertible(false)
    .build()
}
