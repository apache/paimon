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

package org.apache.paimon.spark.util

import org.apache.paimon.catalog.TableQueryAuthResult
import org.apache.paimon.data.{BinaryRow, BinaryString, GenericRow, InternalRow}
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.PaimonRecordReaderIterator
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.stats.SimpleStats
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackSplitImpl
import org.apache.paimon.table.source.{ChainSplit, DataSplit, QueryAuthSplit, Split}
import org.apache.paimon.types.{DataType, DataTypes, RowType}
import org.apache.paimon.utils.IteratorRecordReader

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

class SplitUtilsQueryAuthTest extends AnyFunSuite {

  private def dataSplit(): DataSplit = {
    val file = DataFileMeta.forAppend(
      "data-0.parquet",
      1024L,
      10L,
      SimpleStats.EMPTY_STATS,
      0L,
      9L,
      0L,
      Collections.emptyList[String](),
      null,
      null,
      null,
      null,
      null,
      null)
    DataSplit
      .builder()
      .withSnapshot(1L)
      .withPartition(BinaryRow.EMPTY_ROW)
      .withBucket(0)
      .withBucketPath("bucket-0")
      .withDataFiles(Collections.singletonList(file))
      .build()
  }

  // Synthetic wrapper: the supplied readers already contain post-authorization rows.
  private def authorized(split: Split): Split =
    new QueryAuthSplit(split, new TableQueryAuthResult(null, null))

  test("statistics survive the query-authorization wrapper") {
    val split = dataSplit()
    assert(SplitUtils.splitSize(split) == 1024L)
    assert(SplitUtils.dataFileCount(split) == 1L)

    val wrapped = authorized(split)
    assert(SplitUtils.splitSize(wrapped) == 1024L, "split size collapsed under query auth")
    assert(SplitUtils.dataFileCount(wrapped) == 1L, "file count collapsed under query auth")
  }

  private val partitionColumn =
    PaimonMetadataColumn.PARTITION(StructType(Seq(StructField("dt", StringType))))

  private class EmptyReader extends RecordReader[InternalRow] {
    var closed = false

    override def readBatch(): RecordReader.RecordIterator[InternalRow] = null

    override def close(): Unit = {
      closed = true
    }
  }

  test("empty partition metadata reads release the reader") {
    val reader = new EmptyReader
    val iterator =
      PaimonRecordReaderIterator(
        reader,
        Seq(partitionColumn),
        dataSplit(),
        RowType.of(Array[DataType](DataTypes.STRING()), Array("dt")))
    try {
      assert(!iterator.hasNext)
    } finally {
      iterator.close()
    }
    assert(reader.closed)
  }

  private def chainAndAuthorizedSplits(): Seq[Split] = {
    val chainSplit = ChainSplit.from(dataSplit(), "snapshot")
    Seq(
      authorized(dataSplit()),
      chainSplit,
      authorized(chainSplit),
      new FallbackSplitImpl(chainSplit, true),
      new FallbackSplitImpl(authorized(chainSplit), true))
  }

  test("partition metadata uses visible rows for data, authorized and chain splits") {
    (Seq(dataSplit()) ++ chainAndAuthorizedSplits()).foreach {
      split =>
        val rows = java.util.Arrays.asList[InternalRow](
          GenericRow.of(BinaryString.fromString("masked-a")),
          GenericRow.of(BinaryString.fromString("masked-b")),
          GenericRow.of(null.asInstanceOf[AnyRef]))
        val iterator = PaimonRecordReaderIterator(
          new IteratorRecordReader[InternalRow](rows.iterator()),
          Seq(partitionColumn),
          split,
          RowType.of(Array[DataType](DataTypes.STRING()), Array("dt")))
        try {
          assert(iterator.next().getRow(1, 1).getString(0).toString == "masked-a")
          assert(iterator.next().getRow(1, 1).getString(0).toString == "masked-b")
          assert(iterator.next().getRow(1, 1).isNullAt(0))
          assert(!iterator.hasNext)
        } finally {
          iterator.close()
        }
    }
  }

  test("bucket metadata supports authorized and chain splits") {
    chainAndAuthorizedSplits().foreach {
      split =>
        val reader = new IteratorRecordReader[InternalRow](
          Collections.singletonList[InternalRow](GenericRow.of(Int.box(42))).iterator())
        val iterator =
          PaimonRecordReaderIterator(reader, Seq(PaimonMetadataColumn.BUCKET), split)
        try {
          assert(iterator.hasNext)
          val row = iterator.next()
          assert(row.getInt(0) == 42)
          assert(row.getInt(1) == 0)
          assert(!iterator.hasNext)
        } finally {
          iterator.close()
        }
    }
  }
}
