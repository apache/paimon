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

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.options.Options
import org.apache.paimon.spark.PaimonPartitionReaderFactory
import org.apache.paimon.table.{CatalogEnvironment, DataTable, FileStoreTable}
import org.apache.paimon.table.source.{ReadBuilder, StreamDataTableScan}
import org.apache.paimon.types.{BlobType, DataField, IntType, RowType}

import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.{doNothing, doThrow, mock, never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{IOException, UncheckedIOException}
import java.util.{Arrays, Collections, HashMap}

class PaimonMicroBatchStreamTest extends AnyFunSuite {

  test("never advance consumer past an incomplete snapshot") {
    val (stream, scan) = createStreamWithConsumer()
    val partial = consumerOffset(index = 0L, totalSplits = 2L)
    val complete = consumerOffset(index = 1L, totalSplits = 2L)

    stream.commit(partial)
    verify(scan).notifyCheckpointComplete(5L)
    verify(scan, never()).notifyCheckpointComplete(6L)

    stream.commit(complete)
    verify(scan).notifyCheckpointComplete(6L)
  }

  test("do not advance consumer from a legacy offset without total splits") {
    val (stream, scan) = createStreamWithConsumer()
    val legacyOffset = PaimonSourceOffset("""{"snapshotId":5,"index":1,"scanSnapshot":false}""")

    stream.commit(legacyOffset)

    verify(scan, never()).notifyCheckpointComplete(anyLong())
  }

  test("propagate consumer update failure and allow retry") {
    val (stream, scan) = createStreamWithConsumer()
    val complete = consumerOffset(index = 1L, totalSplits = 2L)
    val failure = new UncheckedIOException(new IOException("expected failure"))
    doThrow(failure).doNothing().when(scan).notifyCheckpointComplete(6L)

    val thrown = intercept[UncheckedIOException] {
      stream.commit(complete)
    }
    assert(thrown eq failure)

    stream.commit(complete)
    verify(scan, times(2)).notifyCheckpointComplete(6L)
  }

  test("propagate empty snapshot consumer update failure and allow retry") {
    val (stream, scan) = createStreamWithConsumer()
    val empty = PaimonSourceOffset.withTotalSplits(
      snapshotId = 6L,
      index = PaimonSourceOffset.INIT_OFFSET_INDEX,
      scanSnapshot = false,
      totalSplits = 0L)
    val failure = new UncheckedIOException(new IOException("expected failure"))
    doThrow(failure).doNothing().when(scan).notifyCheckpointComplete(6L)

    val thrown = intercept[UncheckedIOException] {
      stream.commit(empty)
    }
    assert(thrown eq failure)

    stream.commit(empty)
    verify(scan, times(2)).notifyCheckpointComplete(6L)
  }

  test("skip blob descriptor source table loading when descriptor output is requested") {
    val table = mock(classOf[FileStoreTable])
    val readBuilder = mock(classOf[ReadBuilder])
    val options = new HashMap[String, String]
    options.put(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true")
    options.put(CoreOptions.BLOB_DESCRIPTOR_SOURCE_TABLE.key(), "db.missing_source")
    when(table.options()).thenReturn(options)
    when(readBuilder.readType()).thenReturn(blobRowType)

    val stream = new PaimonMicroBatchStream(table, readBuilder, "unused")
    val readerFactory = stream.createReaderFactory().asInstanceOf[PaimonPartitionReaderFactory]

    assert(readerFactory.uriReaderFactory == null)
    verify(table, never()).catalogEnvironment()
  }

  test("propagate blob descriptor reader to streaming reader factory") {
    val table = mock(classOf[FileStoreTable])
    val readBuilder = mock(classOf[ReadBuilder])
    val catalogEnvironment = mock(classOf[CatalogEnvironment])
    val options = Collections.singletonMap(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "picture")
    when(table.options()).thenReturn(options)
    when(table.coreOptions()).thenReturn(CoreOptions.fromMap(options))
    when(table.catalogEnvironment()).thenReturn(catalogEnvironment)
    when(catalogEnvironment.catalogContext()).thenReturn(CatalogContext.create(new Options))
    when(readBuilder.readType()).thenReturn(blobRowType)

    val stream = new PaimonMicroBatchStream(table, readBuilder, "unused")
    val readerFactory = stream.createReaderFactory().asInstanceOf[PaimonPartitionReaderFactory]

    assert(readerFactory.uriReaderFactory != null)
    assert(readerFactory.blobDescriptorFieldIndices.sameElements(Array(1)))
  }

  private def consumerOffset(index: Long, totalSplits: Long): PaimonSourceOffset = {
    PaimonSourceOffset.withTotalSplits(
      snapshotId = 5L,
      index = index,
      scanSnapshot = false,
      totalSplits = totalSplits)
  }

  private def createStreamWithConsumer(): (PaimonMicroBatchStream, StreamDataTableScan) = {
    val table = mock(classOf[DataTable])
    val scan = mock(classOf[StreamDataTableScan])
    val readBuilder = mock(classOf[ReadBuilder])
    when(table.options())
      .thenReturn(Collections.singletonMap(CoreOptions.CONSUMER_ID.key(), "spark-consumer"))
    when(table.newStreamScan()).thenReturn(scan)
    when(scan.dropStats()).thenReturn(scan)
    (new PaimonMicroBatchStream(table, readBuilder, "unused"), scan)
  }

  private def blobRowType: RowType = {
    new RowType(
      Arrays.asList(new DataField(0, "id", new IntType), new DataField(1, "picture", new BlobType)))
  }
}
