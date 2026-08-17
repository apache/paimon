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
import org.apache.paimon.table.DataTable
import org.apache.paimon.table.source.{ReadBuilder, StreamDataTableScan}

import org.mockito.Mockito.{doNothing, doThrow, mock, never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{IOException, UncheckedIOException}
import java.util.Collections

class PaimonMicroBatchStreamTest extends AnyFunSuite {

  test("advance consumer only after the last split of a snapshot is committed") {
    val (stream, scan) = createStreamWithConsumer()
    val partial = PaimonSourceOffset.withTotalSplits(
      snapshotId = 5L,
      index = 0L,
      scanSnapshot = false,
      totalSplits = 2L)
    val complete = PaimonSourceOffset.withTotalSplits(
      snapshotId = 5L,
      index = 1L,
      scanSnapshot = false,
      totalSplits = 2L)

    stream.commit(partial)
    verify(scan, never()).notifyCheckpointComplete(6L)

    stream.commit(complete)
    verify(scan).notifyCheckpointComplete(6L)
  }

  test("do not advance consumer from a legacy offset without total splits") {
    val (stream, scan) = createStreamWithConsumer()
    val legacyOffset = PaimonSourceOffset("""{"snapshotId":5,"index":1,"scanSnapshot":false}""")

    stream.commit(legacyOffset)

    verify(scan, never()).notifyCheckpointComplete(6L)
  }

  test("propagate consumer update failure and allow retry") {
    val (stream, scan) = createStreamWithConsumer()
    val complete = PaimonSourceOffset.withTotalSplits(
      snapshotId = 5L,
      index = 1L,
      scanSnapshot = false,
      totalSplits = 2L)
    val failure = new UncheckedIOException(new IOException("expected failure"))
    doThrow(failure).doNothing().when(scan).notifyCheckpointComplete(6L)

    val thrown = intercept[UncheckedIOException] {
      stream.commit(complete)
    }
    assert(thrown eq failure)

    stream.commit(complete)
    verify(scan, times(2)).notifyCheckpointComplete(6L)
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
}
