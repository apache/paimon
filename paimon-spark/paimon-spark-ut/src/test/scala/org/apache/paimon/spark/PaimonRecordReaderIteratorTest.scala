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

import org.apache.paimon.catalog.TableQueryAuthResult
import org.apache.paimon.data.{BinaryRow, InternalRow => PaimonInternalRow}
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.FallbackReadFileStoreTable
import org.apache.paimon.table.source.{DataSplit, QueryAuthSplit}

import org.junit.jupiter.api.Assertions.assertDoesNotThrow

import java.util.Collections

/** Tests for [[PaimonRecordReaderIterator]]. */
class PaimonRecordReaderIteratorTest extends PaimonSparkTestBase {

  test("Paimon: fallback query auth split exposes metadata") {
    val dataSplit = DataSplit
      .builder()
      .withSnapshot(1L)
      .withPartition(BinaryRow.singleColumn(1))
      .withBucket(2)
      .withDataFiles(Collections.emptyList())
      .isStreaming(false)
      .withBucketPath("")
      .build()
    val queryAuthSplit = new QueryAuthSplit(
      dataSplit,
      new TableQueryAuthResult(null, Collections.singletonMap("f0", "mask")))
    val fallbackSplit =
      new FallbackReadFileStoreTable.FallbackSplitImpl(queryAuthSplit, false)
    val emptyReader = new RecordReader[PaimonInternalRow] {
      override def readBatch(): RecordReader.RecordIterator[PaimonInternalRow] = null

      override def close(): Unit = {}
    }

    assertDoesNotThrow(
      () =>
        PaimonRecordReaderIterator(emptyReader, Seq(PaimonMetadataColumn.BUCKET), fallbackSplit))
  }
}
