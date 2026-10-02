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
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.PaimonRecordReaderIterator
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.stats.SimpleStats
import org.apache.paimon.table.source.{DataSplit, QueryAuthSplit, Split}

import org.apache.spark.sql.types.BinaryType
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

  test("partition metadata columns are refused on a query-authorized split") {
    val partitionColumn =
      PaimonMetadataColumn(-1, PaimonMetadataColumn.PARTITION_COLUMN, BinaryType)
    val error = intercept[RuntimeException] {
      PaimonRecordReaderIterator(null, Seq(partitionColumn), authorized(dataSplit()))
    }
    assert(error.getMessage.contains("DataSplit"))
  }
}
