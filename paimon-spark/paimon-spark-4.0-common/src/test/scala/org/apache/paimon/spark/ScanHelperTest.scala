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

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.table.source.{DataSplit, Split}

import org.junit.jupiter.api.Assertions

import java.util.HashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

class ScanHelperTest extends PaimonSparkTestBase {

  test("Paimon: reshuffle splits") {
    withSQLConf(("spark.sql.leafNodeDefaultParallelism", "20")) {
      val splitNum = 5
      val fileNum = 100

      val files = scala.collection.mutable.ListBuffer.empty[DataFileMeta]
      0.until(fileNum).foreach {
        i =>
          val path = s"f$i.parquet"
          files += DataFileMeta.forAppend(path, 750000, 30000, null, 0, 29999, 1, FileSource.APPEND)
      }

      val dataSplits = mutable.ArrayBuffer.empty[Split]
      0.until(splitNum).foreach {
        i =>
          dataSplits += DataSplit
            .builder()
            .withSnapshot(1)
            .withBucket(0)
            .withPartition(BinaryRow.EMPTY_ROW)
            .withDataFiles(files.zipWithIndex.filter(_._2 % splitNum == i).map(_._1).toList.asJava)
            .rawConvertible(true)
            .withBucketPath("no use")
            .build()
      }

      val fakeScan = new FakeScan()
      val reshuffled = fakeScan.getInputPartitions(dataSplits.toArray)
      Assertions.assertTrue(reshuffled.length > 5)
    }
  }

  class FakeScan extends ScanHelper {
    override val coreOptions: CoreOptions =
      CoreOptions.fromMap(new HashMap[String, String]())
  }

}
