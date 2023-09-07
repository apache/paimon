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

import org.apache.paimon.spark.{SparkInputPartition, SparkReaderFactory}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.ReadBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}

class PaimonMicroBatchStream(
    originTable: FileStoreTable,
    readBuilder: ReadBuilder,
    checkpointLocation: String)
  extends MicroBatchStream
  with StreamHelper
  with Logging {

  private var committedOffset: Option[PaimonSourceOffset] = None

  lazy val initOffset: PaimonSourceOffset = PaimonSourceOffset(getStartingContext)

  override def latestOffset(): Offset = {
    getLatestOffset
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = {
      val startOffset0 = PaimonSourceOffset.apply(start)
      if (startOffset0.compareTo(initOffset) < 0) {
        initOffset
      } else {
        startOffset0
      }
    }
    val endOffset = PaimonSourceOffset.apply(end)

    getBatch(startOffset, endOffset)
      .map(ids => new SparkInputPartition(ids.entry))
      .toArray[InputPartition]
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new SparkReaderFactory(readBuilder)
  }

  override def initialOffset(): Offset = {
    initOffset
  }

  override def deserializeOffset(json: String): Offset = {
    PaimonSourceOffset.apply(json)
  }

  override def commit(end: Offset): Unit = {
    committedOffset = Some(PaimonSourceOffset.apply(end))
    logInfo(s"$committedOffset is committed.")
  }

  override def stop(): Unit = {}

  override def table: FileStoreTable = originTable

}
