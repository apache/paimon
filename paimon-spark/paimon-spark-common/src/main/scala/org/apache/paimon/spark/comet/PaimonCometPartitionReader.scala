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

package org.apache.paimon.spark.comet

import org.apache.paimon.fs.FileIO
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonInputPartition
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.DataSplit

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.IOException

import scala.collection.JavaConverters._

class PaimonCometPartitionReader(
    fileIO: FileIO,
    partition: PaimonInputPartition,
    schema: StructType,
    table: Table)
  extends PartitionReader[ColumnarBatch] {

  private val files: Iterator[(String, Long)] = partition.splits.iterator.flatMap {
    case dataSplit: DataSplit =>
      dataSplit.dataFiles().asScala.iterator.map {
        f =>
          if (table.isInstanceOf[FileStoreTable]) {
            val fst = table.asInstanceOf[FileStoreTable]
            val pathFactory = fst.store().pathFactory()
            val bucketPath = pathFactory.bucketPath(dataSplit.partition(), dataSplit.bucket())
            val fullPath = new Path(bucketPath, f.fileName()).toString
            (fullPath, f.fileSize())
          } else {
            throw new UnsupportedOperationException("Only FileStoreTable supports Comet reader")
          }
      }
    case _ => Iterator.empty
  }

  private var currentParquetReader: ParquetFileReader = _
  private var currentCometReader: CometColumnarBatchReader = _
  private var currentBatch: ColumnarBatch = _
  private var rowsInCurrentGroup: Long = 0
  private var rowsReadInCurrentGroup: Long = 0

  override def next(): Boolean = {
    // The currentBatch is reused by CometColumnarBatchReader, so we don't need to close it here.
    // Spark expects the batch returned by get() to be valid until next() is called.
    // We update the batch content in advance() call.

    try {
      advance()
    } catch {
      case e: IOException => throw new RuntimeException(e)
    }
  }

  private def advance(): Boolean = {
    // 1. Try to read from current row group
    if (currentCometReader != null && rowsReadInCurrentGroup < rowsInCurrentGroup) {
      val needed = Math.min(4096, rowsInCurrentGroup - rowsReadInCurrentGroup).toInt
      currentBatch = currentCometReader.read(needed)
      rowsReadInCurrentGroup += currentBatch.numRows()
      return true
    }

    // 2. Current row group finished, try next row group
    if (currentParquetReader != null) {
      val pages = currentParquetReader.readNextRowGroup()
      if (pages != null) {
        rowsInCurrentGroup = pages.getRowCount
        rowsReadInCurrentGroup = 0
        currentCometReader.setRowGroupInfo(pages)
        return advance() // Recurse to read batch
      } else {
        // File finished
        currentParquetReader.close()
        currentParquetReader = null
        if (currentCometReader != null) {
          currentCometReader.close()
          currentCometReader = null
        }
      }
    }

    // 3. Next file
    if (files.hasNext) {
      val (file, length) = files.next()
      val inputFile = new SparkParquetInputFile(fileIO, new Path(file), length)
      currentParquetReader = ParquetFileReader.open(inputFile)
      val footer = currentParquetReader.getFooter
      val messageType = footer.getFileMetaData.getSchema

      currentCometReader = PaimonCometReaderBuilder.build(schema, messageType)
      // Recurse to read first row group
      return advance()
    }

    false
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    if (currentBatch != null) {
      currentBatch.close()
    }
    if (currentCometReader != null) {
      currentCometReader.close()
    }
    if (currentParquetReader != null) {
      currentParquetReader.close()
    }
  }
}
