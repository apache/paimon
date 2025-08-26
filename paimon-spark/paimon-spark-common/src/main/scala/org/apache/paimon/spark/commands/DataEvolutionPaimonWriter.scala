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

import org.apache.paimon.CoreOptions
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.spark.DataEvolutionSparkTableWrite
import org.apache.paimon.spark.commands.DataEvolutionPaimonWriter.{deserializeCommitMessage, dynamicOp}
import org.apache.paimon.spark.write.WriteHelper
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink._

import org.apache.spark.sql._

import java.io.IOException
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

case class DataEvolutionPaimonWriter(paimonTable: FileStoreTable) extends WriteHelper {

  private lazy val firstRowIdToPartitionMap: mutable.HashMap[Long, Tuple2[BinaryRow, Long]] =
    initPartitionMap()
  override val table: FileStoreTable = paimonTable.copy(dynamicOp)

  @transient private lazy val serializer = new CommitMessageSerializer

  private def initPartitionMap(): mutable.HashMap[Long, Tuple2[BinaryRow, Long]] = {
    val firstRowIdToPartitionMap = new mutable.HashMap[Long, Tuple2[BinaryRow, Long]]
    table
      .store()
      .newScan()
      .readFileIterator()
      .forEachRemaining(
        k =>
          firstRowIdToPartitionMap
            .put(k.file().firstRowId(), Tuple2.apply(k.partition(), k.file().rowCount())))
    firstRowIdToPartitionMap
  }

  def writePartialFields(data: DataFrame, columnNames: Seq[String]): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    import sparkSession.implicits._
    assert(data.columns.length == columnNames.size + 2)
    val writeType = table.rowType().project(columnNames.asJava)

    val written =
      data.mapPartitions {
        iter =>
          {
            val write = DataEvolutionSparkTableWrite(
              table.newBatchWriteBuilder(),
              writeType,
              firstRowIdToPartitionMap)
            try {
              iter.foreach(row => write.write(row))
              write.finish()
            } finally {
              write.close()
            }
          }
      }

    written
      .collect()
      .map(deserializeCommitMessage(serializer, _))
      .toSeq
  }
}

object DataEvolutionPaimonWriter {
  final private val dynamicOp =
    Collections.singletonMap(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G")
  def apply(table: FileStoreTable): DataEvolutionPaimonWriter = {
    new DataEvolutionPaimonWriter(table)
  }

  private def deserializeCommitMessage(
      serializer: CommitMessageSerializer,
      bytes: Array[Byte]): CommitMessage = {
    try {
      serializer.deserialize(serializer.getVersion, bytes)
    } catch {
      case e: IOException =>
        throw new RuntimeException("Failed to deserialize CommitMessage's object", e)
    }
  }
}
