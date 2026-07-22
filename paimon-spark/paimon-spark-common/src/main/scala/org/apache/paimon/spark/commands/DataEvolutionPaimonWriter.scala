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
import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.spark.write.{DataEvolutionTableDataWrite, WriteHelper, WriteTaskResult}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink._
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.BlobType
import org.apache.paimon.types.VectorType.isVectorStoreFile
import org.apache.paimon.utils.SerializationUtils

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

case class DataEvolutionPaimonWriter(paimonTable: FileStoreTable, dataSplits: Seq[DataSplit])
  extends WriteHelper {

  // File rolling will never be performed
  override val table: FileStoreTable =
    paimonTable.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G"))

  def writePartialFields(
      data: DataFrame,
      columnNames: Seq[String],
      rawBlobPlaceholderMarkerColumns: Map[String, String] = Map.empty): Seq[CommitMessage] = {
    val sparkSession = data.sparkSession
    val uriReaderFactory = uriReaderFactoryForBlobDescriptor
    import sparkSession.implicits._
    assert(data.columns.length == columnNames.size + 2 + rawBlobPlaceholderMarkerColumns.size)
    val writeType = table.rowType().project(columnNames.asJava)

    val options = new CoreOptions(table.schema().options())
    val blobInlineFields = options.blobInlineField().asScala.toSeq
    // Maps from blob field index to corresponding marker column index
    val rawBlobPlaceholderMarkerIndexes = writeType.getFields.asScala.flatMap {
      field =>
        if (
          BlobType.isBlobFileField(field.`type`()) &&
          !blobInlineFields.exists(inlineField => resolver(inlineField, field.name()))
        ) {
          val markerColumn = rawBlobPlaceholderMarkerColumns.getOrElse(
            field.name(),
            throw new UnsupportedOperationException(
              "DataEvolution raw-data BLOB partial writes require an internal placeholder marker " +
                s"for column ${field.name()}.")
          )
          Some(writeType.getFieldIndex(field.name()) -> data.schema.fieldIndex(markerColumn))
        } else {
          None
        }
    }.toMap
    val unusedMarkerColumns =
      rawBlobPlaceholderMarkerColumns.keySet -- rawBlobPlaceholderMarkerIndexes.keys.map(
        index => writeType.getFields.get(index).name())
    if (unusedMarkerColumns.nonEmpty) {
      throw new IllegalArgumentException(
        "Raw BLOB placeholder markers do not match partial write columns: " +
          unusedMarkerColumns.toSeq.sorted.mkString(", "))
    }

    val firstRowIdToPartitionMap = new mutable.HashMap[Long, (Array[Byte], Long)]
    dataSplits.foreach(
      split =>
        split
          .dataFiles()
          .asScala
          .filter(file => !isBlobFile(file.fileName()) && !isVectorStoreFile(file.fileName()))
          .foreach(
            file =>
              firstRowIdToPartitionMap
                .put(
                  file.firstRowId(),
                  // BinaryRow stores data in transient memory segments and relies on Java
                  // serialization hooks to restore them. Store bytes in Spark closures and
                  // broadcasts so Kryo does not serialize BinaryRow internals directly.
                  (SerializationUtils.serializeBinaryRow(split.partition()), file.rowCount())
                )))
    val firstRowIdToPartitionMapBroadcast =
      sparkSession.sparkContext.broadcast(firstRowIdToPartitionMap)
    val writeBuilder = table.newBatchWriteBuilder()

    val written =
      data.mapPartitions {
        iter =>
          {
            val write = DataEvolutionTableDataWrite(
              writeBuilder,
              writeType,
              firstRowIdToPartitionMapBroadcast.value,
              uriReaderFactory,
              rawBlobPlaceholderMarkerIndexes)
            try {
              iter.foreach(row => write.write(row))
              Iterator.apply(write.commit)
            } finally {
              write.close()
            }
          }
      }
    WriteTaskResult.merge(written.collect())
  }
}
