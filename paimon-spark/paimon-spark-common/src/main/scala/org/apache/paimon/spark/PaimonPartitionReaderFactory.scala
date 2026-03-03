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

import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.{DataSplit, ReadBuilder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.Objects

import scala.collection.JavaConverters._

case class PaimonPartitionReaderFactory(
    readBuilder: ReadBuilder,
    table: Table,
    metadataColumns: Seq[PaimonMetadataColumn] = Seq.empty,
    blobAsDescriptor: Boolean)
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case paimonInputPartition: PaimonInputPartition =>
        PaimonPartitionReader(readBuilder, paimonInputPartition, metadataColumns, blobAsDescriptor)
      case _ =>
        throw new RuntimeException(s"It's not a Paimon input partition, $partition")
    }
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    val options = table.options()
    if (
      options.containsKey("comet.enabled") && java.lang.Boolean.parseBoolean(
        options.get("comet.enabled"))
    ) {
      partition match {
        case p: PaimonInputPartition =>
          // Ensure all splits are DataSplit (no system splits)
          p.splits.forall(_.isInstanceOf[DataSplit])
        case _ => false
      }
    } else {
      false
    }
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val dataFields = new java.util.ArrayList(readBuilder.readType().getFields)
    dataFields.addAll(metadataColumns.map(_.toPaimonDataField).asJava)
    val rowType = new org.apache.paimon.types.RowType(dataFields)
    val schema = org.apache.paimon.spark.SparkTypeUtils.fromPaimonRowType(rowType)

    new org.apache.paimon.spark.comet.PaimonCometPartitionReader(
      table.fileIO(),
      partition.asInstanceOf[PaimonInputPartition],
      schema,
      table)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: PaimonPartitionReaderFactory =>
        this.readBuilder.equals(other.readBuilder) && this.metadataColumns == other.metadataColumns

      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(readBuilder)
  }
}
