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

import org.apache.paimon.data
import org.apache.paimon.disk.IOManager
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.source.{ReadBuilder, Split}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.util.{ArrayList => JList}
import java.util.Objects

import scala.collection.JavaConverters._

case class PaimonPartitionReaderFactory(
    readBuilder: ReadBuilder,
    metadataColumns: Seq[PaimonMetadataColumn] = Seq.empty)
  extends PartitionReaderFactory {

  private lazy val ioManager: IOManager = createIOManager()

  private lazy val row: SparkInternalRow = {
    val dataFields = new JList(readBuilder.readType().getFields)
    dataFields.addAll(metadataColumns.map(_.toPaimonDataField).asJava)
    val rowType = new RowType(dataFields)
    new SparkInternalRow(rowType)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case paimonInputPartition: PaimonInputPartition =>
        val readFunc: Split => RecordReader[data.InternalRow] =
          (split: Split) => readBuilder.newRead().withIOManager(ioManager).createReader(split)
        PaimonPartitionReader(readFunc, paimonInputPartition, row, metadataColumns)
      case _ =>
        throw new RuntimeException(s"It's not a Paimon input partition, $partition")
    }
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
