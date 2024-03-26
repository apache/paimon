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
import org.apache.paimon.operation.FileHook
import org.apache.paimon.reader.RecordReader
import org.apache.paimon.spark.SparkUtils.createIOManager
import org.apache.paimon.table.source.{AbstractDataTableRead, ReadBuilder, Split}

import org.apache.spark.sql.Utils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.util.Objects

case class PaimonPartitionReaderFactory(readBuilder: ReadBuilder) extends PartitionReaderFactory {

  private lazy val ioManager: IOManager = createIOManager()

  private lazy val row: SparkInternalRow = new SparkInternalRow(readBuilder.readType())

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case paimonInputPartition: SparkInputPartition =>
        val tableRead = readBuilder.newRead().withIOManager(ioManager)
        tableRead match {
          case dataTableRead: AbstractDataTableRead[_] =>
            addFileHook(dataTableRead)
          case _ =>
        }
        val readFunc: Split => RecordReader[data.InternalRow] =
          (split: Split) => tableRead.createReader(split)
        PaimonPartitionReader(readFunc, paimonInputPartition, row)
      case _ =>
        throw new RuntimeException(s"It's not a Paimon input partition, $partition")
    }
  }

  private def addFileHook(tableRead: AbstractDataTableRead[_]): Unit = {
    tableRead.withFileHook(
      new FileHook(
        FileHook.ReaderTrigger.OPEN_FILE,
        (fileName: String) => Utils.setInputFileName(fileName)
      ))

    tableRead.withFileHook(
      new FileHook(
        FileHook.ReaderTrigger.CLOSE_FILE,
        (_: String) => Utils.unsetInputFileName()
      ))
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: PaimonPartitionReaderFactory =>
        this.readBuilder.equals(other.readBuilder)

      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(readBuilder)
  }
}
