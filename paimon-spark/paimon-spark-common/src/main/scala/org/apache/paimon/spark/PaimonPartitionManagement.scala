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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.hive.HiveCatalog
import org.apache.paimon.metastore.MetastoreClient
import org.apache.paimon.operation.FileStoreCommit
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.BatchWriteBuilder
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{InternalRowPartitionComputer, TypeUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsAtomicPartitionManagement
import org.apache.spark.sql.types.StructType

import java.lang.reflect.Method
import java.util
import java.util.{Objects, UUID, Map => JMap}
import scala.collection.JavaConverters._

trait PaimonPartitionManagement extends SupportsAtomicPartitionManagement {
  self: SparkTable =>

  private lazy val partitionRowType: RowType = TypeUtils.project(table.rowType, table.partitionKeys)

  override lazy val partitionSchema: StructType = SparkTypeUtils.fromPaimonRowType(partitionRowType)

  override def dropPartitions(internalRows: Array[InternalRow]): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val rowConverter = CatalystTypeConverters
          .createToScalaConverter(CharVarcharUtils.replaceCharVarcharWithString(partitionSchema))
        val rowDataPartitionComputer = new InternalRowPartitionComputer(
          fileStoreTable.coreOptions().partitionDefaultName(),
          partitionRowType,
          table.partitionKeys().asScala.toArray)

        val partitions = internalRows.map {
          r =>
            rowDataPartitionComputer
              .generatePartValues(new SparkRow(partitionRowType, rowConverter(r).asInstanceOf[Row]))
              .asInstanceOf[JMap[String, String]]
        }
        val commit: FileStoreCommit = fileStoreTable.store.newCommit(UUID.randomUUID.toString)
        try {
          commit.dropPartitions(partitions.toSeq.asJava, BatchWriteBuilder.COMMIT_IDENTIFIER)
        } finally {
          commit.close()
        }
        true

      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports drop partitions.")
    }
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    throw new UnsupportedOperationException("Load partition is not supported")
  }

  override def listPartitionIdentifiers(
      partitionCols: Array[String],
      internalRow: InternalRow): Array[InternalRow] = {
    assert(
      partitionCols.length == internalRow.numFields,
      s"Number of partition names (${partitionCols.length}) must be equal to " +
        s"the number of partition values (${internalRow.numFields})."
    )
    assert(
      partitionCols.forall(fieldName => partitionSchema.fieldNames.contains(fieldName)),
      s"Some partition names ${partitionCols.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${partitionSchema.sql}'."
    )
    table.newReadBuilder.newScan.listPartitions.asScala
      .map(binaryRow => SparkInternalRow.fromPaimon(binaryRow, partitionRowType))
      .filter(
        sparkInternalRow => {
          partitionCols.zipWithIndex
            .map {
              case (partitionName, index) =>
                val internalRowIndex = partitionSchema.fieldIndex(partitionName)
                val structField = partitionSchema.fields(internalRowIndex)
                Objects.equals(
                  sparkInternalRow.get(internalRowIndex, structField.dataType),
                  internalRow.get(index, structField.dataType))
            }
            .forall(identity)
        })
      .toArray
  }

  override def createPartitions(
      internalRows: Array[InternalRow],
      maps: Array[JMap[String, String]]): Unit = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val rowConverter = CatalystTypeConverters
          .createToScalaConverter(CharVarcharUtils.replaceCharVarcharWithString(partitionSchema))
        val rowDataPartitionComputer = new InternalRowPartitionComputer(
          fileStoreTable.coreOptions().partitionDefaultName(),
          partitionRowType,
          table.partitionKeys().asScala.toArray)
        val partitions = internalRows.map {
          r =>
            rowDataPartitionComputer
              .generatePartValues(new SparkRow(partitionRowType, rowConverter(r).asInstanceOf[Row]))
              .asInstanceOf[JMap[String, String]]
        }
        val metastoreClient: MetastoreClient = fileStoreTable.catalogEnvironment().metastoreClientFactory().create
        partitions.map {
          partition =>
            metastoreClient.addPartition(new util.LinkedHashMap[String, String](partition))
//            val func = catalog.getClass.getMethod(
//              "createPartition",
//              classOf[Identifier],
//              classOf[JMap[_, _]])
//            func.invoke(catalog, getIdentifierFromTableName(table.fullName()), partition)
        }
      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports create partitions.")
    }
  }

  def getIdentifierFromTableName(tableName: String): Identifier = {
    val name: Array[String] = tableName.split("\\.")
    new Identifier(name.apply(0), name.apply(1))
  }
}
