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

package org.apache.spark.sql.execution

import org.apache.paimon.partition.PartitionStatistics
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.spark.utils.CatalogUtils.{checkNamespace, toIdentifier}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogStorageFormat, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsMetadataColumns, TableCatalog}
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PaimonDescribeTableExec(
    output: Seq[Attribute],
    catalog: SparkBaseCatalog,
    identifier: Identifier,
    table: SparkTable,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows)
    addPartitioning(rows)

    if (partitionSpec.nonEmpty) {
      describeDetailedPartitionInfo(rows)
    }

    if (isExtended) {
      addMetadataColumns(rows)
      addTableDetails(rows)
    }

    rows
  }

  private def addTableDetails(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Table Information", "", "")
    rows += toCatalystRow("Name", table.name(), "")

    val tableType = if (table.properties().containsKey(TableCatalog.PROP_EXTERNAL)) {
      CatalogTableType.EXTERNAL.name
    } else {
      CatalogTableType.MANAGED.name
    }
    rows += toCatalystRow("Type", tableType, "")
    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(
        propKey => {
          if (table.properties.containsKey(propKey)) {
            rows += toCatalystRow(propKey.capitalize, table.properties.get(propKey), "")
          }
        })
    val properties =
      conf
        .redactOptions(table.properties.asScala.toMap)
        .toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1)
        .map { case (key, value) => key + "=" + value }
        .mkString("[", ",", "]")
    rows += toCatalystRow("Table Properties", properties, "")

    // If any columns have default values, append them to the result.
    ResolveDefaultColumns.getDescribeMetadata(table.schema).foreach {
      row => rows += toCatalystRow(row._1, row._2, row._3)
    }

    addNotNullMetadata(table.schema, rows)
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.columns().map {
      column =>
        toCatalystRow(
          column.name,
          column.dataType.simpleString,
          Option(column.comment()).getOrElse(""))
    }
  }

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case hasMeta: SupportsMetadataColumns if hasMeta.metadataColumns.nonEmpty =>
      rows += emptyRow()
      rows += toCatalystRow("# Metadata Columns", "", "")
      rows ++= hasMeta.metadataColumns.map {
        column =>
          toCatalystRow(
            column.name,
            column.dataType.simpleString,
            Option(column.comment()).getOrElse(""))
      }
    case _ =>
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    if (table.partitioning.nonEmpty) {
      val partitionColumnsOnly = table.partitioning.forall(t => t.isInstanceOf[IdentityTransform])
      if (partitionColumnsOnly) {
        rows += toCatalystRow("# Partition Information", "", "")
        rows += toCatalystRow(s"# ${output(0).name}", output(1).name, output(2).name)
        rows ++= table.partitioning
          .map(_.asInstanceOf[IdentityTransform].ref.fieldNames())
          .map {
            fieldNames =>
              val nestedField = table.schema.findNestedField(fieldNames)
              assert(
                nestedField.isDefined,
                s"Not found the partition column ${fieldNames.map(quoteIfNeeded).mkString(".")} " +
                  s"in the table schema ${table.schema.catalogString}."
              )
              nestedField.get
          }
          .map {
            case (path, field) =>
              toCatalystRow(
                (path :+ field.name).map(quoteIfNeeded).mkString("."),
                field.dataType.simpleString,
                field.getComment().getOrElse(""))
          }
      } else {
        rows += emptyRow()
        rows += toCatalystRow("# Partitioning", "", "")
        rows ++= table.partitioning.zipWithIndex.map {
          case (transform, index) => toCatalystRow(s"Part $index", transform.describe(), "")
        }
      }
    }
  }

  private def describeDetailedPartitionInfo(rows: ArrayBuffer[InternalRow]): Unit = {
    if (
      table.partitionSchema.fieldNames.length != partitionSpec.size
      || partitionSpec.keys.exists(!table.partitionSchema.fieldNames.contains(_))
    ) {
      throw new AnalysisException(
        "DESCRIBE partition must specify all the correct partition columns")
    }

    if (isExtended) {
      describeFormattedDetailedPartitionInfo(rows)
    }
  }

  private def describeFormattedDetailedPartitionInfo(rows: ArrayBuffer[InternalRow]): Unit = {
    checkNamespace(identifier.namespace())
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Partition Information", "", "")
    rows += toCatalystRow("Database", identifier.namespace().head, "")
    rows += toCatalystRow("Table", identifier.name(), "")
    val partition = catalog
      .paimonCatalog()
      .listPartitions(toIdentifier(identifier))
      .asScala
      .filter(_.spec().asScala == partitionSpec)
    if (partition.size != 1) {
      throw new IllegalArgumentException(
        s"Found ${partition.size} matching partitions. " +
          s"Expected exactly one partition to match the partition spec.")
    }
    val dummyStorageFormat =
      CatalogStorageFormat(None, None, None, None, compressed = false, Map.empty)
    val partParameters: Map[String, String] = Map(
      PartitionStatistics.FIELD_FILE_COUNT -> partition.head.fileCount().toString,
      PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES -> partition.head.fileSizeInBytes().toString,
      PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME -> partition.head
        .lastFileCreationTime()
        .toString,
      PartitionStatistics.FIELD_RECORD_COUNT -> partition.head.recordCount().toString
    )
    val partStats =
      CatalogStatistics(partition.head.fileSizeInBytes(), Some(partition.head.recordCount()))
    CatalogTablePartition(
      partitionSpec,
      dummyStorageFormat,
      partParameters,
      partition.head.lastFileCreationTime(),
      -1,
      Some(partStats)).toLinkedHashMap.foreach(s => rows += toCatalystRow(s._1, s._2, ""))
    rows += emptyRow()
  }

  private def addNotNullMetadata(schema: StructType, rows: ArrayBuffer[InternalRow]): Unit = {
    if (schema.fields.exists(!_.nullable)) {
      rows += emptyRow()
      rows += toCatalystRow("# Column Not Null", "", "")
      schema.fields.filter(!_.nullable).foreach {
        column => rows += toCatalystRow(column.name, column.dataType.simpleString, "NOT NULL")
      }
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")
}
