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
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.spark.util.PartitionStatisticsDisplay
import org.apache.paimon.spark.utils.CatalogUtils.checkNamespace
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, CatalogStorageFormat, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DescribeTableExec
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

case class PaimonDescribeTableExec(
    output: Seq[Attribute],
    catalog: SparkBaseCatalog,
    identifier: Identifier,
    table: SparkTable,
    partitionSpec: Option[ResolvedPartitionSpec],
    isExtended: Boolean)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val constructor = classOf[DescribeTableExec].getConstructors.head
    val args: Seq[AnyRef] = if (constructor.getParameterTypes.length == 3) {
      Seq(output, table, java.lang.Boolean.valueOf(isExtended))
    } else {
      Seq(output, catalog.name(), identifier, table, java.lang.Boolean.valueOf(isExtended))
    }
    val description = constructor.newInstance(args: _*).asInstanceOf[DescribeTableExec]
    val rows = ArrayBuffer.empty ++= description.executeCollect()

    partitionSpec.foreach {
      spec =>
        val metadata = loadPartitionMetadata(spec)
        if (isExtended) {
          describeFormattedDetailedPartitionInfo(spec, metadata, rows)
        }
    }

    if (isExtended) {
      addNotNullMetadata(table.schema, rows)
    }

    rows.toSeq
  }

  /**
   * The partition's statistics, found by value. A FileStoreTable reports them for every partition
   * it holds. Another table, e.g. `t$audit_log`, reports none, so its partition must be one the
   * table lists.
   */
  private def loadPartitionMetadata(spec: ResolvedPartitionSpec): Map[String, String] = {
    val metadata = table.loadPartitionMetadata(spec.ident).asScala.toMap
    if (
      metadata.isEmpty && (table.table.isInstanceOf[FileStoreTable] ||
        table.listPartitionIdentifiers(spec.names.toArray, spec.ident).isEmpty)
    ) {
      throw new NoSuchPartitionException(identifier.toString, spec.ident, table.partitionSchema)
    }
    metadata
  }

  private def describeFormattedDetailedPartitionInfo(
      spec: ResolvedPartitionSpec,
      metadata: Map[String, String],
      rows: ArrayBuffer[InternalRow]): Unit = {
    checkNamespace(identifier.namespace(), catalog.name())
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Partition Information", "", "")
    rows += toCatalystRow("Database", identifier.namespace().head, "")
    rows += toCatalystRow("Table", identifier.name(), "")
    val dummyStorageFormat =
      CatalogStorageFormat(None, None, None, None, compressed = false, Map.empty)
    // Include only reported values. Spark omits the "Partition Parameters" row for an empty map.
    val partParameters = ListMap(
      metadata.toSeq
        .filterNot { case (field, value) => PartitionStatisticsDisplay.isUnreported(field, value) }
        .sortBy(_._1): _*)
    // CatalogTablePartition uses this temporary value only to render its "Partition Statistics"
    // row. It cannot represent an unknown size, so omit the row when size is unknown and omit an
    // unknown row count independently.
    val rowCount = known(metadata, PartitionStatistics.FIELD_RECORD_COUNT).map(BigInt(_))
    val partStats = known(metadata, PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES).map {
      size => CatalogStatistics(size, rowCount)
    }
    val createTime = known(metadata, PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME)
    val partitionDetails = CatalogTablePartition(
      partitionValues(spec),
      dummyStorageFormat,
      partParameters,
      createTime.getOrElse(PartitionStatistics.UNKNOWN),
      -1,
      partStats).toLinkedHashMap
    // Spark formats every createTime as a date; replace an unknown timestamp to avoid a 1969 date.
    if (createTime.isEmpty) {
      partitionDetails.put(
        PaimonDescribeTableExec.CREATED_TIME_KEY,
        PartitionStatisticsDisplay.UNKNOWN)
    }
    partitionDetails.foreach(s => rows += toCatalystRow(s._1, s._2, ""))
    rows += emptyRow()
  }

  private def known(metadata: Map[String, String], field: String): Option[Long] =
    PartitionStatisticsDisplay.known(metadata, field)

  /** The partition's values as strings, in partition column order; NULL renders as "null". */
  private def partitionValues(spec: ResolvedPartitionSpec): TablePartitionSpec = {
    val timeZoneId = Some(conf.sessionLocalTimeZone)
    val values = table.partitionSchema.fields.toSeq.zipWithIndex.map {
      case (field, i) =>
        val dataType = CharVarcharUtils.replaceCharVarcharWithString(field.dataType)
        val value = Compatibility
          .cast(Literal(spec.ident.get(i, dataType), dataType), StringType, timeZoneId)
          .eval()
        field.name -> (if (value == null) "null" else value.toString)
    }
    ListMap(values: _*)
  }

  private def addNotNullMetadata(schema: StructType, rows: ArrayBuffer[InternalRow]): Unit = {
    if (schema.fields.exists(!_.nullable)) {
      rows += toCatalystRow("# Column Not Null", "", "")
      schema.fields.filter(!_.nullable).foreach {
        column => rows += toCatalystRow(column.name, column.dataType.simpleString, "NOT NULL")
      }
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")
}

object PaimonDescribeTableExec {
  // Key for the partition creation-time row emitted by CatalogTablePartition.toLinkedHashMap.
  val CREATED_TIME_KEY = "Created Time"

  // This column metadata indicates the default value associated with a particular table column that
  // is in effect at any given time. Its value begins at the time of the initial CREATE/REPLACE
  // TABLE statement with DEFAULT column definition(s), if any. It then changes whenever an ALTER
  // TABLE statement SETs the DEFAULT. The intent is for this "current default" to be used by
  // UPDATE, INSERT and MERGE, which evaluate each default expression for each row.
  val CURRENT_DEFAULT_COLUMN_METADATA_KEY = "CURRENT_DEFAULT"
}
