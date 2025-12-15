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

import org.apache.paimon.CoreOptions
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{Predicate, TopN}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.schema.PaimonMetadataColumn._
import org.apache.paimon.table.{SpecialFields, Table}
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.types.RowType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

trait ColumnPruningAndPushDown extends Scan with Logging {

  def table: Table

  // Column pruning
  def requiredSchema: StructType

  // Push down
  def pushedPartitionFilters: Seq[PartitionPredicate]
  def pushedDataFilters: Seq[Predicate]
  def pushedLimit: Option[Int] = None
  def pushedTopN: Option[TopN] = None

  val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  lazy val tableRowType: RowType = {
    if (coreOptions.rowTrackingEnabled()) {
      SpecialFields.rowTypeWithRowTracking(table.rowType())
    } else {
      table.rowType()
    }
  }

  lazy val tableSchema: StructType = SparkTypeUtils.fromPaimonRowType(tableRowType)

  final def partitionType: StructType = {
    SparkTypeUtils.toSparkPartitionType(table)
  }

  private[paimon] val (readTableRowType, metadataFields) = {
    requiredSchema.fields.foreach(f => checkMetadataColumn(f.name))
    val (_requiredTableFields, _metadataFields) =
      requiredSchema.fields.partition(field => tableRowType.containsField(field.name))
    val _readTableRowType =
      SparkTypeUtils.prunePaimonRowType(StructType(_requiredTableFields), tableRowType)
    (_readTableRowType, _metadataFields)
  }

  private def checkMetadataColumn(fieldName: String): Unit = {
    if (PATH_AND_INDEX_META_COLUMNS.contains(fieldName)) {
      if (!table.primaryKeys().isEmpty && !coreOptions.deletionVectorsEnabled()) {
        // Here we only issue a warning because after full compaction, primary-key tables can query the
        // index and file path too.
        logWarning(
          s"Only non-primary-key or deletion-vector or full compacted tables support metadata column: $fieldName")
      }
    }

    if (ROW_TRACKING_META_COLUMNS.contains(fieldName)) {
      if (!coreOptions.rowTrackingEnabled()) {
        throw new UnsupportedOperationException(
          s"Only row-tracking tables support metadata column: $fieldName")
      }
    }
  }

  lazy val readBuilder: ReadBuilder = {
    val _readBuilder = table.newReadBuilder().withReadType(readTableRowType)
    if (pushedPartitionFilters.nonEmpty) {
      // todo: remove this, when impl withPartitionFilter(List<PartitionPredicate> partitionPredicate)
      assert(pushedPartitionFilters.size == 1)
      _readBuilder.withPartitionFilter(pushedPartitionFilters.head)
    }
    if (pushedDataFilters.nonEmpty) {
      _readBuilder.withFilter(pushedDataFilters.asJava)
    }
    pushedLimit.foreach(_readBuilder.withLimit)
    pushedTopN.foreach(_readBuilder.withTopN)
    _readBuilder.dropStats()
  }

  final def metadataColumns: Seq[PaimonMetadataColumn] = {
    metadataFields.map(field => PaimonMetadataColumn.get(field.name, partitionType))
  }

  override def readSchema(): StructType = {
    val _readSchema = StructType(
      SparkTypeUtils.fromPaimonRowType(readTableRowType).fields ++ metadataFields)
    if (!_readSchema.equals(requiredSchema)) {
      logInfo(
        s"Actual readSchema: ${_readSchema} is not equal to spark pushed requiredSchema: $requiredSchema")
    }
    _readSchema
  }

  override def description(): String = {
    val pushedPartitionFiltersStr = if (pushedPartitionFilters.nonEmpty) {
      ", PartitionFilters: [" + pushedPartitionFilters.mkString(",") + "]"
    } else {
      ""
    }
    val pushedDataFiltersStr = if (pushedDataFilters.nonEmpty) {
      ", DataFilters: [" + pushedDataFilters.mkString(",") + "]"
    } else {
      ""
    }
    s"${getClass.getSimpleName}: [${table.name}]" +
      pushedPartitionFiltersStr +
      pushedDataFiltersStr +
      pushedTopN.map(topN => s", TopN: [$topN]").getOrElse("") +
      pushedLimit.map(limit => s", Limit: [$limit]").getOrElse("")
  }
}
