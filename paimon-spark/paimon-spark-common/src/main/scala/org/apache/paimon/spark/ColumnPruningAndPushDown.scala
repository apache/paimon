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

import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.Preconditions.checkState

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

trait ColumnPruningAndPushDown extends Scan with Logging {
  def table: Table
  def requiredSchema: StructType
  def filters: Seq[Predicate]
  def pushDownLimit: Option[Int] = None

  lazy val tableRowType: RowType = table.rowType
  lazy val tableSchema: StructType = SparkTypeUtils.fromPaimonRowType(tableRowType)

  final def partitionType: StructType = {
    SparkTypeUtils.toSparkPartitionType(table)
  }

  private[paimon] val (readTableRowType, metadataFields) = {
    checkState(
      requiredSchema.fields.forall(
        field =>
          tableRowType.containsField(field.name) ||
            PaimonMetadataColumn.SUPPORTED_METADATA_COLUMNS.contains(field.name)))
    val (_requiredTableFields, _metadataFields) =
      requiredSchema.fields.partition(field => tableRowType.containsField(field.name))
    val _readTableRowType =
      SparkTypeUtils.prunePaimonRowType(StructType(_requiredTableFields), tableRowType)
    (_readTableRowType, _metadataFields)
  }

  lazy val readBuilder: ReadBuilder = {
    val _readBuilder = table.newReadBuilder().withReadType(readTableRowType)
    if (filters.nonEmpty) {
      val pushedPredicate = PredicateBuilder.and(filters: _*)
      _readBuilder.withFilter(pushedPredicate)
    }
    pushDownLimit.foreach(_readBuilder.withLimit)
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
}
