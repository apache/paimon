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

package org.apache.spark.sql.paimon.shims

import org.apache.paimon.data.variant.{GenericVariant, Variant}
import org.apache.paimon.spark.catalyst.analysis.Spark4ResolutionRules
import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark4SqlExtensionsParser
import org.apache.paimon.spark.data.{Spark4ArrayData, Spark4InternalRow, Spark4InternalRowWithBlob, SparkArrayData, SparkInternalRow}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationRef, LogicalPlan, MergeAction, MergeIntoTable}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Instruction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.{DataTypes, StructType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.VariantVal

import java.util.{Map => JMap}

class Spark4Shim extends SparkShim {

  override def classicApi: ClassicApi = new Classic4Api

  override def createSparkParser(delegate: ParserInterface): ParserInterface = {
    new PaimonSpark4SqlExtensionsParser(delegate)
  }

  override def createCustomResolution(spark: SparkSession): Rule[LogicalPlan] = {
    Spark4ResolutionRules(spark)
  }

  override def createSparkInternalRow(rowType: RowType): SparkInternalRow = {
    new Spark4InternalRow(rowType)
  }

  override def createSparkInternalRowWithBlob(
      rowType: RowType,
      blobFieldIndex: Int,
      blobAsDescriptor: Boolean): SparkInternalRow = {
    new Spark4InternalRowWithBlob(rowType, blobFieldIndex, blobAsDescriptor)
  }

  override def createSparkArrayData(elementType: DataType): SparkArrayData = {
    new Spark4ArrayData(elementType)
  }

  override def createTable(
      tableCatalog: TableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table = {
    val columns = CatalogV2Util.structTypeToV2Columns(schema)
    tableCatalog.createTable(ident, columns, partitions, properties)
  }

  override def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean): CTERelationRef = {
    CTERelationRef(cteId, resolved, output.toSeq, isStreaming)
  }

  override def supportsHashAggregate(
      aggregateBufferAttributes: Seq[Attribute],
      groupingExpression: Seq[Expression]): Boolean = {
    Aggregate.supportsHashAggregate(aggregateBufferAttributes.toSeq, groupingExpression.toSeq)
  }

  override def supportsObjectHashAggregate(
      aggregateExpressions: Seq[AggregateExpression],
      groupByExpressions: Seq[Expression]): Boolean =
    Aggregate.supportsObjectHashAggregate(aggregateExpressions.toSeq, groupByExpressions.toSeq)

  override def createMergeIntoTable(
      targetTable: LogicalPlan,
      sourceTable: LogicalPlan,
      mergeCondition: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction],
      withSchemaEvolution: Boolean): MergeIntoTable = {
    MergeIntoTable(
      targetTable,
      sourceTable,
      mergeCondition,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions,
      withSchemaEvolution)
  }

  override def createKeep(
      context: String,
      condition: Expression,
      output: Seq[Expression]): Instruction = {
    MinorVersionShim.createKeep(context, condition, output)
  }

  override def toPaimonVariant(o: Object): Variant = {
    val v = o.asInstanceOf[VariantVal]
    new GenericVariant(v.getValue, v.getMetadata)
  }

  override def toPaimonVariant(row: InternalRow, pos: Int): Variant = {
    val v = row.getVariant(pos)
    new GenericVariant(v.getValue, v.getMetadata)
  }

  override def toPaimonVariant(array: ArrayData, pos: Int): Variant = {
    val v = array.getVariant(pos)
    new GenericVariant(v.getValue, v.getMetadata)
  }

  override def isSparkVariantType(dataType: org.apache.spark.sql.types.DataType): Boolean =
    dataType.isInstanceOf[VariantType]

  override def SparkVariantType(): org.apache.spark.sql.types.DataType = DataTypes.VariantType

  override def createFileIndex(
      options: CaseInsensitiveStringMap,
      sparkSession: SparkSession,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType): PartitioningAwareFileIndex = {
    MinorVersionShim.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema)
  }
}
