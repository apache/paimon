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

import org.apache.paimon.data.variant.Variant
import org.apache.paimon.spark.catalyst.analysis.Spark3ResolutionRules
import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark3SqlExtensionsParser
import org.apache.paimon.spark.data.{Spark3ArrayData, Spark3InternalRow, Spark3InternalRowWithBlob, SparkArrayData, SparkInternalRow}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

class Spark3Shim extends SparkShim {

  override def classicApi: ClassicApi = new Classic3Api

  override def createSparkParser(delegate: ParserInterface): ParserInterface = {
    new PaimonSpark3SqlExtensionsParser(delegate)
  }

  override def createCustomResolution(spark: SparkSession): Rule[LogicalPlan] = {
    Spark3ResolutionRules(spark)
  }

  override def createSparkInternalRow(rowType: RowType): SparkInternalRow = {
    new Spark3InternalRow(rowType)
  }

  override def createSparkInternalRowWithBlob(
      rowType: RowType,
      blobFields: Set[Int],
      blobAsDescriptor: Boolean): SparkInternalRow = {
    new Spark3InternalRowWithBlob(rowType, blobFields, blobAsDescriptor)
  }

  override def createSparkArrayData(elementType: DataType): SparkArrayData = {
    new Spark3ArrayData(elementType)
  }

  override def createTable(
      tableCatalog: TableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table = {
    tableCatalog.createTable(ident, schema, partitions, properties)
  }

  override def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean): CTERelationRef =
    MinorVersionShim.createCTERelationRef(cteId, resolved, output, isStreaming)

  override def supportsHashAggregate(
      aggregateBufferAttributes: Seq[Attribute],
      groupingExpression: Seq[Expression]): Boolean =
    Aggregate.supportsHashAggregate(aggregateBufferAttributes)

  override def supportsObjectHashAggregate(
      aggregateExpressions: Seq[AggregateExpression],
      groupByExpressions: Seq[Expression]): Boolean =
    Aggregate.supportsObjectHashAggregate(aggregateExpressions)

  override def createMergeIntoTable(
      targetTable: LogicalPlan,
      sourceTable: LogicalPlan,
      mergeCondition: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction],
      withSchemaEvolution: Boolean): MergeIntoTable = {
    MinorVersionShim.createMergeIntoTable(
      targetTable,
      sourceTable,
      mergeCondition,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions)
  }

  override def toPaimonVariant(o: Object): Variant = throw new UnsupportedOperationException()

  override def isSparkVariantType(dataType: org.apache.spark.sql.types.DataType): Boolean = false

  override def SparkVariantType(): org.apache.spark.sql.types.DataType =
    throw new UnsupportedOperationException()

  override def toPaimonVariant(row: InternalRow, pos: Int): Variant =
    throw new UnsupportedOperationException()

  override def toPaimonVariant(array: ArrayData, pos: Int): Variant =
    throw new UnsupportedOperationException()
}
