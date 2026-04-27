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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{CTESubstitution, SubstituteUnresolvedOrdinals}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationRef, LogicalPlan, MergeAction, MergeIntoTable, MergeRows, SubqueryAlias, TableSpec, UnresolvedWith}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Keep
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SparkFormatTable
import org.apache.spark.sql.execution.datasources.{PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

import java.util.{Map => JMap}

/**
 * Spark 4.0-compatible override of the `paimon-spark4-common` `Spark4Shim`. Differences from the
 * 4.1 variant:
 *
 *   - `MergeRows.Keep` takes `(condition, output)` — the `Context` parameter (with `Copy` /
 *     `Update` / `Insert` variants) is 4.1-only.
 *   - `MetadataLogFileIndex` lives at `org.apache.spark.sql.execution.streaming.*` (4.1 moved it to
 *     the `.runtime` subpackage).
 *   - `FileStreamSink` likewise lives at `...execution.streaming.*` (4.1 moved it to `.sinks`).
 *   - `UnresolvedWith.cteRelations` is `Seq[(String, SubqueryAlias)]` — 4.1 extended the tuple with
 *     a trailing `Option[Int]` depth.
 *
 * Runtime dispatch picks this class over the 4.1 `Spark4Shim` because paimon-spark-4.0's own
 * `target/classes` is ahead of the shaded paimon-spark4-common copy on the classpath. The
 * `SparkShim` SPI file registered in paimon-spark4-common points at
 * `org.apache.spark.sql.paimon.shims.Spark4Shim`; classloader resolution returns this 4.0 class.
 */
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
      blobFields: Set[Int],
      blobAsDescriptor: Boolean): SparkInternalRow = {
    new Spark4InternalRowWithBlob(rowType, blobFields, blobAsDescriptor)
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

  override def copyDataSourceV2Relation(
      relation: DataSourceV2Relation,
      newTable: Table): DataSourceV2Relation =
    relation.copy(table = newTable)

  override def copyTableSpecLocation(spec: TableSpec, location: Option[String]): TableSpec =
    spec.copy(location = location)

  override def copyTableSpecProperties(
      spec: TableSpec,
      properties: Map[String, String]): TableSpec =
    spec.copy(properties = properties)

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

  // Spark 4.0 still has `SubstituteUnresolvedOrdinals` (Spark 4.1 removed it because the new
  // resolver framework handles ordinals inline). `PaimonViewResolver` applies the shim's early
  // rules to the parsed view text before storing, so we must substitute `ORDER BY 1` →
  // `ORDER BY col_name` at view-creation time — otherwise the stored plan keeps the literal
  // ordinal and reading the view drops the sort. 4.1 shim omits this rule by design.
  override def earlyBatchRules(): Seq[Rule[LogicalPlan]] =
    Seq(CTESubstitution, SubstituteUnresolvedOrdinals)

  // Spark 4.0's `MergeRows.Keep` has a single 2-arg constructor (condition, output); there is no
  // Copy/Update/Insert context variant. Paimon's merge rewrites treat all three the same in 4.0.
  override def mergeRowsKeepCopy(condition: Expression, output: Seq[Expression]): AnyRef =
    Keep(condition, output)

  override def mergeRowsKeepUpdate(condition: Expression, output: Seq[Expression]): AnyRef =
    Keep(condition, output)

  override def mergeRowsKeepInsert(condition: Expression, output: Seq[Expression]): AnyRef =
    Keep(condition, output)

  override def transformUnresolvedWithCteRelations(
      u: UnresolvedWith,
      transform: SubqueryAlias => SubqueryAlias): UnresolvedWith = {
    u.copy(cteRelations = u.cteRelations.map { case (name, alias) => (name, transform(alias)) })
  }

  override def hasFileStreamSinkMetadata(
      paths: Seq[String],
      hadoopConf: Configuration,
      sqlConf: SQLConf): Boolean = {
    FileStreamSink.hasMetadata(paths, hadoopConf, sqlConf)
  }

  override def createPartitionedMetadataLogFileIndex(
      sparkSession: SparkSession,
      path: Path,
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType): PartitioningAwareFileIndex = {
    new Spark4Shim.PartitionedMetadataLogFileIndex(
      sparkSession,
      path,
      parameters,
      userSpecifiedSchema,
      partitionSchema)
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
}

object Spark4Shim {

  /** Paimon's partition-aware wrapper over Spark 4.0's `MetadataLogFileIndex`. */
  private[shims] class PartitionedMetadataLogFileIndex(
      sparkSession: SparkSession,
      path: Path,
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      override val partitionSchema: StructType)
    extends MetadataLogFileIndex(sparkSession, path, parameters, userSpecifiedSchema) {

    override def partitionSpec(): PartitionSpec = {
      SparkFormatTable.alignPartitionSpec(super.partitionSpec(), partitionSchema)
    }
  }
}
