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
import org.apache.paimon.spark.data.{SparkArrayData, SparkInternalRow}
import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.table.{FileStoreTable, FormatTable}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, LogicalPlan, MergeAction, MergeIntoTable, SubqueryAlias, UnresolvedWith}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * A spark shim trait. It declares methods which have incompatible implementations between Spark 3
 * and Spark 4. The specific SparkShim implementation will be loaded through Service Provider
 * Interface.
 */
trait SparkShim {

  def classicApi: ClassicApi

  def createSparkParser(delegate: ParserInterface): ParserInterface

  def createCustomResolution(spark: SparkSession): Rule[LogicalPlan]

  def createSparkInternalRow(rowType: RowType): SparkInternalRow

  def createSparkInternalRowWithBlob(
      rowType: RowType,
      blobFields: Set[Int],
      blobAsDescriptor: Boolean): SparkInternalRow

  def createSparkArrayData(elementType: DataType): SparkArrayData

  def createTable(
      tableCatalog: TableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table

  /**
   * Constructs a `BatchWrite` for Paimon's V2 write path. The implementation lives in each
   * per-version shim module so the `extends BatchWrite` mixin is compiled against the right Spark
   * minor version: Spark 4.1 added a default method `BatchWrite.commit(.., WriteSummary)` whose
   * inherited signature triggers `ClassNotFoundException: WriteSummary` lazy-linking on Spark 4.0
   * runtimes when the class is loaded for task serialization.
   */
  def createPaimonBatchWrite(
      table: FileStoreTable,
      writeSchema: StructType,
      dataSchema: StructType,
      overwritePartitions: Option[Map[String, String]],
      copyOnWriteScan: Option[PaimonCopyOnWriteScan]): BatchWrite

  /** Same `BatchWrite` mixin problem as [[createPaimonBatchWrite]], but for `FormatTable` writes. */
  def createFormatTableBatchWrite(
      table: FormatTable,
      overwriteDynamic: Option[Boolean],
      overwritePartitions: Option[Map[String, String]],
      writeSchema: StructType): BatchWrite

  def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean): CTERelationRef

  def supportsHashAggregate(
      aggregateBufferAttributes: Seq[Attribute],
      groupingExpression: Seq[Expression]): Boolean

  def supportsObjectHashAggregate(
      aggregateExpressions: Seq[AggregateExpression],
      groupByExpressions: Seq[Expression]): Boolean

  def createMergeIntoTable(
      targetTable: LogicalPlan,
      sourceTable: LogicalPlan,
      mergeCondition: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction],
      withSchemaEvolution: Boolean): MergeIntoTable

  /**
   * Returns the list of "early" substitution rules Paimon needs to apply on a parsed view plan.
   * Spark 3.x exposes both `CTESubstitution` and `SubstituteUnresolvedOrdinals`, but 4.1 removed
   * `SubstituteUnresolvedOrdinals` (its work is handled by the new resolver framework), so the
   * concrete shim chooses the appropriate set for the active Spark version.
   */
  def earlyBatchRules(): Seq[Rule[LogicalPlan]]

  // Build a `MergeRows.Keep` instruction for Paimon's merge rewrites. Spark 4.1 added a leading
  // `Context` parameter; Spark < 3.4 does not have `MergeRows` at all. Returning `AnyRef` here
  // keeps the trait signature free of `MergeRows` so Spark3Shim can link on Spark 3.2 / 3.3.
  def mergeRowsKeepCopy(condition: Expression, output: Seq[Expression]): AnyRef

  def mergeRowsKeepUpdate(condition: Expression, output: Seq[Expression]): AnyRef

  def mergeRowsKeepInsert(condition: Expression, output: Seq[Expression]): AnyRef

  /**
   * Returns a new `UnresolvedWith` with each CTE's `SubqueryAlias` rewritten by the given function.
   * Spark 4.1 extended the cteRelations element tuple from `(String, SubqueryAlias)` to
   * `(String, SubqueryAlias, Option[Int])`, so rebuilding the tuple must live behind a shim.
   */
  def transformUnresolvedWithCteRelations(
      u: UnresolvedWith,
      transform: SubqueryAlias => SubqueryAlias): UnresolvedWith

  /**
   * Returns true when the given set of paths points at a file-stream sink metadata location
   * (formerly `FileStreamSink.hasMetadata`). Spark 4.1 relocated `FileStreamSink` from
   * `org.apache.spark.sql.execution.streaming` to `...streaming.sinks`, so the call must be
   * shimmed.
   */
  def hasFileStreamSinkMetadata(
      paths: Seq[String],
      hadoopConf: org.apache.hadoop.conf.Configuration,
      sqlConf: org.apache.spark.sql.internal.SQLConf): Boolean

  /**
   * Creates a `PartitioningAwareFileIndex` backed by a streaming `MetadataLogFileIndex` with an
   * overridden `partitionSchema`. Spark 4.1 relocated `MetadataLogFileIndex` from
   * `...streaming.MetadataLogFileIndex` to `...streaming.runtime.MetadataLogFileIndex`, so the
   * Paimon subclass lives in each version-specific shim module.
   */
  def createPartitionedMetadataLogFileIndex(
      sparkSession: SparkSession,
      path: org.apache.hadoop.fs.Path,
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType)
      : org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex

  // for variant
  def toPaimonVariant(o: Object): Variant

  def toPaimonVariant(row: InternalRow, pos: Int): Variant

  def toPaimonVariant(array: ArrayData, pos: Int): Variant

  def isSparkVariantType(dataType: org.apache.spark.sql.types.DataType): Boolean

  def SparkVariantType(): org.apache.spark.sql.types.DataType
}
