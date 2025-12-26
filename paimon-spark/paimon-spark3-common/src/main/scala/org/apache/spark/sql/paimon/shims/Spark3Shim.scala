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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Instruction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

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
      blobFieldIndex: Int,
      blobAsDescriptor: Boolean): SparkInternalRow = {
    new Spark3InternalRowWithBlob(rowType, blobFieldIndex, blobAsDescriptor)
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

  override def createKeep(
      context: String,
      condition: Expression,
      output: Seq[Expression]): Instruction = {
    MergeRows.Keep(condition, output)
  }

  override def toPaimonVariant(o: Object): Variant = throw new UnsupportedOperationException()

  override def isSparkVariantType(dataType: org.apache.spark.sql.types.DataType): Boolean = false

  override def SparkVariantType(): org.apache.spark.sql.types.DataType =
    throw new UnsupportedOperationException()

  override def toPaimonVariant(row: InternalRow, pos: Int): Variant =
    throw new UnsupportedOperationException()

  override def toPaimonVariant(array: ArrayData, pos: Int): Variant =
    throw new UnsupportedOperationException()

  def createFileIndex(
      options: CaseInsensitiveStringMap,
      sparkSession: SparkSession,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType): PartitioningAwareFileIndex = {

    class PartitionedMetadataLogFileIndex(
        sparkSession: SparkSession,
        path: Path,
        parameters: Map[String, String],
        userSpecifiedSchema: Option[StructType],
        override val partitionSchema: StructType)
      extends MetadataLogFileIndex(sparkSession, path, parameters, userSpecifiedSchema)

    class PartitionedInMemoryFileIndex(
        sparkSession: SparkSession,
        rootPathsSpecified: Seq[Path],
        parameters: Map[String, String],
        userSpecifiedSchema: Option[StructType],
        fileStatusCache: FileStatusCache = NoopCache,
        userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
        metadataOpsTimeNs: Option[Long] = None,
        override val partitionSchema: StructType)
      extends InMemoryFileIndex(
        sparkSession,
        rootPathsSpecified,
        parameters,
        userSpecifiedSchema,
        fileStatusCache,
        userSpecifiedPartitionSpec,
        metadataOpsTimeNs)

    def globPaths: Boolean = {
      val entry = options.get(DataSource.GLOB_PATHS_KEY)
      Option(entry).forall(_ == "true")
    }

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
      new PartitionedMetadataLogFileIndex(
        sparkSession,
        new Path(paths.head),
        options.asScala.toMap,
        userSpecifiedSchema,
        partitionSchema = partitionSchema)
    } else {
      val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(
        paths,
        hadoopConf,
        checkEmptyGlobPath = true,
        checkFilesExist = true,
        enableGlobbing = globPaths)
      val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

      new PartitionedInMemoryFileIndex(
        sparkSession,
        rootPathsSpecified,
        caseSensitiveMap,
        userSpecifiedSchema,
        fileStatusCache,
        partitionSchema = partitionSchema)
    }
  }
}
