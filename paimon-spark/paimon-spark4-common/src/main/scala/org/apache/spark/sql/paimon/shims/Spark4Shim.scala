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

import org.apache.paimon.Snapshot
import org.apache.paimon.data.variant.{GenericVariant, Variant}
import org.apache.paimon.spark.catalyst.analysis.Spark4ResolutionRules
import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark4SqlExtensionsParser
import org.apache.paimon.spark.data.{Spark4ArrayData, Spark4InternalRow, Spark4InternalRowWithBlob, SparkArrayData, SparkInternalRow}
import org.apache.paimon.spark.format.FormatTableBatchWrite
import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.spark.write.{PaimonBatchWrite, PaimonDeltaBatchWrite}
import org.apache.paimon.table.{FileStoreTable, FormatTable}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.analysis.CTESubstitution
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Assignment, ColumnDefinition, CreateTableLike, CTERelationRef, DescribeRelation, DescribeTablePartition, InsertAction, LogicalPlan, MergeAction, MergeIntoTable, MergeRows, OverwriteByExpression, OverwritePartitionsDynamic, SubqueryAlias, TableSpec, UnresolvedWith, UpdateAction}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Copy, Insert, Keep, Update}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayData, GeneratedColumn, IdentityColumn, ResolveDefaultColumns, STUtils}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, StagingTableCatalog, SupportsPartitionManagement, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.execution.{SparkFormatTable, SparkPlan}
import org.apache.spark.sql.execution.datasources.{PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.v2.{AtomicReplaceTableAsSelectExec, AtomicReplaceTableExec, CreateTableAsSelectExec, DescribeTableExec, ReplaceTableAsSelectExec, ReplaceTableExec}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.streaming.runtime.MetadataLogFileIndex
import org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, Geography, GeographyType, Geometry, GeometryType, StructType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

import java.net.URI
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

  override def withStorageLocation(
      storage: CatalogStorageFormat,
      locationUri: Option[URI]): CatalogStorageFormat =
    storage.copy(locationUri = locationUri)

  override def overwriteByName(
      table: NamedRelation,
      query: LogicalPlan,
      deleteExpr: Expression,
      writeOptions: Map[String, String]): OverwriteByExpression =
    OverwriteByExpression.byName(
      table,
      query,
      deleteExpr,
      writeOptions,
      withSchemaEvolution = false)

  override def overwritePartitionsDynamicByName(
      table: NamedRelation,
      query: LogicalPlan,
      writeOptions: Map[String, String]): OverwritePartitionsDynamic =
    OverwritePartitionsDynamic.byName(table, query, writeOptions, withSchemaEvolution = false)

  override def createCreateTableAsSelectExec(
      catalog: TableCatalog,
      ident: Identifier,
      partitioning: Seq[Transform],
      query: LogicalPlan,
      tableSpec: TableSpec,
      writeOptions: Map[String, String],
      ifNotExists: Boolean): SparkPlan = {
    CreateTableAsSelectExec(
      catalog,
      ident,
      partitioning,
      query,
      tableSpec,
      writeOptions,
      ifNotExists)
  }

  override def createReplaceTableAsSelectExec(
      catalog: TableCatalog,
      ident: Identifier,
      partitioning: Seq[Transform],
      query: LogicalPlan,
      tableSpec: TableSpec,
      writeOptions: Map[String, String],
      orCreate: Boolean): SparkPlan = {
    ReplaceTableAsSelectExec(
      catalog,
      ident,
      partitioning,
      query,
      tableSpec,
      writeOptions,
      orCreate = orCreate,
      invalidateCache)
  }

  override def createAtomicReplaceTableAsSelectExec(
      catalog: StagingTableCatalog,
      ident: Identifier,
      partitioning: Seq[Transform],
      query: LogicalPlan,
      tableSpec: TableSpec,
      writeOptions: Map[String, String],
      orCreate: Boolean): SparkPlan = {
    AtomicReplaceTableAsSelectExec(
      catalog,
      ident,
      partitioning,
      query,
      tableSpec,
      writeOptions,
      orCreate = orCreate,
      invalidateCache)
  }

  override def createReplaceTableExec(
      catalog: TableCatalog,
      ident: Identifier,
      columns: Array[Column],
      partitioning: Seq[Transform],
      tableSpec: TableSpec,
      orCreate: Boolean): SparkPlan = {
    ReplaceTableExec(
      catalog,
      ident,
      columns,
      partitioning,
      tableSpec,
      orCreate = orCreate,
      invalidateCache)
  }

  override def createAtomicReplaceTableExec(
      catalog: StagingTableCatalog,
      ident: Identifier,
      columns: Array[Column],
      partitioning: Seq[Transform],
      tableSpec: TableSpec,
      orCreate: Boolean): SparkPlan = {
    AtomicReplaceTableExec(
      catalog,
      ident,
      columns,
      partitioning,
      tableSpec,
      orCreate = orCreate,
      invalidateCache)
  }

  override def toReplaceTableColumns(
      tableSchema: StructType,
      schemaOrColumns: Any,
      catalog: TableCatalog,
      ident: Identifier): Array[Column] = {
    val statementType = "REPLACE TABLE"
    val columns = schemaOrColumns.asInstanceOf[Seq[ColumnDefinition]]
    ResolveDefaultColumns.validateCatalogForDefaultValue(columns, catalog, ident)
    GeneratedColumn.validateGeneratedColumns(tableSchema, catalog, ident, statementType)
    IdentityColumn.validateIdentityColumn(tableSchema, catalog, ident)
    columns.map(_.toV2Column(statementType)).toArray
  }

  override def copyTableSpec(
      tableSpec: TableSpec,
      additionalProperties: Map[String, String],
      location: Option[String]): TableSpec = {
    tableSpec.copy(properties = tableSpec.properties ++ additionalProperties, location = location)
  }

  private def invalidateCache(tableCatalog: TableCatalog, ident: Identifier): Unit = {
    tableCatalog.invalidateTable(ident)
  }

  override def createPaimonBatchWrite(
      table: FileStoreTable,
      writeSchema: StructType,
      dataSchema: StructType,
      overwritePartitions: Option[Map[String, String]],
      copyOnWriteScan: Option[PaimonCopyOnWriteScan],
      operationType: Option[Snapshot.Operation]): BatchWrite =
    new PaimonBatchWrite(
      table,
      writeSchema,
      dataSchema,
      overwritePartitions,
      copyOnWriteScan,
      operationType)

  override def createPaimonDeltaBatchWrite(
      table: FileStoreTable,
      rowSchema: StructType,
      rowIdSchema: StructType,
      operationType: Snapshot.Operation,
      readSnapshotId: Option[Long]): BatchWrite =
    new PaimonDeltaBatchWrite(table, rowSchema, rowIdSchema, operationType, readSnapshotId)

  override def createFormatTableBatchWrite(
      table: FormatTable,
      overwriteDynamic: Option[Boolean],
      overwritePartitions: Option[Map[String, String]],
      writeSchema: StructType): BatchWrite =
    new FormatTableBatchWrite(table, overwriteDynamic, overwritePartitions, writeSchema)

  override def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean): CTERelationRef = {
    CTERelationRef(cteId, resolved, output.toSeq, isStreaming)
  }

  override def createClusteredDistribution(
      expressions: Seq[Expression],
      numPartitions: Int): Distribution =
    ClusteredDistribution(
      expressions,
      requireAllClusterKeys = false,
      requiredNumPartitions = Some(numPartitions))

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

  override def notMatchedBySourceActions(merge: MergeIntoTable): Seq[MergeAction] =
    merge.notMatchedBySourceActions

  override def createUpdateAction(
      condition: Option[Expression],
      assignments: Seq[Assignment]): UpdateAction =
    UpdateAction(condition, assignments)

  override def createInsertAction(
      condition: Option[Expression],
      assignments: Seq[Assignment]): InsertAction =
    InsertAction(condition, assignments)

  override def copyDataSourceV2Relation(
      relation: DataSourceV2Relation,
      table: Table,
      output: Seq[AttributeReference]): DataSourceV2Relation = {
    relation.copy(table = table, output = output)
  }

  override def createDataSourceV2ScanRelation(
      relation: DataSourceV2ScanRelation,
      scan: Scan,
      output: Seq[AttributeReference]): DataSourceV2ScanRelation = {
    DataSourceV2ScanRelation(relation.relation, scan, output, None, None)
  }

  override def createClusteredDistribution(
      expressions: Seq[Expression],
      requiredNumPartitions: Option[Int]): Distribution = {
    ClusteredDistribution(expressions, requiredNumPartitions = requiredNumPartitions)
  }

  override def earlyBatchRules(): Seq[Rule[LogicalPlan]] = Seq(CTESubstitution)

  override def mergeRowsKeepCopy(condition: Expression, output: Seq[Expression]): AnyRef =
    Keep(Copy, condition, output)

  override def mergeRowsKeepUpdate(condition: Expression, output: Seq[Expression]): AnyRef =
    Keep(Update, condition, output)

  override def mergeRowsKeepInsert(condition: Expression, output: Seq[Expression]): AnyRef =
    Keep(Insert, condition, output)

  override def transformUnresolvedWithCteRelations(
      u: UnresolvedWith,
      transform: SubqueryAlias => SubqueryAlias): UnresolvedWith = {
    u.copy(cteRelations = u.cteRelations.map {
      case (name, alias, depth) => (name, transform(alias), depth)
    })
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

  override def toPaimonGeometry(o: Object): Array[Byte] =
    o.asInstanceOf[Geometry].getBytes

  // Spark 4.2 (SPARK-57058) folded the geo value classes into `BinaryView`: `SpecializedGetters`
  // lost `getGeometry` / `getGeography` in favour of `getBinaryView`, and `STUtils.stAsBinary` was
  // split into `stGeomAsBinary` / `stGeogAsBinary`. `paimon-spark-4.1` forks this file to keep the
  // pre-4.2 calls.
  override def toPaimonGeometry(row: InternalRow, pos: Int): Array[Byte] =
    STUtils.stGeomAsBinary(row.getBinaryView(pos))

  override def toPaimonGeometry(array: ArrayData, pos: Int): Array[Byte] =
    STUtils.stGeomAsBinary(array.getBinaryView(pos))

  override def toPaimonGeography(o: Object): Array[Byte] =
    o.asInstanceOf[Geography].getBytes

  override def toPaimonGeography(row: InternalRow, pos: Int): Array[Byte] =
    STUtils.stGeogAsBinary(row.getBinaryView(pos))

  override def toPaimonGeography(array: ArrayData, pos: Int): Array[Byte] =
    STUtils.stGeogAsBinary(array.getBinaryView(pos))

  override def toSparkGeometry(wkb: Array[Byte], crs: String): Object = {
    val geometryType = sparkGeometryType(crs)
    STUtils.stGeomFromWKB(wkb, geometryType.srid)
  }

  override def toSparkGeography(wkb: Array[Byte], crs: String, algorithm: String): Object = {
    val geographyType = sparkGeographyType(crs, algorithm)
    // 4.2 renamed the single `stSetSrid` overload pair to `stGeogSetSrid` / `stGeomSetSrid`.
    STUtils.stGeogSetSrid(STUtils.stGeogFromWKB(wkb), geographyType.srid)
  }

  override def isSparkGeometryType(dataType: org.apache.spark.sql.types.DataType): Boolean =
    dataType.isInstanceOf[GeometryType]

  override def isSparkGeographyType(dataType: org.apache.spark.sql.types.DataType): Boolean =
    dataType.isInstanceOf[GeographyType]

  override def SparkGeometryType(crs: String): org.apache.spark.sql.types.DataType =
    sparkGeometryType(crs)

  override def SparkGeographyType(
      crs: String,
      algorithm: String): org.apache.spark.sql.types.DataType = sparkGeographyType(crs, algorithm)

  override def sparkGeometryCrs(dataType: org.apache.spark.sql.types.DataType): String = {
    val geometryType = dataType.asInstanceOf[GeometryType]
    require(!geometryType.isMixedSrid, "Paimon does not support mixed-SRID geometry values")
    geometryType.crs
  }

  override def sparkGeographyCrs(dataType: org.apache.spark.sql.types.DataType): String = {
    val geographyType = dataType.asInstanceOf[GeographyType]
    require(!geographyType.isMixedSrid, "Paimon does not support mixed-SRID geography values")
    geographyType.crs
  }

  override def sparkGeographyAlgorithm(dataType: org.apache.spark.sql.types.DataType): String =
    dataType.asInstanceOf[GeographyType].algorithm.toString

  private def sparkGeometryType(crs: String): GeometryType = {
    val geometryType = GeometryType(crs)
    require(!geometryType.isMixedSrid, "Paimon does not support mixed-SRID geometry values")
    geometryType
  }

  private def sparkGeographyType(crs: String, algorithm: String): GeographyType = {
    val geographyType = GeographyType(crs, algorithm)
    require(!geographyType.isMixedSrid, "Paimon does not support mixed-SRID geography values")
    geographyType
  }

  // SQL UDFs (CREATE FUNCTION ... RETURN ...).
  override def rewritePaimonSQLFunctionCommands(spark: SparkSession): Rule[LogicalPlan] =
    org.apache.spark.sql.catalyst.parser.extensions.RewritePaimonSQLFunctionCommands(spark)

  override def resolvePaimonSQLFunction(
      funcIdent: org.apache.spark.sql.catalyst.FunctionIdentifier,
      function: org.apache.paimon.function.Function,
      arguments: Seq[Expression],
      parser: org.apache.spark.sql.catalyst.parser.ParserInterface): Expression =
    org.apache.paimon.spark.catalog.functions.SQLFunctionConverter
      .toSQLFunctionExpression(funcIdent, function, arguments, parser)

  // Spark 4.2 (SPARK-39660) removed partitionSpec from DescribeRelation; DESCRIBE ... PARTITION is
  // a separate DescribeTablePartition plan there.
  override def createTableLikeParts(plan: LogicalPlan)
      : Option[(Seq[String], Seq[String], Option[String], Option[String], Map[String, String], Boolean, Boolean)] =
    plan match {
      case c: CreateTableLike =>
        // Parser-stage rule: the children have not been analyzed yet.
        (c.name, c.source) match {
          case (target: UnresolvedIdentifier, source: UnresolvedTableOrView) =>
            Some(
              (
                target.nameParts,
                source.multipartIdentifier,
                c.provider,
                c.location,
                c.properties,
                c.ifNotExists,
                // `STORED AS` lands in `serdeInfo`; Paimon tables cannot honour Hive storage syntax.
                c.serdeInfo.isDefined))
          case _ => None
        }
      case _ => None
    }

  override def describeTablePartition(
      plan: LogicalPlan): Option[(LogicalPlan, Map[String, String], Boolean, Seq[Attribute])] =
    plan match {
      case d: DescribeTablePartition =>
        (d.table, d.partitionSpec) match {
          case (
                r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _),
                spec: ResolvedPartitionSpec) =>
            // `ResolvedPartitionSpec` holds the values as an `InternalRow`, so read each field by
            // its declared type and render it. Read the types from `partitionSchema()`, the same
            // schema `ResolvePartitionSpec` used to build `names` and `ident`, so a name can never
            // be missing. (Char/varchar is the one place the declared type and the stored value
            // differ: `convertToPartIdent` casts through `replaceCharVarcharWithString`, leaving a
            // plain `UTF8String` under a `CharType(n)` field. Harmless here — `InternalRow.get`
            // ignores the type argument for a `GenericInternalRow`, `Literal`'s validation
            // dispatches on the physical type, where `CharType` maps to `PhysicalStringType` and
            // accepts a `UTF8String`, and `Literal.toString` has no char/varchar/string branch at
            // all, so the value falls through to `other.toString`.)
            //
            // The rendering is `Literal.toString`, NOT what upstream's own
            // `DescribeTablePartitionExec` uses (`ToPrettyString(...)` + `escapePathName`), because
            // the result is compared for equality against Paimon's `Partition.spec()` rather than
            // displayed. Note this rendering and `Partition.spec()`'s own do not agree for every
            // type: with `partition.legacy-name`
            // (the default) Paimon stores `field.toString()`, so a DATE column holds the epoch day
            // while this renders `2021-01-01`. That mismatch predates Spark 4.2 — on <= 4.1 the
            // parser produced the same `2021-01-01` via `Cast(literal, StringType)`.
            val partSchema = table.partitionSchema()
            val values = spec.names.zipWithIndex.map {
              case (name, i) =>
                val field = partSchema(name)
                val value = spec.ident.get(i, field.dataType)
                name -> Literal(value, field.dataType).toString
            }
            Some((r, values.toMap, d.isExtended, d.output))
          case _ => None
        }
      case _ => None
    }

  override def describeRelationPartitionSpec(plan: DescribeRelation): Map[String, String] =
    Map.empty

  override def createDescribeTableExec(
      output: Seq[Attribute],
      catalogName: String,
      identifier: Identifier,
      table: Table,
      isExtended: Boolean): SparkPlan =
    DescribeTableExec(output, catalogName, identifier, table, isExtended)

  override def mergeNeedsSchemaEvolution(merge: MergeIntoTable): Boolean =
    merge.pendingSchemaChanges.nonEmpty
}

object Spark4Shim {

  /** Paimon's partition-aware wrapper over Spark's `MetadataLogFileIndex`. */
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
