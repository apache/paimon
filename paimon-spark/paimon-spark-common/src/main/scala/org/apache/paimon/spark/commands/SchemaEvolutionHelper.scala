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

package org.apache.paimon.spark.commands

import org.apache.paimon.options.Options
import org.apache.paimon.schema.{SchemaMergingUtils, TableSchema}
import org.apache.paimon.spark.{SparkConnectorOptions, SparkTable, SparkTypeUtils}
import org.apache.paimon.spark.catalyst.analysis.PaimonOutputResolver
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.{DataFrame, PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Schema evolution flags resolved from write options and session conf. */
private[spark] case class SchemaEvolutionFlags(
    typeWidening: Boolean,
    allowExplicitCast: Boolean,
    caseSensitive: Boolean)

/**
 * Schema evolution entry points for catalog writes. The two `mergeSchema` overloads commit the
 * evolved schema at execution (`WriteIntoPaimonTable.run` for V1, `PaimonV2Write.toBatch` for V2).
 */
private[spark] trait SchemaEvolutionHelper extends WithFileStoreTable {

  val originTable: FileStoreTable

  protected var newTable: Option[FileStoreTable] = None

  override def table: FileStoreTable = newTable.getOrElse(originTable)

  /**
   * V1 catalog write entry (`WriteIntoPaimonTable.run`). The data is already cast to the evolved
   * schema by [[PaimonOutputResolver]] during analysis, so this only commits the schema.
   */
  def mergeSchema(sparkSession: SparkSession, input: DataFrame, options: Options): Unit =
    if (SchemaEvolutionHelper.mergeSchemaEnabled(options))
      commitEvolution(sparkSession, input.schema, options)

  /** V2 catalog write entry (`PaimonV2Write.toBatch`). Commits and returns the write schema. */
  def mergeSchema(dataSchema: StructType, options: Options): StructType = {
    if (!SchemaEvolutionHelper.mergeSchemaEnabled(options)) return dataSchema
    commitEvolution(SparkSession.active, dataSchema, options) match {
      case Some(evolved) => SparkTypeUtils.fromPaimonRowType(evolved.schema().logicalRowType())
      case None => dataSchema
    }
  }

  /** Commit the evolved schema for the incoming data; updates `newTable` and returns it. */
  private def commitEvolution(
      sparkSession: SparkSession,
      dataSchema: StructType,
      options: Options): Option[FileStoreTable] =
    if (SchemaEvolutionHelper.commitSchemaEvolution(table, dataSchema, sparkSession, options)) {
      val evolved = table.copyWithLatestSchema()
      newTable = Some(evolved)
      Some(evolved)
    } else {
      None
    }

  def updateTableWithOptions(options: Map[String, String]): Unit = {
    newTable = Some(table.copy(options.asJava))
  }
}

private[spark] object SchemaEvolutionHelper {

  /**
   * The single side-effect-free merge primitive (drops system columns; `None` when unchanged),
   * shared by [[expectedAttrsForCatalogWrite]] and [[evolvedTableInMemory]];
   * [[commitSchemaEvolution]] is the persisting counterpart.
   */
  private def computeMergedSchema(
      table: FileStoreTable,
      dataSchema: StructType,
      flags: SchemaEvolutionFlags): Option[TableSchema] = {
    val filtered = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val dataRowType = SparkTypeUtils.toPaimonType(filtered).asInstanceOf[RowType]
    val current = table.schema()
    val merged =
      SchemaMergingUtils.mergeSchemas(
        current,
        dataRowType,
        flags.typeWidening,
        flags.allowExplicitCast,
        flags.caseSensitive)
    if (merged.equals(current)) None else Some(merged)
  }

  /**
   * Filter system columns, resolve flags, and commit the merge of `dataSchema` into the stored
   * schema. Returns whether a schema change was committed. Shared by catalog writes and MERGE INTO;
   * callers that need the evolved table reload it via `copyWithLatestSchema`.
   */
  def commitSchemaEvolution(
      table: FileStoreTable,
      dataSchema: StructType,
      sparkSession: SparkSession,
      options: Options = new Options()): Boolean = {
    val filtered = SparkSystemColumns.filterSparkSystemColumns(dataSchema)
    val flags = readFlags(sparkSession, options)
    val dataRowType = SparkTypeUtils.toPaimonType(filtered).asInstanceOf[RowType]
    table
      .store()
      .mergeSchema(dataRowType, flags.typeWidening, flags.allowExplicitCast, flags.caseSensitive)
  }

  /**
   * Persist the schema that MERGE INTO evolved in memory (see [[evolvedTableInMemory]]) and refresh
   * the catalog cache. Called from the merge command's `run`, so the commit happens at execution,
   * not during analysis.
   */
  def commitEvolvedSchemaAtExecution(
      table: FileStoreTable,
      relation: DataSourceV2Relation,
      sparkSession: SparkSession): Unit = {
    if (!OptionUtils.writeMergeSchemaEnabled()) return
    val evolved = SparkTypeUtils.fromPaimonRowType(table.schema().logicalRowType())
    if (commitSchemaEvolution(table, evolved, sparkSession)) {
      // Refresh the catalog cache so later queries see the new schema.
      for (catalog <- relation.catalog; ident <- relation.identifier) {
        catalog.asInstanceOf[TableCatalog].invalidateTable(ident)
      }
    }
  }

  /**
   * The in-memory post-evolution table (not persisted), letting MERGE INTO show the new columns in
   * the plan. `mergeSchemas` assigns the next schema id deterministically, so the deferred
   * [[commitSchemaEvolution]] reproduces an identical schema. `None` when unchanged.
   */
  def evolvedTableInMemory(
      table: FileStoreTable,
      dataSchema: StructType,
      sparkSession: SparkSession,
      options: Options = new Options()): Option[FileStoreTable] =
    computeMergedSchema(table, dataSchema, readFlags(sparkSession, options)).map(table.copy)

  /** Whether schema evolution is enabled, from the per-write options or the session conf. */
  def mergeSchemaEnabled(options: Options): Boolean =
    options.get(SparkConnectorOptions.MERGE_SCHEMA) || OptionUtils.writeMergeSchemaEnabled()

  /**
   * Compute the resolver's expected attributes for a catalog write. When type widening is enabled
   * with `byName` resolution, returns the post-evolution attrs so the resolver can cast incoming
   * data to the widened target types; otherwise returns `table.output` unchanged.
   */
  def expectedAttrsForCatalogWrite(
      table: DataSourceV2Relation,
      querySchema: StructType,
      options: Options,
      isByName: Boolean,
      sparkSession: SparkSession): Seq[Attribute] = {
    val flags = readFlags(sparkSession, options)
    if (!isByName || !mergeSchemaEnabled(options) || !flags.typeWidening) return table.output

    table.table.asInstanceOf[SparkTable].getTable match {
      case fst: FileStoreTable =>
        computeMergedSchema(fst, querySchema, flags)
          .map(s => PaimonUtils.toAttributes(SparkTypeUtils.fromPaimonRowType(s.logicalRowType())))
          .getOrElse(table.output)
      case _ => table.output
    }
  }

  /** Resolve schema evolution flags from write options and session conf. */
  private def readFlags(sparkSession: SparkSession, options: Options): SchemaEvolutionFlags = {
    val typeWidening = options.get(SparkConnectorOptions.TYPE_WIDENING) || OptionUtils
      .writeMergeSchemaTypeWideningEnabled()
    val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST) || OptionUtils
      .writeMergeSchemaExplicitCastEnabled()
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    SchemaEvolutionFlags(typeWidening, allowExplicitCast, caseSensitive)
  }
}
