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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.{SparkTable, SparkTypeUtils}
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.SchemaHelper
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, MergeAction, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Shared MERGE INTO `merge-schema=true` evolution logic. Mixed in by both the postHoc V1 path
 * ([[PaimonMergeIntoBase]]) and the Spark 4.1 Resolution-batch rewrite for pure append-only tables
 * (`Spark41MergeIntoRewrite`). Only fires when at least one action was authored as `UPDATE *` /
 * `INSERT *`, tracked via [[PaimonMergeActionTags]].
 */
trait MergeSchemaEvolutionHelper extends ExpressionHelper {

  /**
   * @param resolveNotMatchedBySource
   *   how to resolve `notMatchedBySourceActions` on Spark 3.2+/4.x; the version-specific shim is
   *   supplied by the caller.
   */
  protected def evolveTargetIfNeeded(
      merge: MergeIntoTable,
      relation: DataSourceV2Relation,
      v2Table: SparkTable,
      spark: SparkSession,
      resolveNotMatchedBySource: MergeIntoTable => Seq[MergeAction])
      : Option[(MergeIntoTable, DataSourceV2Relation, SparkTable)] = {
    if (!OptionUtils.writeMergeSchemaEnabled()) return None

    val notMatchedBySourceActions = resolveNotMatchedBySource(merge)
    val allActions = merge.matchedActions ++ merge.notMatchedActions ++ notMatchedBySourceActions
    if (!allActions.exists(PaimonMergeActionTags.isFromStar)) return None

    val fileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]
    val sourceSchema = StructType(
      merge.sourceTable.output.map(a => StructField(a.name, a.dataType, a.nullable)))
    val filteredSourceSchema = SparkSystemColumns.filterSparkSystemColumns(sourceSchema)
    val allowExplicitCast = OptionUtils.writeMergeSchemaExplicitCastEnabled()
    val updatedFileStoreTable = SchemaHelper
      .mergeAndCommitSchema(fileStoreTable, filteredSourceSchema, allowExplicitCast)
      .getOrElse(return None)

    // Invalidate Spark catalog cache so subsequent queries see the new schema.
    for (catalog <- relation.catalog; ident <- relation.identifier) {
      catalog.asInstanceOf[TableCatalog].invalidateTable(ident)
    }

    val updatedV2Table = v2Table.copy(table = updatedFileStoreTable)
    val resolver = spark.sessionState.conf.resolver
    val mergedSparkSchema =
      SparkTypeUtils.fromPaimonRowType(updatedFileStoreTable.schema().logicalRowType())
    val newOutput = buildEvolvedOutput(mergedSparkSchema, relation.output, resolver)
    val updatedRelation =
      SparkShimLoader.shim.copyDataSourceV2Relation(relation, updatedV2Table, newOutput)
    val updatedTargetTable = merge.targetTable.transform {
      case r: DataSourceV2Relation if r eq relation => updatedRelation
    }

    val newAttrs = {
      val oldNames = relation.output.map(_.name).toSet
      newOutput.filterNot(a => oldNames.exists(resolver(_, a.name)))
    }
    val expand = expandAction(newAttrs, merge.sourceTable.output, resolver) _

    val updatedMerge = SparkShimLoader.shim.createMergeIntoTable(
      updatedTargetTable,
      merge.sourceTable,
      merge.mergeCondition,
      merge.matchedActions.map(expand),
      merge.notMatchedActions.map(expand),
      notMatchedBySourceActions.map(expand),
      withSchemaEvolution = false
    )
    Some((updatedMerge, updatedRelation, updatedV2Table))
  }

  /** Rebuild the relation's output: reuse existing attribute ids, fabricate ones for new fields. */
  private def buildEvolvedOutput(
      mergedSparkSchema: StructType,
      oldOutput: Seq[Attribute],
      resolver: Resolver): Seq[AttributeReference] = {
    mergedSparkSchema.map {
      field =>
        oldOutput.find(a => resolver(a.name, field.name)) match {
          case Some(existing: AttributeReference) =>
            existing.copy(dataType = field.dataType, nullable = field.nullable)(
              exprId = existing.exprId,
              qualifier = existing.qualifier)
          case _ => AttributeReference(field.name, field.dataType, field.nullable)()
        }
    }
  }

  /**
   * Append assignments for newly-added columns and re-tag the action. Star clauses pull values from
   * source by name; explicit clauses fill NULL.
   */
  private def expandAction(
      newAttrs: Seq[AttributeReference],
      sourceOutput: Seq[Attribute],
      resolver: Resolver)(action: MergeAction): MergeAction = {
    val fromStar = PaimonMergeActionTags.isFromStar(action)
    val newAssignments = newAttrs.map {
      attr =>
        val value: Expression = if (fromStar) {
          sourceOutput
            .find(s => resolver(s.name, attr.name))
            .map(s => castIfNeeded(s, attr.dataType))
            .getOrElse(Literal(null, attr.dataType))
        } else {
          Literal(null, attr.dataType)
        }
        Assignment(attr, value)
    }
    val expanded = action match {
      case i: InsertAction =>
        SparkShimLoader.shim.copyInsertAction(i, i.assignments ++ newAssignments)
      case u: UpdateAction =>
        SparkShimLoader.shim.copyUpdateAction(u, u.assignments ++ newAssignments)
      case d: DeleteAction => d
    }
    PaimonMergeActionTags.carryFromStar(action, expanded)
  }
}
