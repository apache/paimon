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
import org.apache.paimon.spark.commands.{MergeIntoPaimonDataEvolutionTable, MergeIntoPaimonTable, SchemaHelper}
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._

trait PaimonMergeIntoBase
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with ExpressionHelper
  with AssignmentAlignmentHelper {

  val spark: SparkSession

  override val operation: RowLevelOp = MergeInto

  def apply(plan: LogicalPlan): LogicalPlan = {
    // Spark 4.1 marks the plan analyzed before postHoc runs, so `resolveOperators` short-circuits.
    // Use `transformDown` guarded by `allowInvokingTransformsInAnalyzer` to unconditionally visit
    // every node. Pure append-only tables on 4.1+ are handled earlier by `Spark41MergeIntoRewrite`.
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case merge: MergeIntoTable
            if merge.resolved && PaimonRelation.isPaimonTable(merge.targetTable) =>
          val relation = PaimonRelation.getPaimonRelation(merge.targetTable)
          var v2Table = relation.table.asInstanceOf[SparkTable]
          val dataEvolutionEnabled = v2Table.coreOptions.dataEvolutionEnabled()

          checkPaimonTable(v2Table.getTable)
          checkCondition(merge.mergeCondition)
          merge.matchedActions.flatMap(_.condition).foreach(checkCondition)
          merge.notMatchedActions.flatMap(_.condition).foreach(checkCondition)

          val updateActions = merge.matchedActions.collect { case a: UpdateAction => a }
          val primaryKeys = v2Table.getTable.primaryKeys().asScala.toSeq
          if (primaryKeys.nonEmpty) {
            checkUpdateActionValidity(
              AttributeSet(relation.output),
              merge.mergeCondition,
              updateActions,
              primaryKeys)
          }

          // Commit schema changes before alignment so the aligned plan sees new columns.
          val (resolvedMerge, targetOutput) = mergeSchemaIfNeeded(merge, relation, v2Table) match {
            case Some((updatedMerge, updatedRelation, updatedV2Table)) =>
              v2Table = updatedV2Table
              (updatedMerge, updatedRelation.output)
            case None =>
              (merge, relation.output)
          }

          val alignedMergeIntoTable = alignMergeIntoTable(resolvedMerge, targetOutput)

          if (!shouldFallbackToV1MergeInto(alignedMergeIntoTable)) {
            alignedMergeIntoTable
          } else {
            if (dataEvolutionEnabled) {
              MergeIntoPaimonDataEvolutionTable(
                v2Table,
                resolvedMerge.targetTable,
                resolvedMerge.sourceTable,
                resolvedMerge.mergeCondition,
                alignedMergeIntoTable.matchedActions,
                alignedMergeIntoTable.notMatchedActions,
                resolveNotMatchedBySourceActions(alignedMergeIntoTable)
              )
            } else {
              MergeIntoPaimonTable(
                v2Table,
                resolvedMerge.targetTable,
                resolvedMerge.sourceTable,
                resolvedMerge.mergeCondition,
                alignedMergeIntoTable.matchedActions,
                alignedMergeIntoTable.notMatchedActions,
                resolveNotMatchedBySourceActions(alignedMergeIntoTable)
              )
            }
          }
      }
    }
  }

  /**
   * Evolve the target schema with the source's extra columns, only when at least one action was
   * authored as `UPDATE *` / `INSERT *` (carried by a tag — an explicit clause listing every
   * existing column is NOT treated as star). Returns None when merge-schema is disabled, no action
   * is star, or the source has no extra columns.
   */
  private def mergeSchemaIfNeeded(
      merge: MergeIntoTable,
      relation: DataSourceV2Relation,
      v2Table: SparkTable): Option[(MergeIntoTable, DataSourceV2Relation, SparkTable)] = {
    if (!OptionUtils.writeMergeSchemaEnabled()) {
      return None
    }

    val notMatchedBySourceActions = resolveNotMatchedBySourceActions(merge)
    val anyStarAction = merge.matchedActions.exists(PaimonMergeActionTags.isFromStar) ||
      merge.notMatchedActions.exists(PaimonMergeActionTags.isFromStar) ||
      notMatchedBySourceActions.exists(PaimonMergeActionTags.isFromStar)
    if (!anyStarAction) {
      return None
    }

    val fileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]
    val sourceSchema = StructType(
      merge.sourceTable.output.map(a => StructField(a.name, a.dataType, a.nullable)))

    val filteredSourceSchema = SparkSystemColumns.filterSparkSystemColumns(sourceSchema)
    val allowExplicitCast = OptionUtils.writeMergeSchemaExplicitCastEnabled()
    val updatedFileStoreTable = SchemaHelper
      .mergeAndCommitSchema(fileStoreTable, filteredSourceSchema, allowExplicitCast)
      .getOrElse(return None)

    // Invalidate the Spark catalog cache so subsequent queries see the new schema.
    for {
      catalog <- relation.catalog
      ident <- relation.identifier
    } {
      catalog.asInstanceOf[TableCatalog].invalidateTable(ident)
    }

    val updatedV2Table = v2Table.copy(table = updatedFileStoreTable)
    val resolver = spark.sessionState.conf.resolver
    val mergedSparkSchema =
      SparkTypeUtils.fromPaimonRowType(updatedFileStoreTable.schema().logicalRowType())
    val newOutput = mergedSparkSchema.map {
      field =>
        relation.output.find(a => resolver(a.name, field.name)) match {
          case Some(existing) =>
            existing.copy(dataType = field.dataType, nullable = field.nullable)(
              exprId = existing.exprId,
              qualifier = existing.qualifier)
          case None => AttributeReference(field.name, field.dataType, field.nullable)()
        }
    }
    val updatedRelation =
      SparkShimLoader.shim.copyDataSourceV2Relation(relation, updatedV2Table, newOutput)

    val updatedTargetTable = merge.targetTable.transform {
      case r: DataSourceV2Relation if r eq relation => updatedRelation
    }

    // Newly-added columns: star pulls from source by name, explicit clauses get NULL.
    val oldTargetColNames = relation.output.map(_.name)
    val sourceOutput = merge.sourceTable.output
    val newAttrs = newOutput.filterNot(attr => oldTargetColNames.exists(resolver(_, attr.name)))

    def buildNewColumnAssignments(starLike: Boolean): Seq[Assignment] = {
      newAttrs.map {
        attr =>
          val value = if (starLike) {
            sourceOutput
              .find(s => resolver(s.name, attr.name))
              .map(s => castIfNeeded(s, attr.dataType))
              .getOrElse(Literal(null, attr.dataType))
          } else {
            Literal(null, attr.dataType)
          }
          Assignment(attr, value)
      }
    }

    def expandAction(action: MergeAction): MergeAction = {
      val newAssignments = buildNewColumnAssignments(PaimonMergeActionTags.isFromStar(action))
      val expanded = action match {
        case i: InsertAction =>
          SparkShimLoader.shim.copyInsertAction(i, i.assignments ++ newAssignments)
        case u: UpdateAction =>
          SparkShimLoader.shim.copyUpdateAction(u, u.assignments ++ newAssignments)
        case other => other
      }
      PaimonMergeActionTags.carryFromStar(action, expanded)
    }

    val updatedMerge = SparkShimLoader.shim.createMergeIntoTable(
      updatedTargetTable,
      merge.sourceTable,
      merge.mergeCondition,
      merge.matchedActions.map(expandAction),
      merge.notMatchedActions.map(expandAction),
      notMatchedBySourceActions.map(expandAction),
      withSchemaEvolution = false
    )
    Some((updatedMerge, updatedRelation, updatedV2Table))
  }

  private def checkCondition(condition: Expression): Unit = {
    if (!condition.resolved) {
      throw new RuntimeException(s"Condition $condition should have been resolved.")
    }
    if (SubqueryExpression.hasSubquery(condition)) {
      throw new RuntimeException(s"Condition $condition with subquery can't be supported.")
    }
  }

  /** This check will avoid to update the primary key columns */
  private def checkUpdateActionValidity(
      targetOutput: AttributeSet,
      mergeCondition: Expression,
      actions: Seq[UpdateAction],
      primaryKeys: Seq[String]): Unit = {
    // Check whether there are enough `EqualTo` expressions related to all the primary keys.
    lazy val isMergeConditionValid = {
      val mergeExpressions = splitConjunctivePredicates(mergeCondition)
      primaryKeys.forall {
        primaryKey => isUpdateExpressionToPrimaryKey(targetOutput, mergeExpressions, primaryKey)
      }
    }

    // Check whether there are an update expression related to any primary key.
    def isUpdateActionValid(action: UpdateAction): Boolean = {
      validUpdateAssignment(targetOutput, primaryKeys, action.assignments)
    }

    val valid = isMergeConditionValid || actions.forall(isUpdateActionValid)
    if (!valid) {
      throw new RuntimeException("Can't update the primary key column in update clause.")
    }
  }

  def resolveNotMatchedBySourceActions(merge: MergeIntoTable): Seq[MergeAction]

  def alignMergeIntoTable(m: MergeIntoTable, targetOutput: Seq[Attribute]): MergeIntoTable
}
