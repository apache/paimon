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

package org.apache.spark.sql.catalyst.analysis

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.{MergeSchemaEvolutionHelper, PaimonRelation}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, Exists, Expression, IsNotNull, Literal, MetadataAttribute, MonotonicallyIncreasingID, OuterReference, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, AppendData, DeleteAction, Filter, HintInfo, InsertAction, Join, JoinHint, LogicalPlan, MergeAction, MergeIntoTable, MergeRows, NO_BROADCAST_AND_REPLICATION, Project, ReplaceData, UpdateAction}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Copy, Discard, Insert, Instruction, Keep, ROW_ID, Update}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{OPERATION_COLUMN, WRITE_OPERATION, WRITE_WITH_METADATA_OPERATION}
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2Table}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Spark 4.1-only Resolution-batch rule that rewrites MERGE INTO on Paimon tables eligible for V2
 * copy-on-write (no PK / DE / DV / CHAR) into V2 `ReplaceData` / `AppendData` plans, mirroring
 * Spark's built-in `RewriteMergeIntoTable` for non-`SupportsDelta` row-level tables.
 *
 * In Spark 4.1, `RewriteMergeIntoTable` runs in the Resolution batch via `resolveOperators`, which
 * short-circuits on `analyzed=true` plans — by the time it would fire, the `MergeIntoTable` is
 * already marked analyzed and silently skipped, so the planner rejects it with
 * `UNSUPPORTED_FEATURE.TABLE_OPERATION`. We intercept via `transformDown` under
 * `allowInvokingTransformsInAnalyzer` and inline the three `ReplaceData`/`AppendData` branches.
 * `SupportsDelta` is omitted — Paimon is copy-on-write only.
 *
 * We fire before `ResolveAssignments`, so `m.aligned` is `false`. The rule pre-aligns each action
 * list via `PaimonAssignmentUtils.alignActions` (shared with the postHoc `PaimonMergeInto` rule).
 *
 * Row-tracking-only tables use the same V2 copy-on-write rewrite. CHAR columns are excluded —
 * `readSidePadding` races with the rewrite and trips CheckAnalysis; those plans fall back to the
 * postHoc `PaimonMergeInto` V1 path, which also owns PK / DE / DV tables via
 * `RowLevelHelper.shouldFallbackToV1MergeInto`.
 */
object Spark41MergeIntoRewrite
  extends RewriteRowLevelCommand
  with PredicateHelper
  with MergeSchemaEvolutionHelper
  with PureAppendOnlyScope {

  final private val ROW_FROM_SOURCE = "__row_from_source"
  final private val ROW_FROM_TARGET = "__row_from_target"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case m: MergeIntoTable
            if m.resolved && m.rewritable && !m.needSchemaEvolution &&
              targetsV2CopyOnWriteTable(m.targetTable) =>
          // Pure append-only tables skip postHoc `PaimonMergeInto`, so evolve schema here.
          val evolved = evolveSchemaIfPaimon(m)
          rewrite(alignAllMergeActions(evolved, evolved.targetTable.output))
      }
    }
  }

  private def evolveSchemaIfPaimon(m: MergeIntoTable): MergeIntoTable = {
    if (!PaimonRelation.isPaimonTable(m.targetTable)) return m
    val relation = PaimonRelation.getPaimonRelation(m.targetTable)
    val v2Table = relation.table.asInstanceOf[SparkTable]
    evolveTargetIfNeeded(m, relation, v2Table, SparkSession.active, _.notMatchedBySourceActions)
      .map(_._1)
      .getOrElse(m)
  }

  private def rewrite(m: MergeIntoTable): LogicalPlan = {
    val MergeIntoTable(
      aliasedTable,
      source,
      cond,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions,
      _) = m

    EliminateSubqueryAliases(aliasedTable) match {
      case r @ ExtractV2Table(tbl: SupportsRowLevelOperations) =>
        validateMergeIntoConditions(m)
        val relation = r.asInstanceOf[DataSourceV2Relation]
        if (
          matchedActions.isEmpty && notMatchedBySourceActions.isEmpty &&
          notMatchedActions.size == 1
        ) {
          buildSingleInsertAppendPlan(relation, source, cond, notMatchedActions.head)
        } else if (matchedActions.isEmpty && notMatchedBySourceActions.isEmpty) {
          buildNotMatchedOnlyAppendPlan(relation, source, cond, notMatchedActions)
        } else {
          val operationTable = buildOperationTable(tbl, MERGE, CaseInsensitiveStringMap.empty())
          buildReplaceDataPlan(
            relation,
            operationTable,
            source,
            cond,
            matchedActions,
            notMatchedActions,
            notMatchedBySourceActions)
        }
      case _ =>
        m
    }
  }

  // Fast-path #1: single NOT MATCHED InsertAction. Append over a left-anti join.
  private def buildSingleInsertAppendPlan(
      r: DataSourceV2Relation,
      source: LogicalPlan,
      cond: Expression,
      notMatchedAction: MergeAction): LogicalPlan = {
    val insertAction = notMatchedAction.asInstanceOf[InsertAction]
    val filteredSource = insertAction.condition match {
      case Some(insertCond) => Filter(insertCond, source)
      case None => source
    }
    val joinPlan = Join(filteredSource, r, LeftAnti, Some(cond), JoinHint.NONE)
    val output = insertAction.assignments.map(_.value)
    val outputColNames = r.output.map(_.name)
    val projectList = output.zip(outputColNames).map { case (expr, name) => Alias(expr, name)() }
    val project = Project(projectList, joinPlan)
    AppendData.byPosition(r, project)
  }

  // Fast-path #2: only NOT MATCHED actions. Append over a left-anti join with `MergeRows`.
  private def buildNotMatchedOnlyAppendPlan(
      r: DataSourceV2Relation,
      source: LogicalPlan,
      cond: Expression,
      notMatchedActions: Seq[MergeAction]): LogicalPlan = {
    val joinPlan = Join(source, r, LeftAnti, Some(cond), JoinHint.NONE)
    val notMatchedInstructions = notMatchedActions.map {
      case InsertAction(cond, assignments) =>
        Keep(Insert, cond.getOrElse(TrueLiteral), assignments.map(_.value))
      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3053",
          messageParameters = Map("other" -> other.toString))
    }
    val outputs = notMatchedInstructions.flatMap(_.outputs)
    val mergeRows = MergeRows(
      isSourceRowPresent = TrueLiteral,
      isTargetRowPresent = FalseLiteral,
      matchedInstructions = Nil,
      notMatchedInstructions = notMatchedInstructions,
      notMatchedBySourceInstructions = Nil,
      checkCardinality = false,
      output = generateExpandOutput(r.output, outputs),
      joinPlan
    )
    AppendData.byPosition(r, mergeRows)
  }

  // General path producing a `ReplaceData` plan. Mirrors Spark 4.1.1's
  // `RewriteMergeIntoTable.buildReplaceDataPlan` + `buildReplaceDataMergeRowsPlan`.
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      source: LogicalPlan,
      cond: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction]): ReplaceData = {

    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    val checkCardinality = shouldCheckCardinality(matchedActions)

    val joinType = if (notMatchedActions.isEmpty) LeftOuter else FullOuter
    val joinPlan = join(readRelation, source, joinType, cond, checkCardinality)

    val mergeRowsPlan = buildReplaceDataMergeRowsPlan(
      readRelation,
      joinPlan,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions,
      metadataAttrs,
      checkCardinality)

    val (pushableCond, groupFilterCond) = if (notMatchedBySourceActions.isEmpty) {
      (cond, Some(toGroupFilterCondition(relation, source, cond)))
    } else {
      (TrueLiteral, None)
    }

    val writeRelation = relation.copy(table = operationTable)
    val projections = buildReplaceDataProjections(mergeRowsPlan, relation.output, metadataAttrs)
    ReplaceData(writeRelation, pushableCond, mergeRowsPlan, relation, projections, groupFilterCond)
  }

  private def buildReplaceDataMergeRowsPlan(
      targetTable: LogicalPlan,
      joinPlan: LogicalPlan,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction],
      metadataAttrs: Seq[Attribute],
      checkCardinality: Boolean): MergeRows = {

    // Unmatched target rows must be copied through since groups are being replaced wholesale.
    val carryoverRowsOutput = Literal(WRITE_WITH_METADATA_OPERATION) +: targetTable.output
    val keepCarryoverRowsInstruction = Keep(Copy, TrueLiteral, carryoverRowsOutput)

    val matchedInstructions = matchedActions.map {
      action => toInstruction(action, metadataAttrs)
    } :+ keepCarryoverRowsInstruction

    val notMatchedInstructions =
      notMatchedActions.map(action => toInstruction(action, metadataAttrs))

    val notMatchedBySourceInstructions = notMatchedBySourceActions.map {
      action => toInstruction(action, metadataAttrs)
    } :+ keepCarryoverRowsInstruction

    val rowFromSourceAttr = resolveAttrRef(ROW_FROM_SOURCE, joinPlan)
    val rowFromTargetAttr = resolveAttrRef(ROW_FROM_TARGET, joinPlan)

    val outputs = matchedInstructions.flatMap(_.outputs) ++
      notMatchedInstructions.flatMap(_.outputs) ++
      notMatchedBySourceInstructions.flatMap(_.outputs)

    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val attrs = operationTypeAttr +: targetTable.output

    MergeRows(
      isSourceRowPresent = IsNotNull(rowFromSourceAttr),
      isTargetRowPresent = IsNotNull(rowFromTargetAttr),
      matchedInstructions = matchedInstructions,
      notMatchedInstructions = notMatchedInstructions,
      notMatchedBySourceInstructions = notMatchedBySourceInstructions,
      checkCardinality = checkCardinality,
      output = generateExpandOutput(attrs, outputs),
      joinPlan
    )
  }

  private def toGroupFilterCondition(
      relation: DataSourceV2Relation,
      source: LogicalPlan,
      cond: Expression): Expression = {
    val condWithOuterRefs = cond.transformUp {
      case attr: Attribute if relation.outputSet.contains(attr) => OuterReference(attr)
      case other => other
    }
    val outerRefs = condWithOuterRefs.collect { case OuterReference(e) => e }
    Exists(Filter(condWithOuterRefs, source), outerRefs)
  }

  private def join(
      targetTable: LogicalPlan,
      source: LogicalPlan,
      joinType: JoinType,
      joinCond: Expression,
      checkCardinality: Boolean): LogicalPlan = {
    val rowFromTarget = Alias(TrueLiteral, ROW_FROM_TARGET)()
    val targetTableProjExprs = if (checkCardinality) {
      val rowId = Alias(MonotonicallyIncreasingID(), ROW_ID)()
      targetTable.output ++ Seq(rowFromTarget, rowId)
    } else {
      targetTable.output :+ rowFromTarget
    }
    val targetTableProj = Project(targetTableProjExprs, targetTable)

    val rowFromSource = Alias(TrueLiteral, ROW_FROM_SOURCE)()
    val sourceTableProjExprs = source.output :+ rowFromSource
    val sourceTableProj = Project(sourceTableProjExprs, source)

    val joinHint = if (checkCardinality) {
      JoinHint(leftHint = Some(HintInfo(Some(NO_BROADCAST_AND_REPLICATION))), rightHint = None)
    } else {
      JoinHint.NONE
    }
    Join(targetTableProj, sourceTableProj, joinType, Some(joinCond), joinHint)
  }

  private def shouldCheckCardinality(matchedActions: Seq[MergeAction]): Boolean = {
    matchedActions match {
      case Nil => false
      case Seq(DeleteAction(None)) => false
      case _ => true
    }
  }

  // Mirrors `RewriteMergeIntoTable.toInstruction`.
  private def toInstruction(action: MergeAction, metadataAttrs: Seq[Attribute]): Instruction = {
    action match {
      case UpdateAction(cond, assignments, _) =>
        val rowValues = assignments.map(_.value)
        val metadataValues = nullifyMetadataOnUpdate(metadataAttrs)
        val output = Seq(Literal(WRITE_WITH_METADATA_OPERATION)) ++ rowValues ++ metadataValues
        Keep(Update, cond.getOrElse(TrueLiteral), output)

      case DeleteAction(cond) =>
        Discard(cond.getOrElse(TrueLiteral))

      case InsertAction(cond, assignments) =>
        val rowValues = assignments.map(_.value)
        val metadataValues = metadataAttrs.map(attr => Literal(null, attr.dataType))
        val output = Seq(Literal(WRITE_OPERATION)) ++ rowValues ++ metadataValues
        Keep(Insert, cond.getOrElse(TrueLiteral), output)

      case other =>
        throw new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_3052",
          messageParameters = Map("other" -> other.toString))
    }
  }

  // Mirrors `RewriteMergeIntoTable.validateMergeIntoConditions`.
  private def validateMergeIntoConditions(merge: MergeIntoTable): Unit = {
    checkMergeIntoCondition("SEARCH", merge.mergeCondition)
    val actions = merge.matchedActions ++ merge.notMatchedActions ++ merge.notMatchedBySourceActions
    actions.foreach {
      case DeleteAction(Some(cond)) => checkMergeIntoCondition("DELETE", cond)
      case UpdateAction(Some(cond), _, _) => checkMergeIntoCondition("UPDATE", cond)
      case InsertAction(Some(cond), _) => checkMergeIntoCondition("INSERT", cond)
      case _ => // OK
    }
  }

  private def checkMergeIntoCondition(condName: String, cond: Expression): Unit = {
    if (!cond.deterministic) {
      throw QueryCompilationErrors.nonDeterministicMergeCondition(condName, cond)
    }
    if (SubqueryExpression.hasSubquery(cond)) {
      throw QueryCompilationErrors.subqueryNotAllowedInMergeCondition(condName, cond)
    }
    if (cond.exists(_.isInstanceOf[AggregateExpression])) {
      throw QueryCompilationErrors.aggregationNotAllowedInMergeCondition(condName, cond)
    }
  }

  // Scope checks live in `PureAppendOnlyScope`, shared with `Spark41AppendOnlyRowLevelRewrite`.
}
