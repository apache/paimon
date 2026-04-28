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

import org.apache.paimon.spark.catalyst.analysis.AssignmentAlignmentHelper

import org.apache.spark.sql.AnalysisException
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
 * Spark 4.1-only Resolution-batch rule that rewrites MERGE INTO plans targeting pure append-only
 * Paimon tables (no PK / RT / DE / DV) into the V2 `ReplaceData` / `AppendData` plans — the same
 * forms Spark's built-in `RewriteMergeIntoTable` produces for `SupportsRowLevelOperations` tables
 * that don't implement `SupportsDelta`.
 *
 * Why this rule exists: Spark 4.1 moved `RewriteMergeIntoTable` into the main Resolution batch AND
 * implemented its `apply` with `plan resolveOperators { ... }`. `AnalysisHelper.resolveOperators*`
 * short-circuits on already-`analyzed` plans, so by the time the rewrite would run the
 * `MergeIntoTable` node has transitioned to `analyzed=true` and the rewrite silently skips. The
 * `MergeIntoTable` then falls through to the physical planner which rejects it with
 * `UNSUPPORTED_FEATURE.TABLE_OPERATION`. This mirrors the `Spark41AppendOnlyRowLevelRewrite`
 * treatment of UPDATE.
 *
 * Firing in the Resolution batch via `transformDown` guarded by
 * `AnalysisHelper.allowInvokingTransformsInAnalyzer` intercepts the plan before the analyzed flag
 * traps Spark's own rule. The body is a near-verbatim transcription of `RewriteMergeIntoTable`'s
 * three `ReplaceData`/`AppendData` branches (kept in lockstep with Spark 4.1.1) so the produced
 * plans go through Paimon's V2 row-level write path (`PaimonSparkCopyOnWriteOperation` ->
 * `PaimonV2WriteBuilder` -> `PaimonBatchWrite`) exactly like Spark would have produced. The
 * `SupportsDelta` branch is intentionally omitted — Paimon's `PaimonSparkCopyOnWriteOperation` is
 * copy-on-write only.
 *
 * Like the UPDATE rule, we fire before `ResolveAssignments` has run, so `m.aligned` is still
 * `false` when we see the plan. The rule mixes in `AssignmentAlignmentHelper` and aligns all
 * merge-action assignments itself (same helper the postHoc `PaimonMergeInto` rule uses) before
 * dispatching to the branch logic, which depends on `m.aligned == true`.
 *
 * Scope mirrors `Spark41AppendOnlyRowLevelRewrite`:
 *   - `SPARK_VERSION >= "4.1"`
 *   - pure append-only `FileStoreTable`s (no PK / RT / DE / DV)
 *   - no CHAR columns — Spark's `CharVarcharCodegenUtils.readSidePadding` Project races with the
 *     rewrite and trips CheckAnalysis when intercepting before the padding stabilises; those plans
 *     fall through to Paimon's postHoc `PaimonMergeInto` V1 fallback instead.
 *
 * Tables with PK / row-tracking / data-evolution / deletion-vectors still route to the postHoc
 * `PaimonMergeInto` V1 command (`MergeIntoPaimonTable` / `MergeIntoPaimonDataEvolutionTable`) via
 * `RowLevelHelper.shouldFallbackToV1MergeInto` — the V1 path is feature-complete for those table
 * shapes and this rule leaves them alone.
 */
object Spark41MergeIntoRewrite
  extends RewriteRowLevelCommand
  with PredicateHelper
  with AssignmentAlignmentHelper
  with PureAppendOnlyScope {

  final private val ROW_FROM_SOURCE = "__row_from_source"
  final private val ROW_FROM_TARGET = "__row_from_target"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case m: MergeIntoTable
            if m.resolved && m.rewritable && !m.needSchemaEvolution &&
              targetsPureAppendOnly(m.targetTable) =>
          rewrite(alignMergeIntoTable(m))
      }
    }
  }

  /* ------------------------------------------------------------------------------------------- *
   * Dispatcher mirroring `RewriteMergeIntoTable.apply`'s three `ReplaceData`/`AppendData`
   * branches. The `SupportsDelta` branch from Spark is intentionally omitted.
   * ------------------------------------------------------------------------------------------- */

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

  /* ------------------------------------------------------------------------------------------- *
   * Assignment alignment. Spark 4.1's `RewriteMergeIntoTable` gates on `m.aligned`, which is set
   * by `ResolveAssignments` earlier in the Resolution batch. Because we fire before that rule,
   * `m.aligned` is still `false`; align manually using the helper `PaimonMergeInto` uses for V1.
   * ------------------------------------------------------------------------------------------- */

  private def alignMergeIntoTable(m: MergeIntoTable): MergeIntoTable = {
    val targetOutput = m.targetTable.output
    m.copy(
      matchedActions = m.matchedActions.map(alignMergeAction(_, targetOutput)),
      notMatchedActions = m.notMatchedActions.map(alignMergeAction(_, targetOutput)),
      notMatchedBySourceActions = m.notMatchedBySourceActions.map(alignMergeAction(_, targetOutput))
    )
  }

  /* ------------------------------------------------------------------------------------------- *
   * Fast-path #1: single NOT MATCHED InsertAction, no MATCHED, no NOT MATCHED BY SOURCE. Rewrite
   * as an append over a left-anti join so Spark can skip the row-level merge machinery.
   * Transcribed from `RewriteMergeIntoTable.apply` case 1.
   * ------------------------------------------------------------------------------------------- */

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

  /* ------------------------------------------------------------------------------------------- *
   * Fast-path #2: only NOT MATCHED actions (possibly multiple), no MATCHED, no NOT MATCHED BY
   * SOURCE. Rewrite as an append over a left-anti join with a `MergeRows` on top to dispatch
   * between the multiple InsertActions. Transcribed from `RewriteMergeIntoTable.apply` case 2.
   * ------------------------------------------------------------------------------------------- */

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

  /* ------------------------------------------------------------------------------------------- *
   * General path producing a `ReplaceData` plan. Transcribed near-verbatim from
   * `RewriteMergeIntoTable.buildReplaceDataPlan` + `buildReplaceDataMergeRowsPlan`. Kept in
   * lockstep with Spark 4.1.1.
   * ------------------------------------------------------------------------------------------- */

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

    // target records that were read but did not match any MATCHED or NOT MATCHED BY SOURCE
    // actions must be copied over and included in the new state of the table as groups are being
    // replaced.
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

  /** Matches `RewriteMergeIntoTable.shouldCheckCardinality`. */
  private def shouldCheckCardinality(matchedActions: Seq[MergeAction]): Boolean = {
    matchedActions match {
      case Nil => false
      case Seq(DeleteAction(None)) => false
      case _ => true
    }
  }

  /**
   * Converts a MERGE action into an instruction for the group-based (copy-on-write) plan. Matches
   * `RewriteMergeIntoTable.toInstruction(action, metadataAttrs)`.
   */
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

  /* ------------------------------------------------------------------------------------------- *
   * Condition validation. Mirrors `RewriteMergeIntoTable.validateMergeIntoConditions`.
   * ------------------------------------------------------------------------------------------- */

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

  // `targetsPureAppendOnly` / `hasCharColumn` are provided by `PureAppendOnlyScope` — kept in
  // one place so this rule and `Spark41AppendOnlyRowLevelRewrite` can't drift apart on what
  // qualifies as a "pure append-only" Paimon target.
}
