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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.commands.{MergeIntoPaimonDataEvolutionTable, MergeIntoPaimonTable}
import org.apache.paimon.table.PrimaryKeyTableUtils.validatePKUpsertDeletable
import org.apache.paimon.table.Table

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Cast, Coalesce, EqualNullSafe, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, NullType, StructType}

import scala.collection.JavaConverters._

/** A post-hoc resolution rule for MergeInto. */
case class PaimonMergeInto(spark: SparkSession)
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with ExpressionHelper
  with MergeSchemaEvolutionHelper {

  override val operation: RowLevelOp = MergeInto

  def apply(plan: LogicalPlan): LogicalPlan = {
    // Spark 4.1 marks the plan analyzed before postHoc runs, bypassing `resolveOperators`. Pure
    // append-only tables on 4.1+ are handled earlier by `Spark41MergeIntoRewrite`.
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case merge: MergeIntoTable
            if merge.resolved && PaimonRelation.isPaimonTable(merge.targetTable) &&
              !PaimonMergeActionTags.isAligned(merge) =>
          val relation = PaimonRelation.getPaimonRelation(merge.targetTable)
          var v2Table = relation.table.asInstanceOf[SparkTable]

          checkPaimonTable(v2Table.getTable)
          checkDeleteActionValidity(v2Table.getTable, merge)
          checkCondition(merge.mergeCondition)
          (merge.matchedActions ++ merge.notMatchedActions)
            .flatMap(_.condition)
            .foreach(checkCondition)

          val primaryKeys = v2Table.getTable.primaryKeys().asScala.toSeq
          if (primaryKeys.nonEmpty) {
            val updateActions = merge.matchedActions.collect { case a: UpdateAction => a }
            checkUpdateActionValidity(
              AttributeSet(relation.output),
              merge.mergeCondition,
              updateActions,
              primaryKeys)
          }

          // Evolve the target schema in memory before alignment so the aligned plan sees the new
          // columns; the commit is deferred to execution (the merge command's run).
          val (resolvedMerge, targetOutput) =
            evolveTargetIfNeeded(merge, relation, v2Table, spark, resolveNotMatchedBySourceActions)
              .map { case (m, r, t) => v2Table = t; (m, r.output) }
              .getOrElse((merge, relation.output))

          val aligned = alignAllMergeActions(resolvedMerge, targetOutput)

          if (!shouldFallbackToV1MergeInto(aligned)) {
            // Tag so the analyzer's idempotence re-run skips this node (V1 fallback is a different
            // node type and doesn't need the tag).
            PaimonMergeActionTags.markAligned(aligned)
          } else {
            buildV1Command(v2Table, resolvedMerge, aligned)
          }
      }
    }
  }

  private def buildV1Command(
      v2Table: SparkTable,
      resolvedMerge: MergeIntoTable,
      aligned: MergeIntoTable): LogicalPlan = {
    val notMatchedBySource = resolveNotMatchedBySourceActions(aligned)
    if (v2Table.coreOptions.dataEvolutionEnabled()) {
      MergeIntoPaimonDataEvolutionTable(
        v2Table,
        resolvedMerge.targetTable,
        resolvedMerge.sourceTable,
        resolvedMerge.mergeCondition,
        aligned.matchedActions,
        aligned.notMatchedActions,
        notMatchedBySource,
        upsertKeyColumns = keyedUpsertColumns(resolvedMerge)
      )
    } else {
      MergeIntoPaimonTable(
        v2Table,
        resolvedMerge.targetTable,
        resolvedMerge.sourceTable,
        resolvedMerge.mergeCondition,
        aligned.matchedActions,
        aligned.notMatchedActions,
        notMatchedBySource
      )
    }
  }

  // Validate before alignment expands whole ROW/ARRAY/MAP assignments into field projections.
  private def keyedUpsertColumns(merge: MergeIntoTable): Set[String] = {
    val sourceIds = merge.sourceTable.output.map(_.exprId).toSet
    val targetIds = merge.targetTable.output.map(_.exprId).toSet
    def unwrap(expression: Expression): Expression = expression match {
      case cast: Cast => unwrap(cast.child)
      case check: AssertNotNull => unwrap(check.child)
      case other => other
    }
    def attribute(expression: Expression): Option[Attribute] = unwrap(expression) match {
      case attr: Attribute => Some(attr)
      case _ => None
    }
    def sameShape(source: DataType, target: DataType): Boolean = (source, target) match {
      case (NullType, _) => true
      case (StructType(left), StructType(right)) =>
        left.length == right.length && left.zip(right).forall {
          case (a, b) =>
            spark.sessionState.analyzer.resolver(a.name, b.name) &&
            sameShape(a.dataType, b.dataType)
        }
      case (ArrayType(left, _), ArrayType(right, _)) => sameShape(left, right)
      case (MapType(lk, lv, _), MapType(rk, rv, _)) => sameShape(lk, rk) && sameShape(lv, rv)
      case (_: StructType | _: ArrayType | _: MapType, _) => false
      case (_, _: StructType | _: ArrayType | _: MapType) => false
      case _ => true
    }
    def sourceColumn(expression: Expression, target: Attribute): Option[Attribute] =
      attribute(expression).filter(
        attr => sourceIds.contains(attr.exprId) && sameShape(attr.dataType, target.dataType))
    def keyPair(left: Expression, right: Expression): Option[(Attribute, Attribute)] = {
      for {
        target <- attribute(left) if targetIds.contains(target.exprId)
        source <- attribute(right) if sourceIds.contains(source.exprId)
        if !target.dataType.isInstanceOf[StructType] &&
          !target.dataType.isInstanceOf[ArrayType] && !target.dataType.isInstanceOf[MapType] &&
          !source.dataType.isInstanceOf[StructType] &&
          !source.dataType.isInstanceOf[ArrayType] && !source.dataType.isInstanceOf[MapType]
      } yield target -> source
    }
    val keys = splitConjunctivePredicates(merge.mergeCondition).map {
      case EqualNullSafe(left, right) => keyPair(left, right).orElse(keyPair(right, left))
      case _ => None
    }
    if (
      keys.isEmpty || keys.exists(_.isEmpty) ||
      resolveNotMatchedBySourceActions(merge).nonEmpty
    ) return Set.empty
    val keyPairs = keys.flatten
    val keyIds = keyPairs.map(_._1.exprId).toSet
    (merge.matchedActions, merge.notMatchedActions) match {
      case (Seq(update: UpdateAction), Seq(insert: InsertAction))
          if update.condition.isEmpty && insert.condition.isEmpty =>
        val modified = update.assignments.filterNot {
          case Assignment(key: Attribute, value: Attribute) => key.exprId == value.exprId
          case _ => false
        }
        val sources = modified.map {
          case Assignment(key: Attribute, Coalesce(Seq(source, previous)))
              if targetIds.contains(key.exprId) && !keyIds.contains(key.exprId) &&
                attribute(previous).exists(_.exprId == key.exprId) =>
            sourceColumn(source, key).map(attr => key.exprId -> attr.exprId)
          case _ => None
        }
        val inserts = insert.assignments.map {
          case Assignment(key: Attribute, value) if targetIds.contains(key.exprId) =>
            sourceColumn(value, key).map(source => key.exprId -> source.exprId)
          case _ => None
        }
        val validValues = insert.assignments.forall {
          case Assignment(key: Attribute, value) =>
            sourceColumn(value, key).isDefined || unwrap(value).isInstanceOf[Literal]
          case _ => false
        }
        val inserted = inserts.flatten.toMap
        val required = sources.flatten ++ keyPairs.map {
          case (target, source) => target.exprId -> source.exprId
        }
        if (
          modified.nonEmpty && sources.forall(_.isDefined) && validValues &&
          inserted == required.toMap
        ) {
          keyPairs.map(_._1.name).toSet
        } else Set.empty
      case _ => Set.empty
    }
  }

  private def checkCondition(condition: Expression): Unit = {
    if (!condition.resolved) {
      throw new RuntimeException(s"Condition $condition should have been resolved.")
    }
    if (SubqueryExpression.hasSubquery(condition)) {
      throw new RuntimeException(s"Condition $condition with subquery can't be supported.")
    }
  }

  /**
   * A `WHEN MATCHED ... THEN DELETE` or `WHEN NOT MATCHED BY SOURCE ... THEN DELETE` clause emits
   * real [[org.apache.paimon.types.RowKind.DELETE]] records into the LSM tree (see
   * `MergeIntoPaimonTable.constructChangedRows`), so the target's merge engine has to be able to
   * consume them.
   *
   * Without this check a merge engine that rejects delete records -- `partial-update` without
   * `partial-update.remove-record-on-delete` -- accepts the write and commits a snapshot that later
   * makes the table unreadable: the failure only surfaces much later, in
   * `PartialUpdateMergeFunction#add`, at *read* time.
   *
   * This mirrors the validation Flink runs for SQL `DELETE`
   * (`SupportsRowLevelOperationFlinkTableSink#applyRowLevelDelete`) and the one
   * `DeleteFromPaimonTableCommand` already runs, so all three paths reject the same tables with the
   * same message.
   */
  private def checkDeleteActionValidity(table: Table, merge: MergeIntoTable): Unit = {
    // Tables without primary keys rewrite the touched files (or maintain deletion vectors) instead
    // of emitting delete records, so no merge engine is involved.
    if (table.primaryKeys().isEmpty) {
      return
    }
    val deletesRows = (merge.matchedActions ++ resolveNotMatchedBySourceActions(merge))
      .exists(_.isInstanceOf[DeleteAction])
    if (deletesRows) {
      validatePKUpsertDeletable(table)
    }
  }

  private def checkUpdateActionValidity(
      targetOutput: AttributeSet,
      mergeCondition: Expression,
      actions: Seq[UpdateAction],
      primaryKeys: Seq[String]): Unit = {
    lazy val isMergeConditionValid = {
      val mergeExpressions = splitConjunctivePredicates(mergeCondition)
      primaryKeys.forall {
        primaryKey => isUpdateExpressionToPrimaryKey(targetOutput, mergeExpressions, primaryKey)
      }
    }

    def isUpdateActionValid(action: UpdateAction): Boolean = {
      validUpdateAssignment(targetOutput, primaryKeys, action.assignments)
    }

    val valid = isMergeConditionValid || actions.forall(isUpdateActionValid)
    if (!valid) {
      throw new RuntimeException("Can't update the primary key column in update clause.")
    }
  }

  private def resolveNotMatchedBySourceActions(merge: MergeIntoTable): Seq[MergeAction] =
    SparkShimLoader.shim.notMatchedBySourceActions(merge)
}
