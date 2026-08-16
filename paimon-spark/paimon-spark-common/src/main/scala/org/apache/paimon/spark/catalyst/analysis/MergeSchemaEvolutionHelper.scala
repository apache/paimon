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
import org.apache.paimon.spark.commands.SchemaEvolutionHelper
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, MergeAction, MergeIntoTable}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * MERGE INTO schema evolution (merge-schema=true). Computes the evolved schema in memory and
 * rewrites the merge plan so that action alignment targets the new columns; the actual schema
 * commit is deferred to execution (the merge command's `run`).
 *
 * Triggered by `UPDATE *` / `INSERT *` or explicit source-bound assignment keys. Scoped to source
 * columns referenced in matched/not-matched actions.
 */
trait MergeSchemaEvolutionHelper extends ExpressionHelper {

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
    if (
      !allActions
        .exists(a => PaimonMergeActionTags.isFromStar(a) || hasSourceBoundKey(a, merge))
    ) {
      return None
    }

    val resolver = spark.sessionState.conf.resolver
    val scopedActions = merge.matchedActions ++ merge.notMatchedActions
    val sourceExprIds: Set[ExprId] = merge.sourceTable.output.map(_.exprId).toSet
    val containsStar = scopedActions.exists(PaimonMergeActionTags.isFromStar)
    val scopedNames: Set[String] = if (containsStar) {
      merge.sourceTable.output.map(_.name).toSet
    } else {
      scopedActions
        .flatMap(extractAssignments)
        .collect {
          case Assignment(attr: Attribute, _) if sourceExprIds.contains(attr.exprId) => attr.name
        }
        .toSet
    }

    val fileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]
    // Pass raw source types: the core merge decides whether to keep or widen the target type, and
    // the action alignment layer casts incoming values to the result.
    val sourceSchema = StructType(
      merge.sourceTable.output
        .filter(a => scopedNames.exists(n => resolver(n, a.name)))
        .map(a => StructField(a.name, a.dataType, a.nullable)))
    // Compute the evolved schema in memory only; the actual schema commit is deferred to execution
    // (the merge command's run / the V2 write's toBatch) so analysis stays side-effect-free. The
    // evolved relation presents the new columns to the plan; existing rows read them as NULL.
    val updatedFileStoreTable = SchemaEvolutionHelper
      .evolvedTableInMemory(fileStoreTable, sourceSchema, spark)
      .getOrElse(return None)

    val updatedV2Table = v2Table.copy(table = updatedFileStoreTable)
    val newOutput = buildEvolvedOutput(
      SparkTypeUtils.fromPaimonRowType(updatedFileStoreTable.schema().logicalRowType()),
      relation.output,
      resolver)
    val updatedRelation =
      SparkShimLoader.shim.copyDataSourceV2Relation(relation, updatedV2Table, newOutput)
    val updatedTargetTable = merge.targetTable.transform {
      case r: DataSourceV2Relation if r eq relation => updatedRelation
    }

    val newAttrs = {
      val oldNames = relation.output.map(_.name).toSet
      newOutput.filterNot(a => oldNames.exists(resolver(_, a.name)))
    }
    // Refresh target-side refs to the NEW dataType — `semanticEquals` checks dataType too.
    val newAttrById: Map[ExprId, AttributeReference] = newOutput.map(a => a.exprId -> a).toMap
    val refresh = refreshTargetRefs(newAttrById) _
    val transformOne = transformAction(refresh, newAttrs, merge.sourceTable.output, resolver) _

    val updatedMerge = SparkShimLoader.shim.createMergeIntoTable(
      updatedTargetTable,
      merge.sourceTable,
      refresh(merge.mergeCondition),
      merge.matchedActions.map(transformOne),
      merge.notMatchedActions.map(transformOne),
      notMatchedBySourceActions.map(transformOne),
      withSchemaEvolution = false
    )
    Some((updatedMerge, updatedRelation, updatedV2Table))
  }

  protected def alignAllMergeActions(
      m: MergeIntoTable,
      targetOutput: Seq[Attribute]): MergeIntoTable = {
    val mergeSchemaEnabled = OptionUtils.writeMergeSchemaEnabled()
    val shim = SparkShimLoader.shim
    shim.createMergeIntoTable(
      m.targetTable,
      m.sourceTable,
      m.mergeCondition,
      PaimonAssignmentUtils.alignActions(m.matchedActions, targetOutput, mergeSchemaEnabled),
      PaimonAssignmentUtils.alignActions(m.notMatchedActions, targetOutput, mergeSchemaEnabled),
      PaimonAssignmentUtils
        .alignActions(shim.notMatchedBySourceActions(m), targetOutput, mergeSchemaEnabled),
      withSchemaEvolution = false
    )
  }

  /** Reuse existing attribute ids; fabricate ones for new fields. */
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

  /** Rewrite target-side `AttributeReference`s to the NEW attribute; source-side refs untouched. */
  private def refreshTargetRefs(newAttrById: Map[ExprId, AttributeReference])(
      expr: Expression): Expression = expr.transformUp {
    case ar: AttributeReference => newAttrById.getOrElse(ar.exprId, ar)
  }

  /**
   * Single pass per action: refresh target refs + rebind source-bound keys to the evolved target +
   * fill remaining new columns (`*`-actions pull from source by name, explicit clauses NULL).
   */
  private def transformAction(
      refresh: Expression => Expression,
      newAttrs: Seq[AttributeReference],
      sourceOutput: Seq[Attribute],
      resolver: Resolver)(action: MergeAction): MergeAction = {
    val fromStar = PaimonMergeActionTags.isFromStar(action)
    val sourceExprIds = sourceOutput.map(_.exprId).toSet
    val newAttrByName: Map[String, AttributeReference] = newAttrs.map(a => a.name -> a).toMap

    def transformAssignments(assignments: Seq[Assignment]): Seq[Assignment] = {
      val covered = scala.collection.mutable.Set.empty[String]
      val transformed = assignments.map {
        case Assignment(key: Attribute, value) if sourceExprIds.contains(key.exprId) =>
          newAttrByName
            .collectFirst {
              case (name, target) if resolver(name, key.name) =>
                covered += target.name
                Assignment(target, refresh(value))
            }
            .getOrElse(Assignment(refresh(key), refresh(value)))
        case a => Assignment(refresh(a.key), refresh(a.value))
      }
      val fill = newAttrs.filterNot(a => covered.exists(resolver(_, a.name))).map {
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
      transformed ++ fill
    }

    val shim = SparkShimLoader.shim
    val rebuilt = action match {
      case DeleteAction(condition) =>
        DeleteAction(condition.map(refresh))
      case PaimonUpdateAction(condition, assignments) =>
        shim.createUpdateAction(condition.map(refresh), transformAssignments(assignments))
      case InsertAction(condition, assignments) =>
        shim.createInsertAction(condition.map(refresh), transformAssignments(assignments))
      case other => other
    }
    PaimonMergeActionTags.carryFromStar(action, rebuilt)
  }

  private def hasSourceBoundKey(action: MergeAction, merge: MergeIntoTable): Boolean = {
    val sourceSet = merge.sourceTable.outputSet
    val targetSet = merge.targetTable.outputSet
    extractAssignments(action).exists(_.key match {
      case attr: Attribute => sourceSet.contains(attr) && !targetSet.contains(attr)
      case _ => false
    })
  }

  private def extractAssignments(action: MergeAction): Seq[Assignment] = action match {
    case PaimonUpdateAction(_, assignments) => assignments
    case InsertAction(_, assignments) => assignments
    case _ => Nil
  }
}
