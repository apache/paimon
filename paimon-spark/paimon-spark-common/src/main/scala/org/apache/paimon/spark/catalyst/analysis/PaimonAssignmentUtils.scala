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

import org.apache.paimon.spark.SparkTypeUtils.CURRENT_DEFAULT_COLUMN_METADATA_KEY
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.catalyst.analysis.PaimonOutputResolver.MissingFieldBehavior

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertStarAction, MergeAction, UpdateStarAction}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.StructType

/**
 * Top-level alignment for MERGE actions. Per-value work goes through
 * [[PaimonOutputResolver.resolveValue]] with a [[MissingFieldBehavior]] picked here: `UPDATE *` +
 * struct target under merge-schema → `PreserveTarget` (keeps current target value); otherwise
 * `MissingFieldBehavior.of(mergeSchemaEnabled)`.
 *
 * Strict mode: top-level `INSERT *` / `UPDATE *` missing source column throws; explicit clauses
 * still fill / preserve unmentioned columns. Merge-schema mode: missing top-level columns get
 * `CURRENT_DEFAULT` / NULL; source-extras kept for write-time schema evolution.
 */
object PaimonAssignmentUtils extends SQLConfHelper {

  def alignActions(
      actions: Seq[MergeAction],
      targetOutput: Seq[Attribute],
      mergeSchemaEnabled: Boolean): Seq[MergeAction] = {
    actions.map(alignAction(_, targetOutput, mergeSchemaEnabled))
  }

  // Re-carries `FROM_STAR` (dropped on reconstruct); construction goes through the version shim.
  def alignAction(
      action: MergeAction,
      targetOutput: Seq[Attribute],
      mergeSchemaEnabled: Boolean): MergeAction = {
    val fromStar = PaimonMergeActionTags.isFromStar(action)
    val shim = SparkShimLoader.shim
    val aligned = action match {
      case d @ DeleteAction(_) => d
      case PaimonUpdateAction(condition, assignments) =>
        shim.createUpdateAction(
          condition,
          alignUpdateAssignments(targetOutput, assignments, fromStar, mergeSchemaEnabled))
      case InsertAction(condition, assignments) =>
        shim.createInsertAction(
          condition,
          alignInsertAssignments(targetOutput, assignments, fromStar, mergeSchemaEnabled))
      case _: UpdateStarAction | _: InsertStarAction =>
        throw new RuntimeException(s"Star action should already be expanded: $action")
      case _ =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
    PaimonMergeActionTags.carryFromStar(action, aligned)
  }

  /** Align UPDATE assignments (incl. nested keys like `s.c1`) to the target attribute list. */
  def alignUpdateAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      fromStar: Boolean,
      mergeSchemaEnabled: Boolean): Seq[Assignment] = {

    val output = attrs.map {
      attr =>
        applyAssignments(
          col = restoreActualType(attr),
          colExpr = attr,
          assignments = assignments,
          colPath = Seq(attr.name),
          mergeSchemaEnabled = mergeSchemaEnabled,
          updateStar = fromStar
        )
    }
    attrs.zip(output).map { case (attr, expr) => Assignment(attr, expr) }
  }

  /** Align INSERT assignments. Nested-key INSERT is rejected. */
  def alignInsertAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      fromStar: Boolean,
      mergeSchemaEnabled: Boolean): Seq[Assignment] = {

    val (topLevel, nested) = assignments.partition(_.key.isInstanceOf[Attribute])
    if (nested.nonEmpty) {
      val nestedStr = nested.map(_.sql).mkString(", ")
      throw new RuntimeException(s"INSERT assignment keys cannot be nested fields: $nestedStr")
    }
    val resolver = conf.resolver

    attrs.map {
      attr =>
        val matching = topLevel.collect {
          case a if a.key.semanticEquals(attr) => a
          case a @ Assignment(k: Attribute, _) if resolver(k.name, attr.name) => a
        }
        val value = if (matching.isEmpty) {
          if (fromStar && !mergeSchemaEnabled) {
            throw new RuntimeException(
              s"Cannot INSERT * into target column '${attr.name}': source is missing this " +
                s"column. Enable 'spark.paimon.write.merge-schema' to fill it with " +
                s"CURRENT_DEFAULT / NULL.")
          }
          getDefaultValueOrNull(attr)
        } else if (matching.size > 1) {
          val conflict = matching.map(_.value.sql).mkString(", ")
          throw new RuntimeException(s"Multiple assignments for '${attr.name}': $conflict")
        } else {
          val actual = restoreActualType(attr)
          PaimonOutputResolver.resolveValue(
            matching.head.value,
            actual,
            MissingFieldBehavior.of(mergeSchemaEnabled),
            None,
            Seq(attr.name))
        }
        Assignment(attr, value)
    }
  }

  private def applyAssignments(
      col: Attribute,
      colExpr: Expression,
      assignments: Seq[Assignment],
      colPath: Seq[String],
      mergeSchemaEnabled: Boolean,
      updateStar: Boolean = false): Expression = {

    val (exactAssignments, otherAssignments) =
      assignments.partition(assignment => assignment.key.semanticEquals(colExpr))

    val fieldAssignments =
      otherAssignments.filter(
        assignment => assignment.key.find(_.semanticEquals(colExpr)).isDefined)

    if (exactAssignments.size > 1) {
      val conflict = exactAssignments.map(_.value.sql).mkString(", ")
      throw new RuntimeException(s"Multiple assignments for '${colPath.mkString(".")}': $conflict")
    } else if (exactAssignments.nonEmpty && fieldAssignments.nonEmpty) {
      val conflict = (exactAssignments ++ fieldAssignments).map(_.sql).mkString(", ")
      throw new RuntimeException(
        s"Conflicting assignments for '${colPath.mkString(".")}': $conflict")
    } else if (exactAssignments.isEmpty && fieldAssignments.isEmpty) {
      // Strict `UPDATE *` requires every top-level target column to have a source match.
      if (updateStar && !mergeSchemaEnabled && colPath.size == 1) {
        throw new RuntimeException(
          s"Cannot UPDATE * target column '${colPath.mkString(".")}': source is missing this " +
            s"column. Enable 'spark.paimon.write.merge-schema' to preserve the current value.")
      }
      colExpr
    } else if (exactAssignments.nonEmpty) {
      val value = exactAssignments.head.value
      val (behavior, targetExpr) = col.dataType match {
        case _: StructType if updateStar && mergeSchemaEnabled =>
          (MissingFieldBehavior.PreserveTarget, Some(colExpr))
        case _ =>
          (MissingFieldBehavior.of(mergeSchemaEnabled), None)
      }
      PaimonOutputResolver.resolveValue(value, col, behavior, targetExpr, colPath)
    } else {
      applyFieldAssignments(col, colExpr, fieldAssignments, colPath, mergeSchemaEnabled)
    }
  }

  private def applyFieldAssignments(
      col: Attribute,
      colExpr: Expression,
      assignments: Seq[Assignment],
      colPath: Seq[String],
      mergeSchemaEnabled: Boolean): Expression = {

    col.dataType match {
      case structType: StructType =>
        val fieldAttrs = PaimonUtils.toAttributes(structType)
        val fieldExprs = structType.fields.zipWithIndex.map {
          case (field, ordinal) => GetStructField(colExpr, ordinal, Some(field.name))
        }
        val updatedFieldExprs = fieldAttrs.zip(fieldExprs).map {
          case (fieldAttr, fieldExpr) =>
            applyAssignments(
              fieldAttr,
              fieldExpr,
              assignments,
              colPath :+ fieldAttr.name,
              mergeSchemaEnabled)
        }
        toNamedStruct(structType, updatedFieldExprs)

      case otherType =>
        throw new RuntimeException(
          s"Updating nested fields is only supported for StructType but " +
            s"'${colPath.mkString(".")}' is of type $otherType")
    }
  }

  private def toNamedStruct(structType: StructType, fieldExprs: Seq[Expression]): Expression = {
    val parts = structType.fields.zip(fieldExprs).flatMap {
      case (field, expr) => Seq(Literal(field.name), expr)
    }
    CreateNamedStruct(parts)
  }

  private def restoreActualType(attr: Attribute): Attribute = {
    attr.withDataType(CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType))
  }

  /** Parse the column's `CURRENT_DEFAULT` SQL (cast to column type) or fall back to NULL. */
  private def getDefaultValueOrNull(attr: Attribute): Expression = {
    val nullLit = Literal(null, attr.dataType)
    if (attr.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
      try {
        val parsed = SparkSession.active.sessionState.sqlParser
          .parseExpression(attr.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY))
        if (parsed.dataType == attr.dataType) parsed
        else Compatibility.cast(parsed, attr.dataType, Option(conf.sessionLocalTimeZone))
      } catch {
        case scala.util.control.NonFatal(_) => nullLit
      }
    } else {
      nullLit
    }
  }
}
