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
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, Attribute, CreateNamedStruct, Expression, GetStructField, If, IsNull, LambdaFunction, Literal, MapFromArrays, MapKeys, MapValues, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertStarAction, MergeAction, MergeIntoTable, UpdateAction, UpdateStarAction}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

trait AssignmentAlignmentHelper extends SQLConfHelper with ExpressionHelper {

  private lazy val resolver = conf.resolver

  /**
   * @param ref
   *   attribute reference seq, e.g. a => Seq["a"], s.c1 => Seq["s", "c1"]
   * @param expr
   *   update expression
   */
  private case class AttrUpdate(ref: Seq[String], expr: Expression)

  /**
   * Generate aligned expressions, only supports PrimitiveType and StructType. For example, if attrs
   * are [a int, b int, s struct(c1 int, c2 int)] and update assignments are [a = 1, s.c1 = 2], will
   * return [1, b, struct(2, c2)].
   * @param attrs
   *   target attrs
   * @param assignments
   *   update assignments
   * @return
   *   aligned expressions
   */
  protected def generateAlignedExpressions(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      isInsert: Boolean = false): Seq[Expression] = {
    val attrUpdates = assignments.map(a => AttrUpdate(toRefSeq(a.key), a.value))
    recursiveAlignUpdates(attrs, attrUpdates, Nil, isInsert)
  }

  protected def alignAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      isInsert: Boolean = false): Seq[Assignment] = {
    generateAlignedExpressions(attrs, assignments, isInsert).zip(attrs).map {
      case (expression, field) => Assignment(field, expression)
    }
  }

  /** Align a MergeAction's assignments to target output. Star actions must already be expanded. */
  protected def alignMergeAction(action: MergeAction, targetOutput: Seq[Attribute]): MergeAction = {
    // `copyXxxAction` rebuilds the node and drops tags; re-carry FROM_STAR so merge-schema works.
    val aligned = action match {
      case d @ DeleteAction(_) => d
      case u @ PaimonUpdateAction(_, assignments) =>
        SparkShimLoader.shim.copyUpdateAction(u, alignAssignments(targetOutput, assignments))
      case i @ InsertAction(_, assignments) =>
        SparkShimLoader.shim.copyInsertAction(
          i,
          alignAssignments(targetOutput, assignments, isInsert = true))
      case _: UpdateStarAction | _: InsertStarAction =>
        throw new RuntimeException(s"Star action should already be expanded: $action")
      case _ =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
    PaimonMergeActionTags.carryFromStar(action, aligned)
  }

  private def recursiveAlignUpdates(
      targetAttrs: Seq[NamedExpression],
      updates: Seq[AttrUpdate],
      namePrefix: Seq[String] = Nil,
      isInsert: Boolean = false): Seq[Expression] = {

    // build aligned updated expression for each target attr
    targetAttrs.map {
      targetAttr =>
        val headMatchedUpdates = updates.filter(u => resolver(u.ref.head, targetAttr.name))
        if (headMatchedUpdates.isEmpty) {
          if (isInsert) {
            // For Insert, use default value or NULL for missing columns
            getDefaultValueOrNull(targetAttr)
          } else {
            // For Update, return the attr as is
            targetAttr
          }
        } else {
          val exactMatchedUpdate = headMatchedUpdates.find(_.ref.size == 1)
          if (exactMatchedUpdate.isDefined) {
            if (headMatchedUpdates.size == 1) {
              // when an exact match (no nested fields) occurs, it must be the only match, then return it's expr
              resolveByNameAndCast(exactMatchedUpdate.get.expr, targetAttr.dataType)
            } else {
              // otherwise, there must be conflicting updates, for example:
              // - update the same attr multiple times
              // - update a struct attr and its fields at the same time (e.g. s and s.c1)
              val conflictingAttrNames =
                headMatchedUpdates.map(u => (namePrefix ++ u.ref).mkString(".")).distinct
              throw new UnsupportedOperationException(
                s"Conflicting update/insert on attrs: ${conflictingAttrNames.mkString(", ")}"
              )
            }
          } else {
            targetAttr.dataType match {
              case StructType(fields) =>
                val fieldExprs = fields.zipWithIndex.map {
                  case (field, ordinal) =>
                    Alias(GetStructField(targetAttr, ordinal, Some(field.name)), field.name)()
                }
                val newUpdates = updates.map(u => u.copy(ref = u.ref.tail))
                // process StructType's nested fields recursively
                val updatedFieldExprs =
                  recursiveAlignUpdates(
                    fieldExprs,
                    newUpdates,
                    namePrefix :+ targetAttr.name,
                    isInsert)

                // build updated struct expression
                CreateNamedStruct(fields.zip(updatedFieldExprs).flatMap {
                  case (field, expr) =>
                    Seq(Literal(field.name), expr)
                })
              case _ =>
                // can't reach here
                throw new UnsupportedOperationException("")
            }
          }
        }
    }
  }

  /** Get the default value expression for an attribute, or NULL if no default value is defined. */
  private def getDefaultValueOrNull(attr: NamedExpression): Expression = {
    attr match {
      case a: Attribute if a.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY) =>
        val defaultValueStr = a.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
        parseAndResolveDefaultValue(defaultValueStr, a)
      case _ =>
        Literal(null, attr.dataType)
    }
  }

  /** Parse the default value string and resolve it to an expression. */
  private def parseAndResolveDefaultValue(defaultValueStr: String, attr: Attribute): Expression = {
    try {
      val spark = SparkSession.active
      val parsed = spark.sessionState.sqlParser.parseExpression(defaultValueStr)
      castIfNeeded(parsed, attr.dataType)
    } catch {
      case _: Exception =>
        // If parsing fails, fall back to NULL
        Literal(null, attr.dataType)
    }
  }

  /**
   * Resolve an assignment value expression by-name against the target type, then cast if needed.
   * Recursively reorders nested type fields (Struct, Array, Map and any combination) by name to
   * match target field order before casting. This is consistent with Spark's native MERGE INTO
   * behavior (see TableOutputResolver.resolveUpdate).
   */
  private def resolveByNameAndCast(expression: Expression, targetType: DataType): Expression = {
    if (PaimonUtils.sameType(expression.dataType, targetType)) {
      // Types already structurally identical — no reordering needed.
      // This guarantees idempotence when the rule is applied multiple times.
      castIfNeeded(expression, targetType)
    } else {
      val reordered = reorderFieldsByName(expression, expression.dataType, targetType)
      castIfNeeded(reordered, targetType)
    }
  }

  /**
   * Recursively reorder nested type fields by name to match target type's field order. Supports
   * StructType, ArrayType and MapType in any nesting combination. Returns the original expression
   * if no reordering is needed.
   */
  private def reorderFieldsByName(
      expression: Expression,
      sourceType: DataType,
      targetType: DataType): Expression = {
    (sourceType, targetType) match {
      case (s: StructType, t: StructType) if s != t =>
        reorderStructByName(expression, s, t)
      case (ArrayType(sElem, sNull), ArrayType(tElem, _)) if sElem != tElem =>
        val elementVar = NamedLambdaVariable("element", sElem, sNull)
        val reordered = reorderFieldsByName(elementVar, sElem, tElem)
        ArrayTransform(expression, LambdaFunction(reordered, Seq(elementVar)))
      case (MapType(sKey, sVal, sValNull), MapType(tKey, tVal, _))
          if sKey != tKey || sVal != tVal =>
        val keyVar = NamedLambdaVariable("key", sKey, nullable = false)
        val valVar = NamedLambdaVariable("value", sVal, sValNull)
        val reorderedKey = reorderFieldsByName(keyVar, sKey, tKey)
        val reorderedVal = reorderFieldsByName(valVar, sVal, tVal)
        val newKeys = ArrayTransform(MapKeys(expression), LambdaFunction(reorderedKey, Seq(keyVar)))
        val newVals =
          ArrayTransform(MapValues(expression), LambdaFunction(reorderedVal, Seq(valVar)))
        MapFromArrays(newKeys, newVals)
      case _ =>
        expression
    }
  }

  /** Reorder source struct fields to match target field order by name, recursing into nested types. */
  private def reorderStructByName(
      expression: Expression,
      sourceStruct: StructType,
      targetStruct: StructType): Expression = {
    val reorderedFields = targetStruct.map {
      targetField =>
        sourceStruct.fields.zipWithIndex.find(_._1.name == targetField.name) match {
          case Some((sourceField, sourceIdx)) =>
            val fieldExpr = GetStructField(expression, sourceIdx, Some(sourceField.name))
            val reordered =
              reorderFieldsByName(fieldExpr, sourceField.dataType, targetField.dataType)
            Alias(reordered, targetField.name)()
          case None =>
            Alias(Literal(null, targetField.dataType), targetField.name)()
        }
    }
    val struct = CreateNamedStruct(reorderedFields.flatMap(a => Seq(Literal(a.name), a.child)))
    if (expression.nullable) {
      If(IsNull(expression), Literal(null, struct.dataType), struct)
    } else {
      struct
    }
  }
}
