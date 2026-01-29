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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertStarAction, MergeAction, MergeIntoTable, UpdateAction, UpdateStarAction}
import org.apache.spark.sql.types.StructType

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
      v2Write: Boolean = false): Seq[Expression] = {
    val attrUpdates = assignments.map(a => AttrUpdate(toRefSeq(a.key), a.value))
    recursiveAlignUpdates(attrs, attrUpdates, Nil, v2Write)
  }

  protected def alignAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment],
      v2Write: Boolean = false): Seq[Assignment] = {
    generateAlignedExpressions(attrs, assignments, v2Write).zip(attrs).map {
      case (expression, field) => Assignment(field, expression)
    }
  }

  /**
   * Align assignments in a MergeAction based on the target table's output attributes.
   *   - DeleteAction: returns as-is
   *   - UpdateAction: aligns assignments for update
   *   - InsertAction: aligns assignments for insert
   */
  protected def alignMergeAction(
      action: MergeAction,
      targetOutput: Seq[Attribute],
      v2Write: Boolean): MergeAction = {
    action match {
      case d @ DeleteAction(_) => d
      case u @ UpdateAction(_, assignments) =>
        u.copy(assignments = alignAssignments(targetOutput, assignments, v2Write))
      case i @ InsertAction(_, assignments) =>
        i.copy(assignments = alignAssignments(targetOutput, assignments, v2Write))
      case _: UpdateStarAction =>
        throw new RuntimeException("UpdateStarAction should not be here.")
      case _: InsertStarAction =>
        throw new RuntimeException("InsertStarAction should not be here.")
      case _ =>
        throw new RuntimeException(s"Can't recognize this action: $action")
    }
  }

  /**
   * Align all MergeActions in a MergeIntoTable based on the target table's output attributes.
   * Returns a new MergeIntoTable with aligned matchedActions, notMatchedActions, and
   * notMatchedBySourceActions.
   */
  protected def alignMergeIntoTable(
      m: MergeIntoTable,
      targetOutput: Seq[Attribute],
      v2Write: Boolean): MergeIntoTable = {
    m.copy(
      matchedActions = m.matchedActions.map(alignMergeAction(_, targetOutput, v2Write)),
      notMatchedActions = m.notMatchedActions.map(alignMergeAction(_, targetOutput, v2Write)),
      notMatchedBySourceActions =
        m.notMatchedBySourceActions.map(alignMergeAction(_, targetOutput, v2Write))
    )
  }

  private def recursiveAlignUpdates(
      targetAttrs: Seq[NamedExpression],
      updates: Seq[AttrUpdate],
      namePrefix: Seq[String] = Nil,
      v2Write: Boolean = false): Seq[Expression] = {

    // build aligned updated expression for each target attr
    targetAttrs.map {
      targetAttr =>
        val headMatchedUpdates = updates.filter(u => resolver(u.ref.head, targetAttr.name))
        if (headMatchedUpdates.isEmpty) {
          if (v2Write) {
            // For V2Write, use default value or NULL for missing columns
            getDefaultValueOrNull(targetAttr)
          } else {
            // For V1Write, return the attr as is
            targetAttr
          }
        } else {
          val exactMatchedUpdate = headMatchedUpdates.find(_.ref.size == 1)
          if (exactMatchedUpdate.isDefined) {
            if (headMatchedUpdates.size == 1) {
              // when an exact match (no nested fields) occurs, it must be the only match, then return it's expr
              castIfNeeded(exactMatchedUpdate.get.expr, targetAttr.dataType)
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
                    v2Write)

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

}
