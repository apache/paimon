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

import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
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
      assignments: Seq[Assignment]): Seq[Expression] = {
    val attrUpdates = assignments.map(a => AttrUpdate(toRefSeq(a.key), a.value))
    recursiveAlignUpdates(attrs, attrUpdates)
  }

  protected def alignAssignments(
      attrs: Seq[Attribute],
      assignments: Seq[Assignment]): Seq[Assignment] = {
    generateAlignedExpressions(attrs, assignments).zip(attrs).map {
      case (expression, field) => Assignment(field, expression)
    }
  }

  private def recursiveAlignUpdates(
      targetAttrs: Seq[NamedExpression],
      updates: Seq[AttrUpdate],
      namePrefix: Seq[String] = Nil): Seq[Expression] = {

    // build aligned updated expression for each target attr
    targetAttrs.map {
      targetAttr =>
        val headMatchedUpdates = updates.filter(u => resolver(u.ref.head, targetAttr.name))
        if (headMatchedUpdates.isEmpty) {
          // when no matched update, return the attr as is
          targetAttr
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
                  recursiveAlignUpdates(fieldExprs, newUpdates, namePrefix :+ targetAttr.name)

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

}
