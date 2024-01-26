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

import org.apache.paimon.CoreOptions.MERGE_ENGINE
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, BinaryExpression, EqualTo, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.Assignment

trait RowLevelHelper extends SQLConfHelper {

  val operation: RowLevelOp

  protected def checkPaimonTable(table: SparkTable): Unit = {
    val paimonTable = if (table.getTable.primaryKeys().size() > 0) {
      table.getTable
    } else {
      throw new UnsupportedOperationException(
        s"Only support to $operation table with primary keys.")
    }

    val options = Options.fromMap(paimonTable.options)
    val mergeEngine = options.get(MERGE_ENGINE)
    if (!operation.supportedMergeEngine.contains(mergeEngine)) {
      throw new UnsupportedOperationException(
        s"merge engine $mergeEngine can not support $operation, currently only ${operation.supportedMergeEngine
            .mkString(", ")} can support $operation.")
    }
  }

  protected def checkSubquery(condition: Expression): Unit = {
    if (SubqueryExpression.hasSubquery(condition)) {
      throw new RuntimeException(
        s"Subqueries are not supported in $condition operation (condition = $condition).")
    }
  }

  protected def validUpdateAssignment(
      output: AttributeSet,
      primaryKeys: Seq[String],
      assignments: Seq[Assignment]): Boolean = {
    !primaryKeys.exists {
      primaryKey => isUpdateExpressionToPrimaryKey(output, assignments, primaryKey)
    }
  }

  // Check whether there is an update expression related to primary key.
  protected def isUpdateExpressionToPrimaryKey(
      output: AttributeSet,
      expressions: Seq[Expression],
      primaryKey: String): Boolean = {
    val resolver = conf.resolver

    // Check whether this attribute is same to primary key and is from target table.
    def isTargetPrimaryKey(attr: AttributeReference): Boolean = {
      resolver(primaryKey, attr.name) && output.contains(attr)
    }

    expressions
      .filterNot {
        case BinaryExpression(left, right) => left == right
        case Assignment(key, value) => key == value
      }
      .exists {
        case EqualTo(left: AttributeReference, _) =>
          isTargetPrimaryKey(left)
        case Assignment(key: AttributeReference, _) =>
          isTargetPrimaryKey(key)
        case _ => false
      }
  }
}
