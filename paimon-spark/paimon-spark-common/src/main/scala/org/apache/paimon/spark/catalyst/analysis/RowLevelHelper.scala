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
import org.apache.paimon.spark.catalyst.optimizer.OptimizeMetadataOnlyDeleteFromPaimonTable
import org.apache.paimon.table.{FileStoreTable, Table}

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, BinaryExpression, EqualTo, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateTable}

trait RowLevelHelper extends SQLConfHelper with AssignmentAlignmentHelper {

  val operation: RowLevelOp

  protected def checkPaimonTable(table: Table): Unit = {
    operation.checkValidity(table)
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
        case EqualTo(left: AttributeReference, right: AttributeReference) =>
          isTargetPrimaryKey(left) || isTargetPrimaryKey(right)
        case Assignment(key: AttributeReference, _) =>
          isTargetPrimaryKey(key)
        case _ => false
      }
  }

  /** Determines if DataSourceV2 is not supported for the given table. */
  protected def shouldFallbackToV1(table: SparkTable): Boolean = {
    val baseTable = table.getTable
    org.apache.spark.SPARK_VERSION < "3.5" ||
    !baseTable.isInstanceOf[FileStoreTable] ||
    !baseTable.primaryKeys().isEmpty ||
    !table.useV2Write ||
    table.coreOptions.deletionVectorsEnabled() ||
    table.coreOptions.rowTrackingEnabled() ||
    table.coreOptions.dataEvolutionEnabled()
  }

  /** Determines if DataSourceV2 delete is not supported for the given table. */
  protected def shouldFallbackToV1Delete(table: SparkTable, condition: Expression): Boolean = {
    shouldFallbackToV1(table) ||
    OptimizeMetadataOnlyDeleteFromPaimonTable.isMetadataOnlyDelete(
      table.getTable.asInstanceOf[FileStoreTable],
      condition)
  }

  /** Determines if DataSourceV2 update is not supported for the given table. */
  protected def shouldFallbackToV1Update(table: SparkTable, updateTable: UpdateTable): Boolean = {
    shouldFallbackToV1(table) ||
    !updateTable.rewritable ||
    !updateTable
      .copy(assignments = alignAssignments(updateTable.table.output, updateTable.assignments))
      .aligned
  }
}
