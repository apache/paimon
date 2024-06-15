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

import org.apache.paimon.spark.commands.UpdatePaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.JavaConverters._

object PaimonUpdateTable
  extends Rule[LogicalPlan]
  with RowLevelHelper
  with AssignmentAlignmentHelper {

  override val operation: RowLevelOp = Update

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case u @ UpdateTable(PaimonRelation(table), assignments, condition) if u.resolved =>
        checkPaimonTable(table.getTable)

        table.getTable match {
          case paimonTable: FileStoreTable =>
            val primaryKeys = paimonTable.primaryKeys().asScala
            if (!validUpdateAssignment(u.table.outputSet, primaryKeys, assignments)) {
              throw new RuntimeException("Can't update the primary key column.")
            }

            val relation = PaimonRelation.getPaimonRelation(u.table)
            UpdatePaimonTableCommand(
              relation,
              paimonTable,
              condition.getOrElse(TrueLiteral),
              assignments)

          case _ =>
            throw new RuntimeException("Update Operation is only supported for FileStoreTable.")
        }
    }
  }
}
