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

package org.apache.spark.sql.paimon.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationRef, LogicalPlan, MergeAction, MergeIntoTable}

object MinorVersionShim {

  def createCTERelationRef(
      cteId: Long,
      resolved: Boolean,
      output: Seq[Attribute],
      isStreaming: Boolean): CTERelationRef = CTERelationRef(cteId, resolved, output, isStreaming)

  def createMergeIntoTable(
      targetTable: LogicalPlan,
      sourceTable: LogicalPlan,
      mergeCondition: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction],
      notMatchedBySourceActions: Seq[MergeAction]): MergeIntoTable = {
    MergeIntoTable(
      targetTable,
      sourceTable,
      mergeCondition,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions)
  }

}
