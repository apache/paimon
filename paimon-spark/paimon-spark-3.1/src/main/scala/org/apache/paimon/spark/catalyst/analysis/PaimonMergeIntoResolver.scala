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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeAction, MergeIntoTable}

object PaimonMergeIntoResolver extends PaimonMergeIntoResolverBase {

  def resolveNotMatchedBySourceActions(
      merge: MergeIntoTable,
      target: LogicalPlan,
      source: LogicalPlan,
      resolve: (Expression, LogicalPlan) => Expression): Seq[MergeAction] = {
    Seq.empty
  }

  def build(
      merge: MergeIntoTable,
      resolvedCond: Expression,
      resolvedMatched: Seq[MergeAction],
      resolvedNotMatched: Seq[MergeAction],
      resolvedNotMatchedBySource: Seq[MergeAction]): MergeIntoTable = {
    if (resolvedNotMatchedBySource.nonEmpty) {
      throw new RuntimeException("WHEN NOT MATCHED BY SOURCE is not supported here.")
    }

    merge.copy(
      mergeCondition = resolvedCond,
      matchedActions = resolvedMatched,
      notMatchedActions = resolvedNotMatched)
  }

}
