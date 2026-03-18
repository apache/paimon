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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{MergeAction, MergeIntoTable}

/** A post-hoc resolution rule for MergeInto. */
case class PaimonMergeInto(spark: SparkSession) extends PaimonMergeIntoBase {

  /**
   * Align all MergeActions in a MergeIntoTable based on the target table's output attributes.
   * Returns a new MergeIntoTable with aligned matchedActions and notMatchedActions.
   */
  override def alignMergeIntoTable(
      m: MergeIntoTable,
      targetOutput: Seq[Attribute]): MergeIntoTable = {
    m.copy(
      matchedActions = m.matchedActions.map(alignMergeAction(_, targetOutput)),
      notMatchedActions = m.notMatchedActions.map(alignMergeAction(_, targetOutput))
    )
  }

  override def resolveNotMatchedBySourceActions(merge: MergeIntoTable): Seq[MergeAction] = {
    Seq.empty
  }
}
