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

import org.apache.spark.sql.catalyst.plans.logical.MergeAction
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * Marks a `MergeAction` that originated from `INSERT *` / `UPDATE *`. After resolution a star
 * action is indistinguishable from an explicit clause that happens to list every column, but
 * merge-schema needs the original intent to decide between pulling new source columns and filling
 * NULL. Tags do not survive fresh constructor calls — rebuild sites must re-tag via
 * `carryFromStar`.
 */
object PaimonMergeActionTags {

  val FROM_STAR: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("paimon.merge.fromStar")

  def isFromStar(action: MergeAction): Boolean =
    action.getTagValue(FROM_STAR).contains(true)

  def markFromStar[T <: MergeAction](action: T): T = {
    action.setTagValue(FROM_STAR, true)
    action
  }

  def carryFromStar[T <: MergeAction](source: MergeAction, target: T): T = {
    if (isFromStar(source)) markFromStar(target) else target
  }
}
