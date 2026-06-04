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

import org.apache.spark.sql.catalyst.plans.logical.{MergeAction, MergeIntoTable}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * [[FROM_STAR]] records that a `MergeAction` came from `INSERT *` / `UPDATE *` — merge-schema needs
 * the original intent to decide between pulling new source columns and filling NULL. Tags are
 * dropped on reconstruct, so rebuild sites must re-tag via `carryFromStar`.
 *
 * [[ALIGNED]] is a node-level tag on `MergeIntoTable` set after the post-hoc rule has aligned
 * assignments to the target output. The outer analyzer batch re-runs the rule once to verify
 * idempotence; this tag short-circuits re-application so values aren't wrapped twice.
 */
object PaimonMergeActionTags {

  val FROM_STAR: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("paimon.merge.fromStar")

  val ALIGNED: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("paimon.merge.aligned")

  def isFromStar(action: MergeAction): Boolean =
    action.getTagValue(FROM_STAR).contains(true)

  def markFromStar[T <: MergeAction](action: T): T = {
    action.setTagValue(FROM_STAR, true)
    action
  }

  def carryFromStar[T <: MergeAction](source: MergeAction, target: T): T = {
    if (isFromStar(source)) markFromStar(target) else target
  }

  def isAligned(merge: MergeIntoTable): Boolean =
    merge.getTagValue(ALIGNED).contains(true)

  def markAligned[T <: MergeIntoTable](merge: T): T = {
    merge.setTagValue(ALIGNED, true)
    merge
  }
}
