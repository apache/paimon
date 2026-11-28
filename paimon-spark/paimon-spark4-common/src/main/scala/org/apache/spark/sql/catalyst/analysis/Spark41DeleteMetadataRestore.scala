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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}

/**
 * Spark 4.1-only Resolution-batch rule that restores Paimon's metadata-only DELETE optimization
 * after Spark's built-in `RewriteDeleteFromTable` has already rewritten the `DeleteFromTable` node
 * into a V2 `ReplaceData` plan.
 *
 * Unlike [[Spark41UpdateTableRewrite]] / [[Spark41MergeIntoRewrite]], DELETE **does not** hit the
 * `resolveOperators` short-circuit on 4.1 — Paimon has no Resolution-batch rule that advances the
 * `DeleteFromTable` subtree to `analyzed=true`, so Spark's own `RewriteDeleteFromTable` fires
 * normally and produces a correct `ReplaceData`. But that rewrite is unconditional: it also
 * rewrites metadata-only DELETE (whole-table or partition-only predicate) into `ReplaceData`, which
 * defeats Paimon's `OptimizeMetadataOnlyDeleteFromPaimonTable` → `TruncatePaimonTableWithFilter`
 * fast path that a `DeleteFromPaimonTableCommand` would enable.
 *
 * This rule restores, through [[MetadataOnlyDeleteRestore]], the `ReplaceData` (and the
 * `WriteDelta` of unaware-bucket deletion-vector append tables) Spark produced for a metadata-only
 * DELETE on a Paimon table in [[PureAppendOnlyScope]], back to `DeleteFromPaimonTableCommand`.
 * Non-metadata-only DELETE is left alone (Spark's rewrite is correct for data deletes). This is
 * **not** a rewrite of `DeleteFromTable` — it's a restoration layered on top of Spark's existing
 * rewrite output, hence the `…Restore` naming rather than `…Rewrite`. Spark 4.2 and later apply the
 * same restoration in `Spark42RowLevelCommandRewrite`.
 */
object Spark41DeleteMetadataRestore extends RewriteRowLevelCommand with MetadataOnlyDeleteRestore {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown { case RestoredMetadataOnlyDelete(command) => command }
    }
  }
}
