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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.optimizer.OptimizeMetadataOnlyDeleteFromPaimonTable
import org.apache.paimon.spark.commands.DeleteFromPaimonTableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan, ReplaceData}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

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
 * This rule pattern-matches the `ReplaceData` Spark produced (tagged with
 * `RowLevelOperation.Command.DELETE`) and, if the target is a pure append-only Paimon table (see
 * [[PureAppendOnlyScope]]) and the predicate is metadata-only, rewrites back to
 * `DeleteFromPaimonTableCommand`. Non-metadata-only DELETE is left alone (Spark's `ReplaceData` is
 * correct for data deletes). This is **not** a rewrite of `DeleteFromTable` — it's a restoration
 * layered on top of Spark's existing rewrite output, hence the `…Restore` naming rather than
 * `…Rewrite`.
 */
object Spark41DeleteMetadataRestore extends RewriteRowLevelCommand with PureAppendOnlyScope {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (org.apache.spark.SPARK_VERSION < "4.1") return plan
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformDown {
        case rd: ReplaceData if isMetadataOnlyDeleteOnAppendOnlyPaimon(rd) =>
          val origRelation = rd.originalTable.asInstanceOf[DataSourceV2Relation]
          val fs = origRelation.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
          DeleteFromPaimonTableCommand(origRelation, fs, rd.condition)
      }
    }
  }

  /**
   * Whether a `ReplaceData` node (Spark 4.1's post-rewrite DELETE form) targets a pure append-only
   * Paimon table with a metadata-only predicate, such that converting back to
   * `DeleteFromPaimonTableCommand` would let the optimizer fold to `TruncatePaimonTableWithFilter`.
   */
  private def isMetadataOnlyDeleteOnAppendOnlyPaimon(rd: ReplaceData): Boolean = {
    val writeIsDelete = rd.table match {
      case r: DataSourceV2Relation =>
        r.table match {
          case op: RowLevelOperationTable => op.operation.command() == DELETE
          case _ => false
        }
      case _ => false
    }
    writeIsDelete && (rd.originalTable match {
      case r: DataSourceV2Relation if targetsPureAppendOnly(r) =>
        r.table match {
          case spk: SparkTable =>
            spk.getTable match {
              case fs: FileStoreTable =>
                OptimizeMetadataOnlyDeleteFromPaimonTable.isMetadataOnlyDelete(fs, rd.condition)
              case _ => false
            }
          case _ => false
        }
      case _ => false
    })
  }
}
