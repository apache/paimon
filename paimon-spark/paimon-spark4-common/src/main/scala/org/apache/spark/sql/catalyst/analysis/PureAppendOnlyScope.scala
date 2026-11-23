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

import org.apache.paimon.spark.{SparkTable, SparkTypeUtils}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.ExtractV2Table

/**
 * Shared scope predicates for the row-level rewrite rules of Spark 4.1
 * ([[Spark41UpdateTableRewrite]], [[Spark41MergeIntoRewrite]], [[Spark41DeleteMetadataRestore]])
 * and Spark 4.2 (`Spark42RowLevelCommandRewrite`), including [[MetadataOnlyDeleteRestore]].
 *
 * These rules intercept operations against Paimon tables that are valid for Spark's V2
 * copy-on-write rewrite (no primary key, data evolution, deletion vectors, or fixed-length
 * `CHAR(n)` columns; row-tracking-only tables are included) or for the delta-based rewrite
 * (unaware-bucket deletion-vector append tables, see [[targetsV2DeltaTable]]). Tables that violate
 * these constraints go through Paimon's postHoc V1 commands or Spark's built-in analysis path.
 *
 * Kept as a mix-in trait so the rules share exactly one definition of the scope.
 */
trait PureAppendOnlyScope {

  protected def targetsV2CopyOnWriteTable(aliasedTable: LogicalPlan): Boolean = {
    targetsPaimonFileStoreTable(aliasedTable) {
      case (sparkTable, fs) =>
        fs.primaryKeys().isEmpty &&
        !sparkTable.coreOptions.dataEvolutionEnabled() &&
        !sparkTable.coreOptions.deletionVectorsEnabled() &&
        !SparkTypeUtils.containsCharType(fs.rowType())
    }
  }

  /**
   * Whether the target is a Paimon table on the delta-based row-level path. Delegates to
   * `SparkTable.supportsV2DeltaOps` so the delta gating conditions have a single source of truth
   * (unlike the copy-on-write scope above, the full capability predicate is also the correct plan
   * scope: its extra `useV2Write` / Spark version checks are trivially true once the table has
   * exposed `SupportsRowLevelOperations` on 4.1).
   */
  protected def targetsV2DeltaTable(aliasedTable: LogicalPlan): Boolean = {
    targetsPaimonFileStoreTable(aliasedTable) {
      case (sparkTable, _) => SparkTable.supportsV2DeltaOps(sparkTable)
    }
  }

  private def targetsPaimonFileStoreTable(aliasedTable: LogicalPlan)(
      predicate: (SparkTable, FileStoreTable) => Boolean): Boolean = {
    EliminateSubqueryAliases(aliasedTable) match {
      case ExtractV2Table(sparkTable: SparkTable) =>
        sparkTable.getTable match {
          case fs: FileStoreTable => predicate(sparkTable, fs)
          case _ => false
        }
      case _ => false
    }
  }
}
