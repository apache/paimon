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
 * Shared scope predicates for the Spark 4.1 Resolution-batch row-level rewrite rules
 * ([[Spark41UpdateTableRewrite]] for UPDATE + metadata-only DELETE reverse-optimization,
 * [[Spark41MergeIntoRewrite]] for MERGE).
 *
 * These rules only intercept operations against Paimon tables that are valid for Spark's V2
 * copy-on-write rewrite: no primary key, data evolution, deletion vectors, or fixed-length
 * `CHAR(n)` columns. Row-tracking-only tables are included; tables that violate any of these
 * constraints go through Paimon's postHoc V1 commands or Spark's built-in analysis path.
 *
 * Kept as a mix-in trait so the two rewrite objects stay single-responsibility (one rule per Spark
 * row-level command, mirroring Spark's own `RewriteUpdateTable` / `RewriteMergeIntoTable` layout)
 * while sharing exactly one definition of the scope.
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
