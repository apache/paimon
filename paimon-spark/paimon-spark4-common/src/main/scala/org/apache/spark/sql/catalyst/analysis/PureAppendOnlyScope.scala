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
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.ExtractV2Table

/**
 * Shared scope predicates for the Spark 4.1 Resolution-batch row-level rewrite rules
 * ([[Spark41AppendOnlyRowLevelRewrite]] for UPDATE + metadata-only DELETE reverse-optimization,
 * [[Spark41MergeIntoRewrite]] for MERGE).
 *
 * Both rules only intercept operations against **pure append-only** Paimon tables: no primary key,
 * row tracking, data evolution, deletion vectors, or fixed-length `CHAR(n)` columns. Tables that
 * violate any of these constraints either have a working V2 rewrite path on 4.1 (PK / DV / RT / DE
 * go through Paimon's own postHoc V1 commands) or race with Spark's `CharVarcharCodegenUtils`
 * padding Project (CHAR columns — see [[hasCharColumn]]).
 *
 * Kept as a mix-in trait so the two rewrite objects stay single-responsibility (one rule per Spark
 * row-level command, mirroring Spark's own `RewriteUpdateTable` / `RewriteMergeIntoTable` layout)
 * while sharing exactly one definition of the scope.
 */
trait PureAppendOnlyScope {

  /**
   * Whether the target of a row-level operation is a pure append-only Paimon table that Spark 4.1's
   * built-in rewrite rules can't handle (see the two rule class docs for why).
   */
  protected def targetsPureAppendOnly(aliasedTable: LogicalPlan): Boolean = {
    EliminateSubqueryAliases(aliasedTable) match {
      case ExtractV2Table(sparkTable: SparkTable) =>
        sparkTable.getTable match {
          case fs: FileStoreTable =>
            fs.primaryKeys().isEmpty &&
            !sparkTable.coreOptions.rowTrackingEnabled() &&
            !sparkTable.coreOptions.dataEvolutionEnabled() &&
            !sparkTable.coreOptions.deletionVectorsEnabled() &&
            !hasCharColumn(fs)
          case _ => false
        }
      case _ => false
    }
  }

  /**
   * Tables with fixed-length `CHAR(n)` columns go through Spark's
   * `CharVarcharCodegenUtils.readSidePadding` Project that gets inserted between the
   * `DataSourceV2Relation` and its consumers. If we intercept before that padding project settles,
   * CheckAnalysis trips on mismatched attribute ids (see PR 7648 history). Let those plans fall
   * through to Paimon's postHoc V1 fallback rules which run after the padding project stabilizes.
   */
  protected def hasCharColumn(fs: FileStoreTable): Boolean = {
    import org.apache.paimon.types.CharType
    import scala.collection.JavaConverters._
    fs.rowType().getFields.asScala.exists(_.`type`().isInstanceOf[CharType])
  }
}
