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

package org.apache.paimon.spark.extensions

import org.apache.paimon.spark.catalyst.analysis.{PaimonAnalysis, PaimonDeleteTable, PaimonFunctionResolver, PaimonIncompatibleResolutionRules, PaimonMergeInto, PaimonPostHocResolutionRules, PaimonProcedureResolver, PaimonUpdateTable, PaimonViewResolver, ReplacePaimonFunctions, RewriteUpsertTable}
import org.apache.paimon.spark.catalyst.optimizer.{MergePaimonScalarSubqueries, OptimizeMetadataOnlyDeleteFromPaimonTable}
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions
import org.apache.paimon.spark.commands.BucketExpression
import org.apache.paimon.spark.execution.{OldCompatibleStrategy, PaimonStrategy}
import org.apache.paimon.spark.execution.adaptive.DisableUnnecessaryPaimonBucketedScan

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.paimon.shims.SparkShimLoader

/** Spark session extension to extends the syntax and adds the rules. */
class PaimonSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => SparkShimLoader.shim.createSparkParser(parser) }

    // analyzer extensions
    extensions.injectResolutionRule(spark => new PaimonAnalysis(spark))
    extensions.injectResolutionRule(spark => PaimonProcedureResolver(spark))
    extensions.injectResolutionRule(spark => PaimonViewResolver(spark))
    extensions.injectResolutionRule(spark => PaimonFunctionResolver(spark))
    extensions.injectResolutionRule(spark => SparkShimLoader.shim.createCustomResolution(spark))
    extensions.injectResolutionRule(spark => PaimonIncompatibleResolutionRules(spark))
    extensions.injectResolutionRule(spark => RewriteUpsertTable(spark))

    extensions.injectPostHocResolutionRule(spark => ReplacePaimonFunctions(spark))
    extensions.injectPostHocResolutionRule(spark => PaimonPostHocResolutionRules(spark))

    extensions.injectPostHocResolutionRule(_ => PaimonUpdateTable)
    extensions.injectPostHocResolutionRule(_ => PaimonDeleteTable)
    extensions.injectPostHocResolutionRule(spark => PaimonMergeInto(spark))

    // Spark 4.1 moved the row-level command rewrite rules (`RewriteUpdateTable` /
    // `RewriteDeleteFromTable` / `RewriteMergeIntoTable`) into the main Resolution batch and
    // implemented them with `plan resolveOperators { ... }`, which short-circuits on
    // `analyzed=true` nodes. For pure append-only tables (no PK / RT / DE / DV) on Spark 4.1+,
    // UPDATE and MERGE subtrees get flipped to `analyzed=true` by Paimon's own Resolution-batch
    // rules before Spark's rewrite can fire, so the nodes fall through to the physical planner
    // and are rejected with `UNSUPPORTED_FEATURE.TABLE_OPERATION`. DELETE is not affected —
    // Paimon has no Resolution-batch rule touching `DeleteFromTable` — but Spark's unconditional
    // `RewriteDeleteFromTable` defeats Paimon's metadata-only-delete optimization.
    //
    // Three companion rules in `paimon-spark4-common` fix each case:
    //   - `Spark41UpdateTableRewrite`  — transcribes `RewriteUpdateTable` output (ReplaceData)
    //   - `Spark41MergeIntoRewrite`    — transcribes `RewriteMergeIntoTable` output (non-delta)
    //   - `Spark41DeleteMetadataRestore` — reverses Spark's ReplaceData back to
    //     `DeleteFromPaimonTableCommand` for metadata-only deletes so truncate folding kicks in
    //
    // All three use `transformDown` + `AnalysisHelper.allowInvokingTransformsInAnalyzer` to
    // bypass the analyzed short-circuit. All three are loaded reflectively because:
    //   - Under the `spark3` profile the classes are absent (not on classpath).
    //   - Under a Spark 4.0 runtime the class bodies reference 4.1-only types
    //     (`RowLevelOperationTable`, `ReplaceData` six-arg signature, `ExtractV2Table`,
    //     `MergeRows.Keep(Copy, ...)`, ...), so they must not be statically referenced from
    //     `paimon-spark-common` or they'd fail to link when `paimon-spark-4.0` loads this
    //     extension.
    // The `SPARK_VERSION >= "4.1"` gate ensures we only look them up on 4.1+; the
    // `ClassNotFoundException` catch handles the spark3 case silently.
    loadSpark41Rule("Spark41UpdateTableRewrite").foreach(
      rule => extensions.injectResolutionRule(_ => rule))
    loadSpark41Rule("Spark41DeleteMetadataRestore").foreach(
      rule => extensions.injectResolutionRule(_ => rule))
    loadSpark41Rule("Spark41MergeIntoRewrite").foreach(
      rule => extensions.injectResolutionRule(_ => rule))

    // table function extensions
    PaimonTableValuedFunctions.supportedFnNames.foreach {
      fnName =>
        extensions.injectTableFunction(
          PaimonTableValuedFunctions.getTableValueFunctionInjection(fnName))
    }

    // scalar function extensions
    BucketExpression.supportedFnNames.foreach {
      fnName => extensions.injectFunction(BucketExpression.getFunctionInjection(fnName))
    }

    // optimization rules
    extensions.injectOptimizerRule(_ => OptimizeMetadataOnlyDeleteFromPaimonTable)
    extensions.injectOptimizerRule(_ => MergePaimonScalarSubqueries)

    // planner extensions
    extensions.injectPlannerStrategy(spark => PaimonStrategy(spark))
    // old compatible
    extensions.injectPlannerStrategy(spark => OldCompatibleStrategy(spark))

    // query stage preparation
    extensions.injectQueryStagePrepRule(_ => DisableUnnecessaryPaimonBucketedScan)
  }

  /**
   * Reflectively loads a Spark 4.1-only rewrite rule object from `paimon-spark4-common` by its
   * simple class name. Returns `None` under Spark < 4.1 (gate avoids touching 4.1-only types in the
   * rule class bodies) or when the class is not on the classpath (e.g. the `spark3` build).
   */
  private def loadSpark41Rule(simpleName: String): Option[Rule[LogicalPlan]] = {
    if (org.apache.spark.SPARK_VERSION < "4.1") {
      None
    } else {
      try {
        val cls = Class.forName(s"org.apache.spark.sql.catalyst.analysis.$simpleName$$")
        Some(cls.getField("MODULE$").get(null).asInstanceOf[Rule[LogicalPlan]])
      } catch {
        case _: ClassNotFoundException => None
      }
    }
  }
}
