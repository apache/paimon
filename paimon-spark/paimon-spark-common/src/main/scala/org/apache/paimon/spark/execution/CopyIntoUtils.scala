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

package org.apache.paimon.spark.execution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{Command, InsertIntoDir, LogicalPlan, ParsedStatement}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.paimon.shims.SparkShimLoader

object CopyIntoUtils {

  /**
   * Build a [[DataFrame]] from the inline query of `COPY INTO <location> FROM (<query>)`.
   *
   * The query is parsed with the session parser (the Paimon parser) via `parsePlan`, so it goes
   * through the exact same path as `spark.sql(<query>)`, including Paimon's parser rules (e.g. the
   * v1 function rewrite). Any read-only query is accepted (`SELECT`, `WITH ... SELECT`, `VALUES`,
   * etc.); the only restriction is that it must have no side effects, enforced by the
   * [[hasSideEffect]] guard on the resulting plan: the Paimon parser does not reject statements at
   * parse time (its `parseQuery` just delegates to `parsePlan`), so DDL/DML reaches us as a plan
   * and must be rejected by inspecting the tree.
   */
  def queryToDataFrame(spark: SparkSession, query: String): DataFrame = {
    val plan =
      try {
        spark.sessionState.sqlParser.parsePlan(query)
      } catch {
        case e: ParseException =>
          throw new IllegalArgumentException(
            s"COPY INTO <location> FROM (<query>) only supports read-only queries: $query",
            e)
      }
    if (hasSideEffect(plan)) {
      throw new IllegalArgumentException(
        "COPY INTO <location> FROM (<query>) only supports read-only queries, " +
          s"but got a statement with side effects: $query")
    }
    SparkShimLoader.shim.classicApi.createDataset(spark, plan)
  }

  /**
   * Whether `plan` contains a node with side effects anywhere in its tree:
   *   - `Command` covers resolved commands such as `DROP TABLE`.
   *   - `ParsedStatement` covers parsed-but-unresolved DDL/DML such as `INSERT`, CTAS,
   *     `CREATE VIEW` (it is the parent of `InsertIntoStatement`); a pure SELECT never contains
   *     such a node.
   *   - `InsertIntoDir` (the `INSERT OVERWRITE DIRECTORY` plan) is neither of the above, so it is
   *     matched explicitly.
   *
   * `find` is used rather than `exists` because `TreeNode.exists` is absent on Spark 3.2.
   */
  private def hasSideEffect(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: Command | _: ParsedStatement | _: InsertIntoDir => true
      case _ => false
    }.isDefined
  }

  def quoteIdentifier(catalogName: String, ident: Identifier): String = {
    val parts = Seq(catalogName) ++
      ident.namespace().toSeq ++
      Seq(ident.name())
    parts.filter(_.nonEmpty).map(p => s"`${p.replace("`", "``")}`").mkString(".")
  }

  def extractBaseName(fullPath: String): String = {
    fullPath.substring(fullPath.lastIndexOf('/') + 1)
  }

  /** Count rows per file, keyed by base file name. */
  def countPerFile(df: DataFrame, fileCol: String): Map[String, Long] = {
    df.groupBy(col(fileCol))
      .count()
      .collect()
      .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
      .toMap
  }
}
