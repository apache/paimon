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

import org.apache.paimon.spark.catalog.SupportView
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedWith}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.DataSource

import scala.util.control.NonFatal

/** Binds the relations of a Paimon view body to the view's catalog and namespace. */
object PaimonViewRelations {

  // The name is a FunctionIdentifier on Spark 3.2 and 3.3 and a Seq[String] since 3.4.
  private def isPaimonFunction(f: UnresolvedTableValuedFunction): Boolean = {
    val name = f.productElement(0) match {
      case id: FunctionIdentifier => id.funcName
      case parts: Seq[_] => parts.lastOption.map(_.toString).getOrElse("")
      case other => String.valueOf(other)
    }
    PaimonTableValuedFunctions.supportedFnNames.exists(_.equalsIgnoreCase(name))
  }

  /**
   * Qualifies the unresolved relations of a view body that has gone through CTE substitution: a
   * single name gets the view's catalog and namespace, and a multi-part name that does not start
   * with a catalog gets the view's catalog. Temporary views and `format.`path`` keep their names.
   * Flink also resolves the body of a Paimon view in the view's database.
   *
   * Some names only become relations later in analysis: those of a CTE that CTE substitution left
   * in place (under the LEGACY CTE policy), of `IDENTIFIER(...)` that the parser did not fold, and
   * the table names Paimon's table-valued functions take as strings. A body with any of them is
   * left unchanged and resolves in the current namespace, as before, so that its names do not bind
   * partly here and partly there.
   */
  def qualify(
      spark: SparkSession,
      plan: LogicalPlan,
      catalog: SupportView,
      ident: Identifier): LogicalPlan = {
    val deferred = plan.collectWithSubqueries {
      case w: UnresolvedWith => w
      // Spark 3.5+; matched by name so that this links on older versions.
      case p if p.nodeName == "PlanWithUnresolvedIdentifier" => p
      case f: UnresolvedTableValuedFunction if isPaimonFunction(f) => f
    }
    if (deferred.nonEmpty) {
      return plan
    }

    val catalogManager = spark.sessionState.catalogManager
    val sessionCatalog = catalogManager.v1SessionCatalog
    val conf = spark.sessionState.conf
    val catalogName = catalog.paimonCatalogName()
    val catalogAndNamespace = catalogName +: ident.namespace().toSeq

    // A two-part name whose head is a data source is left to ResolveSQLOnFile, also when a database
    // of the view catalog has that name.
    def isFileTable(nameParts: Seq[String]): Boolean =
      nameParts.length == 2 && conf.runSQLonFile && {
        try {
          DataSource.lookupDataSource(nameParts.head, conf)
          true
        } catch {
          case NonFatal(_) => false
        }
      }

    plan.transformUpWithSubqueries {
      case u: UnresolvedRelation =>
        val nameParts = u.multipartIdentifier
        if (sessionCatalog.isTempView(nameParts)) {
          u
        } else if (nameParts.length == 1) {
          u.copy(multipartIdentifier = catalogAndNamespace ++ nameParts)
        } else if (catalogManager.isCatalogRegistered(nameParts.head) || isFileTable(nameParts)) {
          u
        } else {
          u.copy(multipartIdentifier = catalogName +: nameParts)
        }
    }
  }
}
