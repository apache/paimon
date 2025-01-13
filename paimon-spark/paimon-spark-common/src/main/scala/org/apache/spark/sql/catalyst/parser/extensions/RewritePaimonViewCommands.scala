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

package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.paimon.spark.catalog.SupportView
import org.apache.paimon.spark.catalyst.plans.logical.{CreatePaimonView, DropPaimonView, ResolvedIdentifier, ShowPaimonViews}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{CTESubstitution, ResolvedNamespace, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}

case class RewritePaimonViewCommands(spark: SparkSession)
  extends Rule[LogicalPlan]
  with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {

    // A new member was added to CreatePaimonView since spark4.0,
    // unapply pattern matching is not used here to ensure compatibility across multiple spark versions.
    case c: CreateView =>
      ResolvedIdent
        .unapply(c.child)
        .map {
          resolved =>
            CreatePaimonView(
              child = resolved,
              queryText = c.originalText.get,
              query = CTESubstitution.apply(c.query),
              columnAliases = c.userSpecifiedColumns.map(_._1),
              columnComments = c.userSpecifiedColumns.map(_._2.orElse(None)),
              comment = c.comment,
              properties = c.properties,
              allowExisting = c.allowExisting,
              replace = c.replace
            )
        }
        .getOrElse(c)

    case DropView(ResolvedIdent(resolved), ifExists: Boolean) =>
      DropPaimonView(resolved, ifExists)

    case ShowViews(_, pattern, output) if catalogManager.currentCatalog.isInstanceOf[SupportView] =>
      ShowPaimonViews(
        ResolvedNamespace(catalogManager.currentCatalog, catalogManager.currentNamespace),
        pattern,
        output)
  }

  private object ResolvedIdent {
    def unapply(unresolved: Any): Option[ResolvedIdentifier] = unresolved match {
      case UnresolvedIdentifier(CatalogAndIdentifier(viewCatalog: SupportView, ident), _) =>
        Some(ResolvedIdentifier(viewCatalog, ident))
      case _ =>
        None
    }
  }
}
