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

import org.apache.paimon.catalog.Catalog.ViewNotExistException
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.catalog.SupportView
import org.apache.paimon.view.View

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{CTESubstitution, GetColumnByOrdinal, SubstituteUnresolvedOrdinals, UnresolvedRelation, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, UpCast}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.extensions.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog}

case class PaimonViewResolver(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PaimonLookupCatalog {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u @ UnresolvedRelation(parts @ CatalogAndIdentifier(catalog: SupportView, ident), _, _) =>
      try {
        val view = catalog.loadView(ident)
        createViewRelation(parts, view)
      } catch {
        case _: ViewNotExistException =>
          u
      }

    case u @ UnresolvedTableOrView(CatalogAndIdentifier(catalog: SupportView, ident), _, _) =>
      try {
        catalog.loadView(ident)
        ResolvedPaimonView(catalog, ident)
      } catch {
        case _: ViewNotExistException =>
          u
      }
  }

  private def createViewRelation(nameParts: Seq[String], view: View): LogicalPlan = {
    val parsedPlan =
      parseViewText(nameParts.toArray.mkString("."), view.query(SupportView.DIALECT))

    // Apply early analysis rules that won't re-run for plans injected during Resolution batch.
    val earlyRules = Seq(CTESubstitution, SubstituteUnresolvedOrdinals)
    val rewritten = earlyRules.foldLeft(parsedPlan)((plan, rule) => rule.apply(plan))

    // Spark internally replaces CharType/VarcharType with StringType during V2 table resolution,
    // so the view's schema must also use StringType to avoid UpCast failures
    // (e.g., "Cannot up cast from STRING to VARCHAR(50)").
    val viewSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
      SparkTypeUtils.fromPaimonRowType(view.rowType()))

    val aliases = viewSchema.fields.zipWithIndex.map {
      case (expected, pos) =>
        val attr = GetColumnByOrdinal(pos, expected.dataType)
        Alias(UpCast(attr, expected.dataType), expected.name)(explicitMetadata =
          Some(expected.metadata))
    }

    SubqueryAlias(nameParts, Project(aliases, rewritten))
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    val origin = Origin(
      objectType = Some("VIEW"),
      objectName = Some(name)
    )
    try {
      CurrentOrigin.withOrigin(origin) {
        try {
          spark.sessionState.sqlParser.parseQuery(viewText)
        } catch {
          // For compatibility with Spark 3.2 and below
          case _: NoSuchMethodError =>
            spark.sessionState.sqlParser.parsePlan(viewText)
        }
      }
    } catch {
      case _: ParseException =>
        throw new RuntimeException("Failed to parse view text: " + viewText)
    }
  }
}

case class ResolvedPaimonView(catalog: SupportView, identifier: Identifier) extends LeafNode {
  override def output: Seq[Attribute] = Nil
}
