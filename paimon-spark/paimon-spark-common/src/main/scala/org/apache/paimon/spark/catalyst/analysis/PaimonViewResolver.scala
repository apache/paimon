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

import org.apache.paimon.catalog.{Identifier => PaimonIdentifier}
import org.apache.paimon.catalog.Catalog.ViewNotExistException
import org.apache.paimon.rest.{RESTCatalogOptions, RESTReadVia}
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.catalog.SupportView
import org.apache.paimon.spark.utils.CatalogUtils.toIdentifier
import org.apache.paimon.view.View

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedRelation, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, UpCast}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.extensions.{CurrentOrigin, Origin}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

case class PaimonViewResolver(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PaimonLookupCatalog {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u @ UnresolvedRelation(parts @ CatalogAndIdentifier(catalog: SupportView, ident), _, _) =>
      val (viewIdent, inheritedRoot) = realViewIdentifier(catalog, ident)
      try {
        val view = catalog.loadView(viewIdent)
        val realParts = parts.dropRight(1) :+ viewIdent.name()
        createViewRelation(realParts, catalog, viewIdent, inheritedRoot, view)
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

  private def createViewRelation(
      nameParts: Seq[String],
      catalog: SupportView,
      viewIdent: Identifier,
      inheritedRoot: Option[PaimonIdentifier],
      view: View): LogicalPlan = {
    val parsedPlan =
      parseViewText(nameParts.toArray.mkString("."), view.query(SupportView.DIALECT))

    // Apply early analysis rules that won't re-run for plans injected during Resolution batch.
    val earlyRules = SparkShimLoader.shim.earlyBatchRules()
    val earlyRewritten = earlyRules.foldLeft(parsedPlan)((plan, rule) => rule.apply(plan))
    val rewritten = if (readViaEnabled(catalog)) {
      val readRoot = inheritedRoot.getOrElse(toIdentifier(viewIdent, catalog.paimonCatalogName()))
      markRelations(earlyRewritten, catalog, readRoot)
    } else {
      earlyRewritten
    }

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

  private def realViewIdentifier(
      catalog: SupportView,
      identifier: Identifier): (Identifier, Option[PaimonIdentifier]) = {
    if (!readViaEnabled(catalog)) {
      return (identifier, None)
    }

    val parsed = RESTReadVia.parse(toIdentifier(identifier, catalog.paimonCatalogName()))
    val realIdentifier = Identifier.of(identifier.namespace(), parsed.identifier().getObjectName)
    val inheritedRoot = if (parsed.readVia().isPresent) {
      Some(parsed.readVia().get())
    } else {
      None
    }
    (realIdentifier, inheritedRoot)
  }

  private def markRelations(
      plan: LogicalPlan,
      viewCatalog: SupportView,
      readRoot: PaimonIdentifier): LogicalPlan = {
    plan.resolveOperators {
      case relation @ UnresolvedRelation(
            parts @ CatalogAndIdentifier(referenceCatalog: SupportView, identifier),
            _,
            _) =>
        if (referenceCatalog ne viewCatalog) {
          throw new IllegalArgumentException(
            s"REST read-via does not support references across Paimon catalogs: " +
              s"${viewCatalog.paimonCatalogName()} -> ${referenceCatalog.paimonCatalogName()}")
        }
        val marked = RESTReadVia.withReadVia(
          toIdentifier(identifier, referenceCatalog.paimonCatalogName()),
          readRoot)
        relation.copy(multipartIdentifier = parts.dropRight(1) :+ marked.getObjectName)
    }
  }

  private def readViaEnabled(catalog: SupportView): Boolean = {
    java.lang.Boolean.parseBoolean(
      catalog
        .paimonCatalog()
        .options()
        .getOrDefault(RESTCatalogOptions.READ_VIA_ENABLED.key(), "false"))
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    val origin = Origin(
      objectType = Some("VIEW"),
      objectName = Some(name)
    )
    try {
      CurrentOrigin.withOrigin(origin) {
        PaimonUtils.parseQueryCompat(spark.sessionState.sqlParser, viewText)
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
