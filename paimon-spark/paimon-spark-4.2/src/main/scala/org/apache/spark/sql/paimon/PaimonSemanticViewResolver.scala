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

package org.apache.spark.sql.paimon

import org.apache.paimon.catalog.{DelegateCatalog, Identifier => PaimonIdentifier}
import org.apache.paimon.rest.RESTCatalog
import org.apache.paimon.rest.exceptions.NoSuchResourceException
import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.ossie.OssieModel
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.view.SemanticViewDefinition

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Join, LateralJoin, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.PaimonLookupCatalog
import org.apache.spark.sql.execution.command.MetricViewHelper
import org.apache.spark.sql.metricview.util.MetricViewPlanner

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Resolve REST semantic definitions through Spark's Metric View engine without persisting a copy. */
class PaimonSemanticViewResolver(spark: SparkSession)
  extends Rule[LogicalPlan]
  with PaimonLookupCatalog {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val definitions = mutable.Map[(RESTCatalog, PaimonIdentifier), Option[SemanticViewDefinition]]()
    plan.resolveOperatorsDown {
      case u @ UnresolvedRelation(
            parts @ CatalogAndIdentifier(catalog: SparkCatalog, ident),
            _,
            _) =>
        DelegateCatalog.rootCatalog(catalog.paimonCatalog()) match {
          // An ordinary view expanded by the preceding rule may introduce unresolved source
          // tables after Spark's relation resolution has run. Leave those for the next iteration.
          case rest: RESTCatalog if !catalog.tableExists(ident) =>
            val id = PaimonIdentifier.create(ident.namespace().head, ident.name())
            val definition = definitions.getOrElseUpdate(
              (rest, id),
              try {
                Some(
                  rest
                    .semanticViewManagement()
                    .getSemanticView(id)
                    .getDefinition)
              } catch {
                case _: NoSuchResourceException => None
              })
            definition
              .map {
                definition =>
                  require(!u.isStreaming, "Ossie Semantic Views support batch queries only")
                  require(
                    u.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES) == null,
                    "Ossie Semantic Views are read-only")
                  require(
                    !isJoined(plan, u),
                    "Joins against Ossie Semantic Views are not supported yet")
                  val model = OssieModel.parse(definition)
                  val source =
                    spark.sessionState.sqlParser.parseMultipartIdentifier(model.source) match {
                      case Seq(table) => Seq(catalog.name(), ident.namespace().head, table)
                      case Seq(db, table) => Seq(catalog.name(), db, table)
                      case Seq(cat, db, table) if cat == catalog.name() => Seq(cat, db, table)
                      case _ =>
                        throw new IllegalArgumentException(
                          "Ossie source must be a table in the same Paimon catalog")
                    }
                  val table =
                    catalog.paimonCatalog().getTable(PaimonIdentifier.create(source(1), source(2)))
                  require(
                    table.isInstanceOf[FileStoreTable],
                    "Ossie source must be a physical Paimon table")
                  val yaml = OssieToSparkMetricView.convert(
                    spark,
                    model,
                    source,
                    table.rowType().getFieldNames.asScala.toSeq)
                  val (analyzed, _) = MetricViewHelper.analyzeMetricViewText(spark, parts, yaml)
                  val metadata = CatalogTable(
                    identifier = TableIdentifier(
                      ident.name(),
                      Some(ident.namespace().head),
                      Some(catalog.name())),
                    tableType = CatalogTableType.VIEW,
                    storage = CatalogStorageFormat.empty,
                    schema = analyzed.schema,
                    viewText = Some(yaml)
                  )
                  SubqueryAlias(
                    parts,
                    MetricViewPlanner
                      .planRead(metadata, yaml, spark.sessionState.sqlParser, analyzed.schema))
              }
              .getOrElse(u)
          case _ => u
        }
    }
  }

  private def isJoined(plan: LogicalPlan, relation: UnresolvedRelation): Boolean = {
    val ctes = plan.collectWithSubqueries { case cte: CTERelationDef => cte }
    val semanticCtes = mutable.Set.empty[Long]
    def dependsOnSemantic(node: LogicalPlan): Boolean =
      node.collectFirstWithSubqueries {
        case n if n eq relation => true
        case ref: CTERelationRef if semanticCtes.contains(ref.cteId) => true
      }.isDefined

    // CTE definitions are siblings of their consumers, and can reference other CTEs. Resolve
    // those dependencies before inspecting joins, including joins inside scalar subqueries.
    var changed = true
    while (changed) {
      val previousSize = semanticCtes.size
      ctes.foreach(cte => if (dependsOnSemantic(cte.child)) { semanticCtes += cte.id })
      changed = semanticCtes.size != previousSize
    }
    plan.collectFirstWithSubqueries {
      case join: Join if dependsOnSemantic(join) => true
      case join: LateralJoin if dependsOnSemantic(join) => true
    }.isDefined
  }
}
