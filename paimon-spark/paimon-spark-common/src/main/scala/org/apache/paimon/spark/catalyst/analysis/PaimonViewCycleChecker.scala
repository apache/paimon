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
import org.apache.paimon.spark.catalog.SupportView

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.Locale

import scala.collection.mutable

/** Checks Paimon view dependencies for cycles and excessive nesting. */
private[spark] class PaimonViewCycleChecker(spark: SparkSession) extends PaimonLookupCatalog {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  // Reuse only within one resolution traversal, never across queries or metadata changes.
  private val completedDepths = mutable.Map[ViewReference, Int]()
  private val maxNestedViewDepth = spark.sessionState.conf.maxNestedViewDepth

  def validate(catalog: SupportView, ident: Identifier, queryText: String): Unit = {
    val active = mutable.ArrayBuffer[ViewReference]()

    def visit(
        currentCatalog: SupportView,
        currentIdent: Identifier,
        currentQueryText: => String): Int = {
      val reference = viewReference(currentCatalog, currentIdent)
      val cycleStart = active.indexOf(reference)
      if (cycleStart >= 0) {
        val cycle = (active.slice(cycleStart, active.size) :+ reference)
          .map(_.displayName)
          .mkString(" -> ")
        throw new IllegalArgumentException(s"Recursive Paimon view reference detected: $cycle")
      }

      def checkDepth(depth: Int): Unit = {
        if (active.size + depth > maxNestedViewDepth) {
          val path = (active.toSeq :+ reference).map(_.displayName).mkString(" -> ")
          throw new IllegalArgumentException(
            s"Paimon view depth exceeds spark.sql.view.maxNestedViewDepth " +
              s"($maxNestedViewDepth): $path")
        }
      }

      completedDepths.get(reference) match {
        case Some(depth) =>
          // A shared dependency may be reached along a deeper path after its first validation.
          checkDepth(depth)
          depth
        case None =>
          // Load first so that a table at the depth limit is not counted as another view.
          val query = currentQueryText
          checkDepth(1)
          active += reference
          try {
            val childDepth = viewDependencies(query).foldLeft(0) {
              case (depth, (dependencyCatalog, dependencyIdent)) =>
                val dependencyDepth =
                  try {
                    visit(
                      dependencyCatalog,
                      dependencyIdent,
                      dependencyCatalog.loadView(dependencyIdent).query(SupportView.DIALECT))
                  } catch {
                    case _: ViewNotExistException => 0
                  }
                math.max(depth, dependencyDepth)
            }
            val depth = childDepth + 1
            completedDepths(reference) = depth
            depth
          } finally {
            active.remove(active.size - 1)
          }
      }
    }

    visit(catalog, ident, queryText)
  }

  private def viewDependencies(queryText: String): Seq[(SupportView, Identifier)] = {
    val parsedPlan = PaimonUtils.parseQueryCompat(spark.sessionState.sqlParser, queryText)
    val earlyRules = SparkShimLoader.shim.earlyBatchRules()
    val rewritten = earlyRules.foldLeft(parsedPlan)((plan, rule) => rule.apply(plan))
    val dependencies = mutable.ArrayBuffer[(SupportView, Identifier)]()

    def collectDependencies(plan: LogicalPlan): Unit = {
      plan.foreach {
        node =>
          node match {
            case UnresolvedRelation(
                  CatalogAndIdentifier(dependencyCatalog: SupportView, dependencyIdent),
                  _,
                  _) =>
              dependencies += ((dependencyCatalog, dependencyIdent))
            case _ =>
          }
          node.expressions.foreach {
            _.collect { case subquery: SubqueryExpression => subquery.plan }
              .foreach(collectDependencies)
          }
      }
    }

    collectDependencies(rewritten)
    dependencies.toSeq
  }

  private def viewReference(catalog: SupportView, ident: Identifier): ViewReference = {
    def normalize(name: String): String = {
      if (catalog.paimonCatalog().caseSensitive()) name else name.toLowerCase(Locale.ROOT)
    }
    ViewReference(
      catalog.paimonCatalogName(),
      ident.namespace().toSeq.map(normalize),
      normalize(ident.name()))
  }

  private case class ViewReference(catalogName: String, namespace: Seq[String], name: String) {
    def displayName: String = (catalogName +: namespace :+ name).mkString(".")
  }
}
