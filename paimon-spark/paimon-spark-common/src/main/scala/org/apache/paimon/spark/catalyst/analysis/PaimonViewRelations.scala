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
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions
import org.apache.paimon.view.View

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedWith}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Binds the relations of a Paimon view body to the view's catalog and namespace. */
object PaimonViewRelations {

  /**
   * View option that makes the body resolve in the view's catalog and namespace. Views without it,
   * such as those created by earlier versions, resolve in the current namespace.
   */
  val RESOLVE_IN_VIEW_NAMESPACE = "spark.view.resolve-in-view-namespace"

  /**
   * The properties `CREATE VIEW` stores: unless the statement sets [[RESOLVE_IN_VIEW_NAMESPACE]],
   * it is `true`, or `false` for a body that [[qualify]] leaves unchanged, so that reads bind it as
   * it was bound at creation.
   */
  def newViewProperties(
      properties: Map[String, String],
      query: LogicalPlan): Map[String, String] = {
    if (properties.contains(RESOLVE_IN_VIEW_NAMESPACE)) {
      properties
    } else {
      properties + (RESOLVE_IN_VIEW_NAMESPACE -> (!hasDeferredNames(query)).toString)
    }
  }

  def viewOptions(view: View): Map[String, String] =
    Option(view.options()).map(_.asScala.toMap).getOrElse(Map.empty)

  // The name is a FunctionIdentifier on Spark 3.2 and 3.3 and a Seq[String] since 3.4.
  private def isPaimonFunction(f: UnresolvedTableValuedFunction): Boolean = {
    val name = f.productElement(0) match {
      case id: FunctionIdentifier => id.funcName
      case parts: Seq[_] => parts.lastOption.map(_.toString).getOrElse("")
      case other => String.valueOf(other)
    }
    PaimonTableValuedFunctions.supportedFnNames.exists(_.equalsIgnoreCase(name))
  }

  // Names that become relations later in analysis.
  private def hasDeferredNames(plan: LogicalPlan): Boolean = {
    plan.collectWithSubqueries {
      case w: UnresolvedWith => w
      // Spark 3.5+; matched by name so that this links on older versions.
      case p if p.nodeName == "PlanWithUnresolvedIdentifier" => p
      case f: UnresolvedTableValuedFunction if isPaimonFunction(f) => f
    }.nonEmpty
  }

  // The sources ResolveSQLOnFile reads files with: file sources other than hive.
  private def isFileSource(name: String, conf: SQLConf): Boolean =
    !name.equalsIgnoreCase("hive") && {
      try {
        val cls = DataSource.lookupDataSource(name, conf)
        classOf[FileFormat].isAssignableFrom(cls) || classOf[FileDataSourceV2].isAssignableFrom(cls)
      } catch {
        case NonFatal(_) => false
      }
    }

  private def isNotFound(e: Throwable): Boolean = e match {
    case _: NoSuchTableException | _: NoSuchNamespaceException | _: NoSuchDatabaseException |
        _: ViewNotExistException =>
      true
    case _ => false
  }

  // `absent` says which lookup errors mean that there is no such relation.
  private def found(lookup: => Any, absent: Throwable => Boolean): Boolean = {
    try {
      lookup
      true
    } catch {
      case NonFatal(e) => !absent(e)
    }
  }

  private def hasTable(
      plugin: AnyRef,
      relation: Identifier,
      absent: Throwable => Boolean): Boolean =
    plugin match {
      case tables: TableCatalog => found(tables.loadTable(relation), absent)
      case _ => false
    }

  private def hasView(plugin: AnyRef, relation: Identifier, absent: Throwable => Boolean): Boolean =
    plugin match {
      case views: SupportView => found(views.loadView(relation), absent)
      case _ => false
    }

  private def hasRelation(
      plugin: AnyRef,
      relation: Identifier,
      absent: Throwable => Boolean): Boolean =
    hasTable(plugin, relation, absent) || hasView(plugin, relation, absent)

  // While it resolves a Spark view, Spark reads only the temporary views that view referred to.
  // Since Spark 4.2 `session.v` and `system.session.v` name the temporary view `v`.
  private def isTempView(spark: SparkSession, nameParts: Seq[String]): Boolean = {
    val context = AnalysisContext.get
    // Inside a Spark view this is the conf the view stored, as Spark uses.
    val resolver = SQLConf.get.resolver
    val viewName = nameParts match {
      case Seq(s, v) if s.equalsIgnoreCase("session") => Seq(v)
      case Seq(c, s, v) if c.equalsIgnoreCase("system") && s.equalsIgnoreCase("session") => Seq(v)
      case _ => nameParts
    }
    def sameName(a: Seq[String], b: Seq[String]): Boolean =
      a.length == b.length && a.zip(b).forall { case (x, y) => resolver(x, y) }
    spark.sessionState.catalogManager.v1SessionCatalog.isTempView(nameParts) &&
    (context.catalogAndNamespace.isEmpty || context.referredTempViewNames.exists {
      name => sameName(name, nameParts) || sameName(name, viewName)
    })
  }

  /**
   * When `options` sets [[RESOLVE_IN_VIEW_NAMESPACE]], qualifies the unresolved relations of a view
   * body after CTE substitution: a single name gets the view's catalog and namespace, and a
   * multi-part name that does not start with a catalog gets the view's catalog. Temporary views
   * Spark would read keep their names, and so does `format.`path`` where it reads a file.
   *
   * A body with names that become relations later in analysis is left unchanged, so that they do
   * not bind partly here and partly there: a CTE left by the LEGACY CTE policy, an unfolded
   * `IDENTIFIER(...)`, and the string table names of Paimon's table-valued functions.
   */
  def qualify(
      spark: SparkSession,
      plan: LogicalPlan,
      catalog: SupportView,
      ident: Identifier,
      options: Map[String, String]): LogicalPlan = {
    if (!options.get(RESOLVE_IN_VIEW_NAMESPACE).exists("true".equalsIgnoreCase)) {
      return plan
    }
    if (hasDeferredNames(plan)) {
      return plan
    }

    val catalogManager = spark.sessionState.catalogManager
    val conf = spark.sessionState.conf
    val catalogName = catalog.paimonCatalogName()
    val catalogAndNamespace = catalogName +: ident.namespace().toSeq

    // A name left as it is can bind a Paimon view of the current catalog, through
    // PaimonViewResolver, or a table of the catalog Spark looks it up in: that of the Spark view
    // being resolved, if any, and the current catalog otherwise. Otherwise Spark reads a file. So
    // the name is left only if neither, nor the view's catalog, has a relation so named. In the
    // view's catalog any failed lookup means none, as in SparkCatalog.loadSparkTable for a file
    // source namespace; elsewhere only a not found error does, as in CatalogV2Util.loadTable.
    def isFileTable(nameParts: Seq[String]): Boolean =
      nameParts.length == 2 && conf.runSQLonFile && isFileSource(nameParts.head, conf) && {
        val relation = Identifier.of(Array(nameParts.head), nameParts.last)
        val current = catalogManager.currentCatalog
        val lookupCatalog =
          AnalysisContext.get.catalogAndNamespace.headOption.getOrElse(current.name())
        !hasRelation(catalog, relation, _ => true) &&
        (lookupCatalog == catalogName || !catalogManager.isCatalogRegistered(lookupCatalog) ||
          !hasTable(catalogManager.catalog(lookupCatalog), relation, isNotFound)) &&
        (current.name() == catalogName || !hasView(current, relation, isNotFound))
      }

    plan.transformUpWithSubqueries {
      case u: UnresolvedRelation =>
        val nameParts = u.multipartIdentifier
        if (isTempView(spark, nameParts)) {
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
