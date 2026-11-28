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

import java.lang.reflect.{InvocationTargetException, Method}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

/** Binds the relations of a Paimon view body to the view's catalog and namespace. */
object PaimonViewRelations {

  /**
   * View option that makes the body resolve in the view's catalog and namespace. Views without it,
   * such as those created by earlier versions, resolve in the current namespace.
   */
  val RESOLVE_IN_VIEW_NAMESPACE = "spark.view.resolve-in-view-namespace"

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

  // Spark's ViewCatalog exists since Spark 3.4, so it is loaded by name.
  private lazy val viewCatalogClass: Option[Class[_]] =
    Try(Class.forName("org.apache.spark.sql.connector.catalog.ViewCatalog")).toOption

  private lazy val loadViewMethod: Option[Method] =
    viewCatalogClass.map(_.getMethod("loadView", classOf[Identifier]))

  private lazy val noSuchViewClass: Option[Class[_]] =
    Try(Class.forName("org.apache.spark.sql.catalyst.analysis.NoSuchViewException")).toOption

  private def hasSparkView(plugin: AnyRef, relation: Identifier): Boolean =
    viewCatalogClass.filter(_.isInstance(plugin)).flatMap(_ => loadViewMethod).exists {
      loadView =>
        found(
          try {
            loadView.invoke(plugin, relation)
          } catch {
            case e: InvocationTargetException => throw Option(e.getCause).getOrElse(e)
          },
          e => isNotFound(e) || noSuchViewClass.exists(_.isInstance(e))
        )
    }

  private def hasRelation(
      plugin: AnyRef,
      relation: Identifier,
      absent: Throwable => Boolean): Boolean =
    hasTable(plugin, relation, absent) || hasView(plugin, relation, absent)

  /**
   * When `options` sets [[RESOLVE_IN_VIEW_NAMESPACE]], qualifies the unresolved relations of a view
   * body after CTE substitution: a single name gets the view's catalog and namespace, and a
   * multi-part name that does not start with a catalog gets the view's catalog. As for Spark's
   * persistent views, temporary views are not read. `format.`path`` keeps its name where Spark
   * reads a file.
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
    if (!resolvesInViewNamespace(options)) {
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
    // PaimonViewResolver, or a table or Spark view (since Spark 4.2, or through extensions) of
    // the catalog Spark looks it up in: that of the Spark view being resolved, if any, and the
    // current catalog otherwise. Otherwise Spark reads a file. So the name is left only if none
    // of these, nor the view's catalog, has a relation so named. In the view's catalog any failed
    // lookup means none, as in SparkCatalog.loadSparkTable for a file source namespace; elsewhere
    // only not found does.
    def isFileTable(nameParts: Seq[String]): Boolean =
      nameParts.length == 2 && conf.runSQLonFile && isFileSource(nameParts.head, conf) && {
        val relation = Identifier.of(Array(nameParts.head), nameParts.last)
        val current = catalogManager.currentCatalog
        val lookupCatalog =
          AnalysisContext.get.catalogAndNamespace.headOption.getOrElse(current.name())
        !hasRelation(catalog, relation, _ => true) &&
        (lookupCatalog == catalogName || !catalogManager.isCatalogRegistered(lookupCatalog) || {
          val lookup = catalogManager.catalog(lookupCatalog)
          !hasTable(lookup, relation, isNotFound) && !hasSparkView(lookup, relation)
        }) &&
        (current.name() == catalogName || !hasView(current, relation, isNotFound))
      }

    plan.transformUpWithSubqueries {
      case u: UnresolvedRelation =>
        val nameParts = u.multipartIdentifier
        if (nameParts.length == 1) {
          u.copy(multipartIdentifier = catalogAndNamespace ++ nameParts)
        } else if (catalogManager.isCatalogRegistered(nameParts.head) || isFileTable(nameParts)) {
          u
        } else {
          u.copy(multipartIdentifier = catalogName +: nameParts)
        }
    }
  }

  /**
   * The query and properties `CREATE VIEW` stores. Unless the statement sets
   * [[RESOLVE_IN_VIEW_NAMESPACE]], it is `true`, or `false` for a body that [[qualify]] leaves
   * unchanged, so that reads bind it as it was bound at creation. A view with `true` cannot read a
   * temporary view, as its reads would not see it; with `allowTempViews`, such a body gets `false`.
   */
  def qualifyNewView(
      spark: SparkSession,
      query: LogicalPlan,
      catalog: SupportView,
      ident: Identifier,
      properties: Map[String, String],
      allowTempViews: Boolean = false): (LogicalPlan, Map[String, String]) = {
    val sessionCatalog = spark.sessionState.catalogManager.v1SessionCatalog
    val tempView = query.collectWithSubqueries {
      case u: UnresolvedRelation if sessionCatalog.isTempView(u.multipartIdentifier) =>
        u.multipartIdentifier
    }.headOption
    val viewProperties = if (properties.contains(RESOLVE_IN_VIEW_NAMESPACE)) {
      properties
    } else {
      val resolveInViewNamespace =
        !hasDeferredNames(query) && !(allowTempViews && tempView.isDefined)
      properties + (RESOLVE_IN_VIEW_NAMESPACE -> resolveInViewNamespace.toString)
    }
    if (resolvesInViewNamespace(viewProperties) && !hasDeferredNames(query)) {
      tempView.foreach {
        name =>
          val viewName = (catalog.paimonCatalogName() +: ident.namespace() :+ ident.name())
            .mkString(".")
          throw new UnsupportedOperationException(
            s"View $viewName cannot read the temporary view ${name.mkString(".")}, as it " +
              s"resolves its tables in its own namespace. Set '$RESOLVE_IN_VIEW_NAMESPACE' = " +
              "'false' in TBLPROPERTIES to resolve them in the namespace of each read.")
      }
    }
    (qualify(spark, query, catalog, ident, viewProperties), viewProperties)
  }

  private def resolvesInViewNamespace(options: Map[String, String]): Boolean =
    options.get(RESOLVE_IN_VIEW_NAMESPACE).exists("true".equalsIgnoreCase)
}
