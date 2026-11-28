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

import org.apache.paimon.spark.{SparkCatalog, SparkGenericCatalog}
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.catalyst.plans.logical.PaimonCreateTableLikeStatement
import org.apache.paimon.spark.commands.PaimonCreateTableLikeCommand

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, CatalogV2Util, Identifier, LookupCatalog, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand => SparkCreateTableLikeCommand}

case class RewriteCreateTableLikeCommand(spark: SparkSession)
  extends Rule[LogicalPlan]
  with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager
  private lazy val gteqSpark3_4: Boolean = org.apache.spark.SPARK_VERSION >= "3.4"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!gteqSpark3_4) {
      return plan
    }

    plan.resolveOperatorsUp {
      case c: SparkCreateTableLikeCommand =>
        rewrite(
          toMultipartIdentifier(c.targetTable),
          toMultipartIdentifier(c.sourceTable),
          c.provider,
          c.fileFormat,
          c.properties,
          c.ifNotExists).getOrElse(c)
      case PaimonCreateTableLikeStatement(targetParts, sourceParts, c) =>
        rewrite(targetParts, sourceParts, c.provider, c.fileFormat, c.properties, c.ifNotExists)
          .getOrElse(sessionCatalogCommand(targetParts, sourceParts, c))
    }
  }

  /**
   * The Paimon command for a CREATE TABLE LIKE whose target is a Paimon table, or None to leave the
   * statement to Spark. `fileFormat` is evaluated only when the target is in `SparkCatalog`, or in
   * `SparkGenericCatalog` with a Paimon provider.
   */
  private[extensions] def rewrite(
      targetParts: Seq[String],
      sourceParts: Seq[String],
      provider: Option[String],
      fileFormat: => CatalogStorageFormat,
      properties: Map[String, String],
      ifNotExists: Boolean): Option[LogicalPlan] = {
    val target: Option[(SparkBaseCatalog, Identifier)] = targetParts match {
      case CatalogAndTableIdentifier(targetCatalog: SparkCatalog, targetIdent) =>
        if (usesHiveStorageSyntax(fileFormat)) {
          throw new UnsupportedOperationException(
            "CREATE TABLE LIKE ... STORED AS is not supported for SparkCatalog.")
        }
        Some((targetCatalog, targetIdent))
      case CatalogAndTableIdentifier(targetCatalog: SparkGenericCatalog, targetIdent)
          if provider.exists(SparkBaseCatalog.usePaimon) && !usesHiveStorageSyntax(fileFormat) =>
        Some((targetCatalog, targetIdent))
      case _ => None
    }
    target.map {
      case (targetCatalog, targetIdent) =>
        createTableLikeCommand(
          targetCatalog,
          targetIdent,
          sourceParts,
          provider,
          fileFormat.locationUri.map(_.toString),
          properties,
          ifNotExists)
    }
  }

  private def createTableLikeCommand(
      targetCatalog: SparkBaseCatalog,
      targetIdent: Identifier,
      sourceParts: Seq[String],
      provider: Option[String],
      location: Option[String],
      properties: Map[String, String],
      ifNotExists: Boolean): LogicalPlan = {
    if (spark.sessionState.catalog.isTempView(sourceParts)) {
      throw new UnsupportedOperationException(
        "CREATE TABLE LIKE with a temporary view as the source is not supported for a Paimon " +
          s"table: ${sourceParts.quoted}.")
    }
    sourceParts match {
      case CatalogAndTableIdentifier(sourceCatalog: TableCatalog, sourceIdent) =>
        PaimonCreateTableLikeCommand(
          targetCatalog,
          targetIdent,
          sourceCatalog,
          sourceIdent,
          provider,
          location,
          properties,
          ifNotExists
        )
      case _ =>
        throw new UnsupportedOperationException(
          s"CREATE TABLE LIKE source table must be resolved from TableCatalog: " +
            s"${sourceParts.quoted}.")
    }
  }

  /**
   * [[CatalogAndIdentifier]], except that when a name of more than three parts resolves to a Paimon
   * catalog, the parts between the first and the last are joined into one namespace part. Other
   * catalogs keep every namespace part.
   */
  private object CatalogAndTableIdentifier {
    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(_: SparkBaseCatalog, _) if parts.length > 3 =>
        CatalogAndIdentifier.unapply(
          Seq(parts.head, parts.slice(1, parts.length - 1).mkString("."), parts.last))
      case CatalogAndIdentifier(catalog, ident) => Some((catalog, ident))
      case _ => None
    }
  }

  /**
   * Spark's command for a statement only Paimon's grammar accepted and whose target is not a Paimon
   * table. That command resolves names in the session catalog by database and table and ignores the
   * catalog name, so the target, and a source that is not a temporary view, must be such names.
   */
  private def sessionCatalogCommand(
      targetParts: Seq[String],
      sourceParts: Seq[String],
      command: SparkCreateTableLikeCommand): LogicalPlan = {
    val sourceTable = if (spark.sessionState.catalog.isTempView(sourceParts)) {
      Some(TableIdentifier(sourceParts.last, sourceParts.init.lastOption))
    } else {
      sessionCatalogTable(sourceParts)
    }
    (sessionCatalogTable(targetParts), sourceTable) match {
      case (Some(target), Some(source)) => command.copy(targetTable = target, sourceTable = source)
      case _ =>
        throw new UnsupportedOperationException(
          s"CREATE TABLE ${targetParts.quoted} LIKE ${sourceParts.quoted} is not supported: " +
            "the target is not a Paimon table, and Spark's command for it needs the target, and " +
            "a source that is not a temporary view, to be a database and a table in the " +
            "session catalog.")
    }
  }

  private def sessionCatalogTable(parts: Seq[String]): Option[TableIdentifier] = parts match {
    case CatalogAndIdentifier(catalog, ident)
        if CatalogV2Util.isSessionCatalog(catalog) && ident.namespace().length == 1 =>
      Some(TableIdentifier(ident.name(), Some(ident.namespace().head)))
    case _ => None
  }

  private def usesHiveStorageSyntax(fileFormat: CatalogStorageFormat): Boolean = {
    fileFormat.inputFormat.isDefined
  }

  private def toMultipartIdentifier(
      targetTable: org.apache.spark.sql.catalyst.TableIdentifier): Seq[String] = {
    (targetTable.catalog.toSeq ++ targetTable.database.toSeq :+ targetTable.table).toList
  }
}
