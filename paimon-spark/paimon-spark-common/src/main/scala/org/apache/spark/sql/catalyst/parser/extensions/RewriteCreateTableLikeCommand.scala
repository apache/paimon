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
import org.apache.paimon.spark.commands.PaimonCreateTableLikeCommand

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, LookupCatalog, TableCatalog}
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
    }
  }

  /**
   * The Paimon command for a CREATE TABLE LIKE whose target is a Paimon table, or None to leave the
   * statement to Spark. `fileFormat` is evaluated only for a Paimon target.
   */
  private[extensions] def rewrite(
      targetParts: Seq[String],
      sourceParts: Seq[String],
      provider: Option[String],
      fileFormat: => CatalogStorageFormat,
      properties: Map[String, String],
      ifNotExists: Boolean): Option[LogicalPlan] = {
    val target: Option[(SparkBaseCatalog, Identifier)] = targetParts match {
      case CatalogAndIdentifier(targetCatalog: SparkCatalog, targetIdent) =>
        if (usesHiveStorageSyntax(fileFormat)) {
          throw new UnsupportedOperationException(
            "CREATE TABLE LIKE ... STORED AS is not supported for SparkCatalog.")
        }
        Some((targetCatalog, targetIdent))
      case CatalogAndIdentifier(targetCatalog: SparkGenericCatalog, targetIdent)
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
    sourceParts match {
      case CatalogAndIdentifier(sourceCatalog: TableCatalog, sourceIdent) =>
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

  private def usesHiveStorageSyntax(fileFormat: CatalogStorageFormat): Boolean = {
    fileFormat.inputFormat.isDefined
  }

  private def toMultipartIdentifier(
      targetTable: org.apache.spark.sql.catalyst.TableIdentifier): Seq[String] = {
    (targetTable.catalog.toSeq ++ targetTable.database.toSeq :+ targetTable.table).toList
  }
}
