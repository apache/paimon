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

package org.apache.paimon.spark.commands

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.SparkSource

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.command.LeafRunnableCommand

import java.util.Locale

import scala.collection.JavaConverters._

case class PaimonCreateTableLikeCommand(
    targetCatalog: TableCatalog,
    targetIdent: Identifier,
    sourceCatalog: TableCatalog,
    sourceIdent: Identifier,
    provider: Option[String],
    location: Option[String],
    properties: Map[String, String] = Map.empty,
    ifNotExists: Boolean)
  extends LeafRunnableCommand
  with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceTable = sourceCatalog.loadTable(sourceIdent)
    val sourceProperties = sourceTable.properties().asScala.toMap
    val sourceProvider =
      normalizedProvider(sourceProperties.get(TableCatalog.PROP_PROVIDER), unknownProvider)
    val targetProvider = normalizedProvider(
      provider.orElse(sourceProperties.get(TableCatalog.PROP_PROVIDER)),
      defaultTargetProvider)
    val sourceSchema =
      CharVarcharUtils.getRawSchema(sourceTable.schema(), sparkSession.sessionState.conf)
    try {
      targetCatalog.createTable(
        targetIdent,
        sourceSchema,
        sourceTable.partitioning(),
        buildCreateProperties(sourceProperties, sourceProvider, targetProvider).asJava)
    } catch {
      case _: TableAlreadyExistsException if ifNotExists =>
    }

    Seq.empty
  }

  private def buildCreateProperties(
      sourceProperties: Map[String, String],
      sourceProvider: String,
      targetProvider: String): Map[String, String] = {
    copySourceProperties(sourceProperties, sourceProvider, targetProvider) ++
      Map(TableCatalog.PROP_PROVIDER -> targetProvider) ++
      location.map(TableCatalog.PROP_LOCATION -> _).toMap ++
      properties
  }

  private def copySourceProperties(
      sourceProperties: Map[String, String],
      sourceProvider: String,
      targetProvider: String): Map[String, String] = {
    val copiedComment = sourceProperties
      .get(TableCatalog.PROP_COMMENT)
      .map(TableCatalog.PROP_COMMENT -> _)
      .toMap
    val copiedProperties = filterCopiedProperties(sourceProperties)

    if (sourceProvider == targetProvider) {
      copiedProperties ++ copiedComment
    } else {
      warnSkippedProperties(sourceProvider, targetProvider, copiedProperties.keySet)
      copiedComment
    }
  }

  private def warnSkippedProperties(
      sourceProvider: String,
      targetProvider: String,
      skippedKeys: Set[String]): Unit = {
    if (skippedKeys.nonEmpty) {
      logWarning(
        s"Skip copying source table properties in CREATE TABLE LIKE because source provider " +
          s"'$sourceProvider' differs from target provider " +
          s"'$targetProvider': " +
          skippedKeys.toSeq.sorted.mkString(","))
    }
  }

  private def normalizedProvider(provider: Option[String], defaultProvider: String): String = {
    provider.map(normalizeProvider).getOrElse(defaultProvider)
  }

  private def normalizeProvider(provider: String): String = {
    provider.toLowerCase(Locale.ROOT)
  }

  private val unknownProvider = "unknown"

  private val defaultTargetProvider = normalizeProvider(SparkSource.NAME)

  private def filterCopiedProperties(sourceProperties: Map[String, String]): Map[String, String] = {
    sourceProperties.filterNot {
      case (key, _) if key == CoreOptions.PATH.key() => true
      case (TableCatalog.PROP_PROVIDER, _) => true
      case (TableCatalog.PROP_COMMENT, _) => true
      case (TableCatalog.PROP_LOCATION, _) => true
      case (TableCatalog.PROP_OWNER, _) => true
      case (TableCatalog.PROP_EXTERNAL, _) => true
      case (TableCatalog.PROP_IS_MANAGED_LOCATION, _) => true
      case _ => false
    }
  }
}
