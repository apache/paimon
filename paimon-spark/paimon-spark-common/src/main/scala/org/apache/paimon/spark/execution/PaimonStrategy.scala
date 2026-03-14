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

package org.apache.paimon.spark.execution

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.{SparkCatalog, SparkGenericCatalog, SparkTable, SparkUtils}
import org.apache.paimon.spark.catalog.{SparkBaseCatalog, SupportView}
import org.apache.paimon.spark.catalyst.analysis.ResolvedPaimonView
import org.apache.paimon.spark.catalyst.plans.logical.{CreateOrReplaceTagCommand, CreatePaimonView, DeleteTagCommand, DropPaimonView, PaimonCallCommand, PaimonDropPartitions, RenameTagCommand, ResolvedIdentifier, ShowPaimonViews, ShowTagsCommand, TruncatePaimonTableWithFilter}
import org.apache.paimon.table.Table

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, DescribeRelation, LogicalPlan, ShowCreateTable}
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.{PaimonDescribeTableExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation}
import org.apache.spark.sql.execution.shim.PaimonCreateTableAsSelectStrategy
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._

case class PaimonStrategy(spark: SparkSession)
  extends SparkStrategy
  with PredicateHelper
  with PaimonLookupCatalog {

  import DataSourceV2Implicits._
  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case ctas: CreateTableAsSelect =>
      PaimonCreateTableAsSelectStrategy(spark)(ctas)

    case c @ PaimonCallCommand(procedure, args) =>
      val input = buildInternalRow(args)
      PaimonCallExec(c.output, procedure, input) :: Nil

    case t @ ShowTagsCommand(PaimonCatalogAndIdentifier(catalog, ident)) =>
      ShowTagsExec(catalog, ident, t.output) :: Nil

    case CreateOrReplaceTagCommand(
          PaimonCatalogAndIdentifier(table, ident),
          tagName,
          tagOptions,
          create,
          replace,
          ifNotExists) =>
      CreateOrReplaceTagExec(table, ident, tagName, tagOptions, create, replace, ifNotExists) :: Nil

    case DeleteTagCommand(PaimonCatalogAndIdentifier(catalog, ident), tagStr, ifExists) =>
      DeleteTagExec(catalog, ident, tagStr, ifExists) :: Nil

    case RenameTagCommand(PaimonCatalogAndIdentifier(catalog, ident), sourceTag, targetTag) =>
      RenameTagExec(catalog, ident, sourceTag, targetTag) :: Nil

    case CreatePaimonView(
          ResolvedIdentifier(viewCatalog: SupportView, ident),
          queryText,
          query,
          columnAliases,
          columnComments,
          queryColumnNames,
          comment,
          properties,
          allowExisting,
          replace
        ) =>
      CreatePaimonViewExec(
        viewCatalog,
        ident,
        queryText,
        query.schema,
        columnAliases,
        columnComments,
        queryColumnNames,
        comment,
        properties,
        allowExisting,
        replace) :: Nil

    case DropPaimonView(ResolvedIdentifier(viewCatalog: SupportView, ident), ifExists) =>
      DropPaimonViewExec(viewCatalog, ident, ifExists) :: Nil

    // A new member was added to ResolvedNamespace since spark4.0,
    // unapply pattern matching is not used here to ensure compatibility across multiple spark versions.
    case ShowPaimonViews(r: ResolvedNamespace, pattern, output)
        if r.catalog.isInstanceOf[SupportView] =>
      ShowPaimonViewsExec(output, r.catalog.asInstanceOf[SupportView], r.namespace, pattern) :: Nil

    case ShowCreateTable(ResolvedPaimonView(viewCatalog, ident), _, output) =>
      ShowCreatePaimonViewExec(output, viewCatalog, ident) :: Nil

    case DescribeRelation(ResolvedPaimonView(viewCatalog, ident), _, isExtended, output) =>
      DescribePaimonViewExec(output, viewCatalog, ident, isExtended) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended, output) =>
      (r.table, r.catalog) match {
        case (sparkTable: SparkTable, sparkCatalog: SparkBaseCatalog) =>
          PaimonDescribeTableExec(
            output,
            sparkCatalog,
            r.identifier,
            sparkTable,
            partitionSpec,
            isExtended) :: Nil
        case _ => Nil
      }

    case PaimonDropPartitions(
          r @ ResolvedTable(_, _, table: SparkTable, _),
          parts,
          ifExists,
          purge) =>
      PaimonDropPartitionsExec(
        table,
        parts.asResolvedPartitionSpecs,
        ifExists,
        purge,
        recacheTable(r)) :: Nil

    case TruncatePaimonTableWithFilter(
          table: Table,
          partitionPredicate: Option[PartitionPredicate]) =>
      TruncatePaimonTableWithFilterExec(table, partitionPredicate) :: Nil

    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private object PaimonCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier =
        SparkUtils.catalogAndIdentifier(spark, identifier.asJava, catalogManager.currentCatalog)
      catalogAndIdentifier.catalog match {
        case paimonCatalog: SparkCatalog =>
          Some((paimonCatalog, catalogAndIdentifier.identifier()))
        case paimonCatalog: SparkGenericCatalog =>
          Some((paimonCatalog, catalogAndIdentifier.identifier()))
        case _ =>
          None
      }
    }
  }

  private def recacheTable(r: ResolvedTable)(): Unit = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    SparkShimLoader.shim.classicApi.recacheByPlan(spark, v2Relation)
  }
}
