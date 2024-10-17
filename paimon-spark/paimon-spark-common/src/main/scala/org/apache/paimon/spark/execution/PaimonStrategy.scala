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

import org.apache.paimon.spark.{SparkCatalog, SparkUtils}
import org.apache.paimon.spark.catalyst.plans.logical.{CreateTagCommand, DeleteTagCommand, PaimonCallCommand, RenameTagCommand, ShowTagsCommand}

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, PaimonLookupCatalog, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.shim.PaimonCreateTableAsSelectStrategy

import scala.collection.JavaConverters._

case class PaimonStrategy(spark: SparkSession)
  extends Strategy
  with PredicateHelper
  with PaimonLookupCatalog {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case ctas: CreateTableAsSelect =>
      PaimonCreateTableAsSelectStrategy(spark)(ctas)

    case c @ PaimonCallCommand(procedure, args) =>
      val input = buildInternalRow(args)
      PaimonCallExec(c.output, procedure, input) :: Nil

    case t @ ShowTagsCommand(PaimonCatalogAndIdentifier(catalog, ident)) =>
      ShowTagsExec(catalog, ident, t.output) :: Nil

    case CreateTagCommand(
          PaimonCatalogAndIdentifier(table, ident),
          tagName,
          tagOptions,
          ifNotExists) =>
      CreateTagExec(table, ident, tagName, tagOptions, ifNotExists) :: Nil

    case DeleteTagCommand(PaimonCatalogAndIdentifier(catalog, ident), tagStr, ifExists) =>
      DeleteTagExec(catalog, ident, tagStr, ifExists) :: Nil

    case RenameTagCommand(PaimonCatalogAndIdentifier(catalog, ident), sourceTag, targetTag) =>
      RenameTagExec(catalog, ident, sourceTag, targetTag) :: Nil

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
        case _ =>
          None
      }
    }
  }
}
