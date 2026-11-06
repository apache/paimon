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

import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.catalyst.plans.logical.{CreateUserDefinedFunction, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, SupportsNamespaces}
import org.apache.spark.sql.paimon.shims.SparkShimLoader

/**
 * Parser-stage rule that rewrites a Paimon-catalog CREATE FUNCTION ... RETURN
 * (CreateUserDefinedFunction) into CreatePaimonSQLFunctionCommand before Spark's
 * ResolveSessionCatalog rejects it. Only does plan rewriting; analysis, validation, and derivation
 * happen in CreatePaimonSQLFunctionCommand.run().
 */
case class RewritePaimonSQLFunctionCommands(spark: SparkSession) extends Rule[LogicalPlan] {

  private lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  private lazy val lookup = PaimonFunctionLookup(catalogManager)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!OptionUtils.v1FunctionEnabled()) {
      return plan
    }

    plan.resolveOperatorsUp {
      case c: CreateUserDefinedFunction =>
        lookup.CatalogAndFunctionIdentifier.unapply(c.child) match {
          case Some((catalog: SupportV1Function, funcIdent, false)) =>
            if (lookup.isPaimonBuiltInFunction(funcIdent)) {
              throw new UnsupportedOperationException(s"Can't create built-in function: $funcIdent")
            }
            if (c.isTableFunc) {
              throw new UnsupportedOperationException(
                s"Paimon does not support creating SQL table functions yet: $funcIdent")
            }
            // Paimon persists no function collation, so a declared or inherited one would be
            // silently dropped.
            defaultCollationSource(catalog, funcIdent, c).foreach {
              source =>
                throw new UnsupportedOperationException(
                  "Paimon does not support DEFAULT COLLATION on SQL functions yet: " +
                    s"$funcIdent $source")
            }
            CreatePaimonSQLFunctionCommand(
              catalog,
              funcIdent,
              c.inputParamText,
              c.returnTypeText,
              c.exprText,
              c.queryText,
              c.comment,
              c.isDeterministic,
              c.containsSQL,
              isTableFunc = false,
              c.ignoreIfExists,
              c.replace
            )
          case _ => c
        }
    }
  }

  /**
   * Describes the default collation Spark 4.2 would give the function and where it comes from: the
   * declared one, else its database's default. `None` if the function gets none.
   */
  private def defaultCollationSource(
      catalog: SupportV1Function,
      funcIdent: FunctionIdentifier,
      plan: LogicalPlan): Option[String] = {
    val shim = SparkShimLoader.shim
    shim.sqlFunctionDefaultCollation(plan).map(collation => s"declares $collation").orElse {
      (catalog, funcIdent.database) match {
        case (ns: SupportsNamespaces, Some(db)) if shim.sqlFunctionInheritsDatabaseCollation =>
          // A missing database has no collation; the create command reports it.
          val collation =
            try Option(ns.loadNamespaceMetadata(Array(db)).get(SupportsNamespaces.PROP_COLLATION))
            catch { case _: NoSuchNamespaceException => None }
          collation.map(c => s"inherits $c from database $db")
        case _ => None
      }
    }
  }
}
