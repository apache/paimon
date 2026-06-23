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
import org.apache.paimon.spark.catalog.functions.SQLFunctionConverter
import org.apache.paimon.spark.execution.CreatePaimonV1FunctionCommand
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{CreateUserDefinedFunction, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * Parser-stage rule that rewrites a Paimon-catalog `CREATE FUNCTION ... RETURN ...`
 * (`CreateUserDefinedFunction`) into [[CreatePaimonV1FunctionCommand]], before Spark's
 * `ResolveSessionCatalog` throws `MISSING_CATALOG_ABILITY.CREATE_FUNCTION`. Fields are read by name
 * (not positional unapply) since `CreateUserDefinedFunction`'s arity differs across Spark 4.0/4.1.
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
            val paimonFunction = SQLFunctionConverter.toPaimonFunction(
              funcIdent,
              c.inputParamText,
              c.returnTypeText,
              c.exprText,
              c.queryText,
              c.comment,
              c.isDeterministic,
              c.containsSQL,
              spark.sessionState.sqlParser)
            CreatePaimonV1FunctionCommand(
              catalog,
              funcIdent,
              paimonFunction,
              c.ignoreIfExists,
              c.replace)
          case _ => c
        }
    }
  }
}
