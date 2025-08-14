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

import org.apache.paimon.function.{Function => PaimonFunction}
import org.apache.paimon.function.FunctionDefinition
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.catalog.functions.PaimonFunctions
import org.apache.paimon.spark.execution.{CreatePaimonV1FunctionCommand, DescribePaimonV1FunctionCommand, DropPaimonV1FunctionCommand}
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedException, UnresolvedFunction, UnresolvedFunctionName, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{CreateFunction, DescribeFunction, DropFunction, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UNRESOLVED_FUNCTION}
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog}
import org.apache.spark.sql.types.DataType

case class RewritePaimonFunctionCommands(spark: SparkSession)
  extends Rule[LogicalPlan]
  with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Add a global switch to enable/disable v1 function.
    if (!OptionUtils.v1FunctionEnabled()) {
      return plan
    }

    plan.resolveOperatorsUp {
      case CreateFunction(
            CatalogAndFunctionIdentifier(v1FunctionCatalog: SupportV1Function, funcIdent),
            className,
            resources,
            ifExists,
            replace) =>
        if (isPaimonBuildInFunction(funcIdent)) {
          throw new UnsupportedOperationException(s"Can't create build-in function: $funcIdent")
        }
        val v1Function = CatalogFunction(funcIdent, className, resources)
        CreatePaimonV1FunctionCommand(v1FunctionCatalog, v1Function, ifExists, replace)

      case DropFunction(
            CatalogAndFunctionIdentifier(v1FunctionCatalog: SupportV1Function, funcIdent),
            ifExists) =>
        if (isPaimonBuildInFunction(funcIdent)) {
          throw new UnsupportedOperationException(s"Can't drop build-in function: $funcIdent")
        }
        DropPaimonV1FunctionCommand(v1FunctionCatalog, funcIdent, ifExists)

      case DescribeFunction(
            CatalogAndFunctionIdentifier(v1FunctionCatalog: SupportV1Function, funcIdent),
            isExtended)
          // For Paimon built-in functions, Spark will resolve them by itself.
          if !isPaimonBuildInFunction(funcIdent) =>
        DescribePaimonV1FunctionCommand(v1FunctionCatalog, funcIdent, isExtended)

      // Needs to be done here and transform to `UnResolvedPaimonV1Function`, so that spark's Analyzer can resolve
      // the 'arguments' without throwing an exception, saying that function is not supported.
      case l: LogicalPlan =>
        l.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
          case u: UnresolvedFunction =>
            CatalogAndFunctionIdentifier.unapply(u.nameParts) match {
              case Some((v1FunctionCatalog: SupportV1Function, funcIdent))
                  // For Paimon built-in functions, Spark will resolve them by itself.
                  if !isPaimonBuildInFunction(funcIdent) =>
                // If the function is already registered, avoid redundant lookup in the catalog to reduce overhead.
                if (v1FunctionCatalog.v1FunctionRegistered(funcIdent)) {
                  UnResolvedPaimonV1Function(v1FunctionCatalog, funcIdent, None, u.arguments)
                } else {
                  val function = v1FunctionCatalog.getV1Function(funcIdent)
                  function.definition(FUNCTION_DEFINITION_NAME) match {
                    case _: FunctionDefinition.FileFunctionDefinition =>
                      if (u.isDistinct && u.filter.isDefined) {
                        throw new UnsupportedOperationException(
                          s"DISTINCT with FILTER is not supported, func name: $funcIdent")
                      }
                      UnResolvedPaimonV1Function(
                        v1FunctionCatalog,
                        funcIdent,
                        Some(function),
                        u.arguments)
                    case _ => u
                  }
                }
              case _ => u
            }
        }
    }
  }

  private object CatalogAndFunctionIdentifier {

    def unapply(unresolved: LogicalPlan): Option[(CatalogPlugin, FunctionIdentifier)] =
      unresolved match {
        case ui: UnresolvedIdentifier =>
          unapply(ui.nameParts)
        case name: UnresolvedFunctionName =>
          unapply(name.multipartIdentifier)
        case _ =>
          None
      }

    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, FunctionIdentifier)] = {
      nameParts match {
        // Spark's built-in functions is without database name or catalog name.
        case Seq(funName) if isSparkBuiltInFunction(FunctionIdentifier(funName)) =>
          None
        case CatalogAndIdentifier(v1FunctionCatalog: SupportV1Function, ident)
            if v1FunctionCatalog.v1FunctionEnabled() =>
          Some(v1FunctionCatalog, FunctionIdentifier(ident.name(), Some(ident.namespace().last)))
        case _ =>
          None
      }
    }
  }

  private def isPaimonBuildInFunction(funcIdent: FunctionIdentifier): Boolean = {
    PaimonFunctions.names.contains(funcIdent.funcName)
  }

  private def isSparkBuiltInFunction(funcIdent: FunctionIdentifier): Boolean = {
    FunctionRegistry.builtin.functionExists(funcIdent)
  }
}

/** An unresolved Paimon V1 function to let Spark resolve the necessary variables. */
case class UnResolvedPaimonV1Function(
    v1FunctionCatalog: SupportV1Function,
    funcIdent: FunctionIdentifier,
    func: Option[PaimonFunction],
    arguments: Seq[Expression])
  extends Expression
  with Unevaluable {

  override def children: Seq[Expression] = arguments

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNCTION)

  override def prettyName: String = funcIdent.identifier

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UnResolvedPaimonV1Function = {
    copy(arguments = newChildren)
  }
}
