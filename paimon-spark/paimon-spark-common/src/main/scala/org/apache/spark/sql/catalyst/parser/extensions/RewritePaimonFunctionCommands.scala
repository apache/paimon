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

import org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME
import org.apache.paimon.function.{Function => PaimonFunction}
import org.apache.paimon.function.FunctionDefinition
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.catalog.functions.PaimonFunctions
import org.apache.paimon.spark.execution.{CreatePaimonV1FunctionCommand, DescribePaimonV1FunctionCommand, DropPaimonV1FunctionCommand}
import org.apache.paimon.spark.function.BuiltInFunctions
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, UnresolvedFunction, UnresolvedFunctionName, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{CreateFunction, DescribeFunction, DropFunction, LogicalPlan, SubqueryAlias, UnresolvedWith}
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

    val applied = plan.resolveOperatorsUp {
      case CreateFunction(CatalogAndFunctionIdentifier(_, funcIdent, true), _, _, _, replace) =>
        if (replace) {
          throw new UnsupportedOperationException(
            s"$funcIdent is a temporary function, you should use `CREATE OR REPLACE TEMPORARY FUNCTION $funcIdent` or DROP TEMPORARY FUNCTION $funcIdent`.")
        } else {
          throw new UnsupportedOperationException(
            s"$funcIdent is a temporary function and already exists.")
        }

      case CreateFunction(
            CatalogAndFunctionIdentifier(v1FunctionCatalog: SupportV1Function, funcIdent, false),
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
            CatalogAndFunctionIdentifier(v1FunctionCatalog: SupportV1Function, funcIdent, false),
            ifExists) =>
        if (isPaimonBuildInFunction(funcIdent)) {
          throw new UnsupportedOperationException(s"Can't drop build-in function: $funcIdent")
        }
        // The function may be v1 function or not, anyway it can be safely deleted here.
        DropPaimonV1FunctionCommand(v1FunctionCatalog, funcIdent, ifExists)

      case d @ DescribeFunction(
            CatalogAndFunctionIdentifier(v1FunctionCatalog: SupportV1Function, funcIdent, false),
            isExtended)
          // For Paimon built-in functions, Spark will resolve them by itself.
          if !isPaimonBuildInFunction(funcIdent) =>
        val function = v1FunctionCatalog.getFunction(funcIdent)
        if (isPaimonV1Function(function)) {
          DescribePaimonV1FunctionCommand(function, isExtended)
        } else {
          d
        }
    }

    // Needs to be done here and transform to `UnResolvedPaimonV1Function`, so that spark's Analyzer can resolve
    // the 'arguments' without throwing an exception, saying that function is not supported.
    transformPaimonV1Function(applied)
  }

  private def transformPaimonV1Function(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case u: UnresolvedWith =>
        u.copy(cteRelations = u.cteRelations.map(
          t => (t._1, transformPaimonV1Function(t._2).asInstanceOf[SubqueryAlias])))
      case l: LogicalPlan =>
        l.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
          case u: UnresolvedFunction =>
            CatalogAndFunctionIdentifier.unapply(u.nameParts) match {
              case Some((v1FunctionCatalog: SupportV1Function, funcIdent, false))
                  // For Paimon built-in functions, Spark will resolve them by itself.
                  if !isPaimonBuildInFunction(funcIdent) =>
                // If the function is already registered, avoid redundant lookup in the catalog to reduce overhead.
                if (v1FunctionCatalog.v1FunctionRegistered(funcIdent)) {
                  UnResolvedPaimonV1Function(funcIdent, u, None)
                } else {
                  val function = v1FunctionCatalog.getFunction(funcIdent)
                  if (isPaimonV1Function(function)) {
                    UnResolvedPaimonV1Function(funcIdent, u, Some(function))
                  } else {
                    u
                  }
                }
              case _ => u
            }
        }
    }
  }

  private object CatalogAndFunctionIdentifier {

    def unapply(unresolved: LogicalPlan): Option[(CatalogPlugin, FunctionIdentifier, Boolean)] =
      unresolved match {
        case ui: UnresolvedIdentifier =>
          unapply(ui.nameParts)
        case name: UnresolvedFunctionName =>
          unapply(name.multipartIdentifier)
        case _ =>
          None
      }

    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, FunctionIdentifier, Boolean)] = {
      nameParts match {
        // Spark's built-in or tmp functions is without database name or catalog name.
        case Seq(funName) if isSparkBuiltInFunction(FunctionIdentifier(funName)) =>
          None
        case Seq(funName) if isSparkTmpFunc(FunctionIdentifier(funName)) =>
          Some(null, FunctionIdentifier(funName), true)
        case CatalogAndIdentifier(v1FunctionCatalog: SupportV1Function, ident)
            if v1FunctionCatalog.v1FunctionEnabled() =>
          Some(
            v1FunctionCatalog,
            FunctionIdentifier(
              ident.name(),
              Some(ident.namespace().last),
              Some(v1FunctionCatalog.name)),
            false)
        case _ =>
          None
      }
    }
  }

  private def isPaimonBuildInFunction(funcIdent: FunctionIdentifier): Boolean = {
    funcIdent.database match {
      case Some(db)
          if db == SYSTEM_DATABASE_NAME && PaimonFunctions.names.contains(funcIdent.funcName) =>
        true
      case _ => BuiltInFunctions.FUNCTIONS.containsKey(funcIdent.funcName)
    }
  }

  private def isSparkBuiltInFunction(funcIdent: FunctionIdentifier): Boolean = {
    catalogManager.v1SessionCatalog.isBuiltinFunction(funcIdent)
  }

  private def isPaimonBuiltInFunction(funcIdent: FunctionIdentifier): Boolean = {
    BuiltInFunctions.FUNCTIONS.containsKey(funcIdent.funcName)
  }

  private def isSparkTmpFunc(funcIdent: FunctionIdentifier): Boolean = {
    catalogManager.v1SessionCatalog.isTemporaryFunction(funcIdent)
  }

  private def isPaimonV1Function(fun: PaimonFunction): Boolean = {
    fun.definition(FUNCTION_DEFINITION_NAME) match {
      case _: FunctionDefinition.FileFunctionDefinition => true
      case _ => false
    }
  }
}

/** An unresolved Paimon V1 function to let Spark resolve the necessary variables. */
case class UnResolvedPaimonV1Function(
    funcIdent: FunctionIdentifier,
    arguments: Seq[Expression],
    isDistinct: Boolean,
    filter: Option[Expression] = None,
    ignoreNulls: Boolean = false,
    func: Option[PaimonFunction] = None)
  extends Expression
  with Unevaluable {

  override def children: Seq[Expression] = arguments ++ filter.toSeq

  override def dataType: DataType = throw new UnresolvedException("dataType")

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override lazy val resolved = false

  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNCTION)

  override def prettyName: String = funcIdent.identifier

  override def toString: String = {
    val distinct = if (isDistinct) "distinct " else ""
    s"'$prettyName($distinct${children.mkString(", ")})"
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UnResolvedPaimonV1Function = {
    if (filter.isDefined) {
      copy(arguments = newChildren.dropRight(1), filter = Some(newChildren.last))
    } else {
      copy(arguments = newChildren)
    }
  }
}

object UnResolvedPaimonV1Function {

  def apply(
      funcIdent: FunctionIdentifier,
      u: UnresolvedFunction,
      fun: Option[PaimonFunction]): UnResolvedPaimonV1Function = {
    UnResolvedPaimonV1Function(funcIdent, u.arguments, u.isDistinct, u.filter, u.ignoreNulls, fun)
  }
}
