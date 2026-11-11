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
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.catalog.functions.FileFunctionConverter
import org.apache.paimon.spark.execution.{CreatePaimonV1FunctionCommand, DescribePaimonV1FunctionCommand, DropPaimonV1FunctionCommand}
import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, UnresolvedFunction, UnresolvedGenerator}
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{CreateFunction, DescribeFunction, DropFunction, LogicalPlan, SubqueryAlias, UnresolvedWith}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{GENERATOR, PLAN_EXPRESSION, TreePattern, UNRESOLVED_FUNCTION}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{DataType, StructType}

case class RewritePaimonFunctionCommands(spark: SparkSession) extends Rule[LogicalPlan] {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  private lazy val lookup = PaimonFunctionLookup(catalogManager)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!OptionUtils.v1FunctionEnabled()) {
      return plan
    }

    val applied = plan.resolveOperatorsUp {
      case CreateFunction(
            lookup.CatalogAndFunctionIdentifier(_, funcIdent, true),
            _,
            _,
            _,
            replace) =>
        if (replace) {
          throw new UnsupportedOperationException(
            s"$funcIdent is a temporary function, you should use `CREATE OR REPLACE TEMPORARY FUNCTION $funcIdent` or DROP TEMPORARY FUNCTION $funcIdent`.")
        } else {
          throw new UnsupportedOperationException(
            s"$funcIdent is a temporary function and already exists.")
        }

      case CreateFunction(
            lookup.CatalogAndFunctionIdentifier(
              v1FunctionCatalog: SupportV1Function,
              funcIdent,
              false),
            className,
            resources,
            ifExists,
            replace) =>
        if (lookup.isPaimonBuiltInFunction(funcIdent)) {
          throw new UnsupportedOperationException(s"Can't create built-in function: $funcIdent")
        }
        val paimonFunction =
          FileFunctionConverter.fromCatalogFunction(
            CatalogFunction(funcIdent, className, resources))
        CreatePaimonV1FunctionCommand(
          v1FunctionCatalog,
          funcIdent,
          paimonFunction,
          ifExists,
          replace)

      case DropFunction(
            lookup.CatalogAndFunctionIdentifier(
              v1FunctionCatalog: SupportV1Function,
              funcIdent,
              false),
            ifExists) =>
        if (lookup.isPaimonBuiltInFunction(funcIdent)) {
          throw new UnsupportedOperationException(s"Can't drop built-in function: $funcIdent")
        }
        // The function may be v1 function or not, anyway it can be safely deleted here.
        DropPaimonV1FunctionCommand(v1FunctionCatalog, funcIdent, ifExists)

      case d @ DescribeFunction(
            lookup.CatalogAndFunctionIdentifier(
              v1FunctionCatalog: SupportV1Function,
              funcIdent,
              false),
            isExtended)
          // For Paimon built-in functions, Spark will resolve them by itself.
          if !lookup.isPaimonBuiltInFunction(funcIdent) =>
        val function = v1FunctionCatalog.getFunction(funcIdent)
        if (PaimonFunctionLookup.isPaimonV1Function(function)) {
          DescribePaimonV1FunctionCommand(function, isExtended)
        } else {
          d
        }
    }

    // Transform function references to UnResolvedPaimonV1Function so Spark can resolve arguments.
    transformPaimonV1Function(applied)
  }

  private def transformPaimonV1Function(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case u: UnresolvedWith =>
        SparkShimLoader.shim.transformUnresolvedWithCteRelations(
          u,
          alias => transformPaimonV1Function(alias).asInstanceOf[SubqueryAlias])
      case l: LogicalPlan =>
        l.transformExpressionsWithPruning(
          _.containsAnyPattern(UNRESOLVED_FUNCTION, GENERATOR, PLAN_EXPRESSION)) {
          case subquery: SubqueryExpression =>
            // Spark's recursive subquery analyzer does not rerun this parser rewrite.
            subquery.withNewPlan(transformPaimonV1Function(subquery.plan))
          case u: UnresolvedFunction =>
            lookupFunction(u.nameParts)
              .map {
                case (funcIdent, function) => UnResolvedPaimonV1Function(funcIdent, u, function)
              }
              .getOrElse(u)
          case u @ UnresolvedGenerator(name, arguments) =>
            // Spark parses a two-part generator name as database.function. Qualify it before
            // catalog lookup so a database with the same name as a catalog is not reinterpreted.
            val nameParts = name.database
              .map(
                database =>
                  Seq(
                    name.catalog.getOrElse(catalogManager.currentCatalog.name()),
                    database,
                    name.funcName))
              .getOrElse(name.nameParts)
            lookupFunction(nameParts)
              .map {
                case (funcIdent, function) =>
                  UnResolvedPaimonV1Generator(funcIdent, arguments, function)
              }
              .getOrElse(u)
        }
    }
  }

  private def lookupFunction(
      nameParts: Seq[String]): Option[(FunctionIdentifier, Option[PaimonFunction])] = {
    lookup.CatalogAndFunctionIdentifier.unapply(nameParts) match {
      case Some((v1FunctionCatalog: SupportV1Function, funcIdent, false))
          // Spark resolves built-in functions itself.
          if !lookup.isPaimonBuiltInFunction(funcIdent) =>
        // Avoid redundant catalog lookups for functions already loaded into the registry.
        if (v1FunctionCatalog.v1FunctionRegistered(funcIdent)) {
          Some((funcIdent, None))
        } else {
          val function = v1FunctionCatalog.getFunction(funcIdent)
          if (PaimonFunctionLookup.isPaimonV1Function(function)) {
            Some((funcIdent, Some(function)))
          } else {
            None
          }
        }
      case _ => None
    }
  }
}

/** Keeps a REST catalog generator unresolved until Spark resolves its arguments. */
case class UnResolvedPaimonV1Generator(
    funcIdent: FunctionIdentifier,
    arguments: Seq[Expression],
    func: Option[PaimonFunction])
  extends Generator {

  override def children: Seq[Expression] = arguments

  override def elementSchema: StructType = throw new UnresolvedException("elementSchema")
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def foldable: Boolean = throw new UnresolvedException("foldable")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false

  override def prettyName: String = funcIdent.unquotedString

  def resolve(function: Expression): Generator = function match {
    case generator: Generator => generator
    case other =>
      throw QueryCompilationErrors.generatorNotExpectedError(
        funcIdent,
        other.getClass.getCanonicalName)
  }

  // Nothing is compatible with both Scala 2.12's TraversableOnce and 2.13's IterableOnce.
  override def eval(input: InternalRow): Nothing =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UnResolvedPaimonV1Generator =
    copy(arguments = newChildren)
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
