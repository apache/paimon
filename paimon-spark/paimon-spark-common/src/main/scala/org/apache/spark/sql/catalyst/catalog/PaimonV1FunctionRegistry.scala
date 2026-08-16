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

package org.apache.spark.sql.catalyst.catalog

import org.apache.paimon.function.{Function => PaimonFunction}
import org.apache.paimon.spark.catalog.functions.FileFunctionConverter

import org.apache.spark.sql.{PaimonUtils, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{FunctionAlreadyExistsException, FunctionRegistry, FunctionRegistryBase, SimpleFunctionRegistry}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, Expression, ExpressionInfo, FrameLessOffsetWindowFunction, Lag, Lead, NthValue, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.parser.extensions.{PaimonFunctionLookup, UnResolvedPaimonV1Function}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.hive.HiveUDFExpressionBuilder
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.BooleanType

import java.util.Locale

case class PaimonV1FunctionRegistry(session: SparkSession) extends SQLConfHelper {

  /** Resolve a v1 function (SQL or file) to an Expression. */
  def registerAndResolveFunction(u: UnResolvedPaimonV1Function): Expression = {
    val qualifiedIdent = qualifyIdentifier(u.funcIdent)
    val sqlFunc = u.func
      .filter(PaimonFunctionLookup.isPaimonSQLFunction)
      .orElse(Option(sqlFunctionCache.get(qualifiedIdent)))
    if (sqlFunc.isDefined) {
      // SQL UDFs resolve into a Spark `SQLFunctionExpression` (Spark 4.0+ only, via the shim), which
      // Spark's own `ResolveSQLFunctions` rule then inlines.
      if (u.isDistinct || u.filter.isDefined || u.ignoreNulls) {
        throw new UnsupportedOperationException(
          s"SQL function ${u.funcIdent} does not support DISTINCT, FILTER or IGNORE NULLS.")
      }
      val resolvedFun = SparkShimLoader.shim.resolvePaimonSQLFunction(
        u.funcIdent,
        sqlFunc.get,
        u.arguments,
        session.sessionState.sqlParser)
      sqlFunctionCache.putIfAbsent(qualifiedIdent, sqlFunc.get)
      resolvedFun
    } else {
      // File (Hive) function: register into the function registry and resolve via the Hive builder.
      val resolvedFun = resolvePersistentFunctionInternal(
        u.funcIdent,
        u.func,
        u.arguments,
        functionRegistry,
        makeFunctionBuilder)
      validateFunction(resolvedFun, u.arguments.length, u)
    }
  }

  /** Check if the function is registered (file function in Hive registry or SQL function in cache). */
  def isRegistered(funcIdent: FunctionIdentifier): Boolean = {
    val qualifiedIdent = qualifyIdentifier(funcIdent)
    functionRegistry.functionExists(qualifiedIdent) || sqlFunctionCache.containsKey(qualifiedIdent)
  }

  /** Unregister the function. */
  def unregisterFunction(funcIdent: FunctionIdentifier): Unit = {
    val qualifiedIdent = qualifyIdentifier(funcIdent)
    if (functionRegistry.functionExists(qualifiedIdent)) {
      functionRegistry.dropFunction(qualifiedIdent)
    }
    sqlFunctionCache.remove(qualifiedIdent)
  }

  private val functionResourceLoader: FunctionResourceLoader =
    SparkShimLoader.shim.classicApi.sessionResourceLoader(session)
  private val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry
  private val functionExpressionBuilder: FunctionExpressionBuilder = HiveUDFExpressionBuilder

  // SQL functions bypass the Hive registry; cache them here to avoid repeated catalog IO.
  private val sqlFunctionCache =
    new java.util.concurrent.ConcurrentHashMap[FunctionIdentifier, PaimonFunction]()

  private def resolvePersistentFunctionInternal[T](
      funcIdent: FunctionIdentifier,
      func: Option[PaimonFunction],
      arguments: Seq[Expression],
      registry: FunctionRegistryBase[T],
      createFunctionBuilder: CatalogFunction => FunctionRegistryBase[T]#FunctionBuilder): T = {

    // Synchronized: registerFunction with overrideIfExists=false throws on concurrent duplicate loads.
    synchronized {
      val qualifiedIdent = qualifyIdentifier(funcIdent)
      if (registry.functionExists(qualifiedIdent)) {
        registry.lookupFunction(qualifiedIdent, arguments)
      } else {
        require(func.isDefined, "Function must be defined")
        val catalogFunction = FileFunctionConverter.toCatalogFunction(func.get)
        loadFunctionResources(catalogFunction.resources)
        // Preserve user-provided identifier (case-sensitivity may differ from catalog).
        val funcMetadata = catalogFunction.copy(identifier = qualifiedIdent)
        registerFunction(
          funcMetadata,
          overrideIfExists = false,
          registry = registry,
          functionBuilder = createFunctionBuilder(funcMetadata))
        registry.lookupFunction(qualifiedIdent, arguments)
      }
    }
  }

  private def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    resources.foreach(functionResourceLoader.loadResource)
  }

  private def registerFunction[T](
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      registry: FunctionRegistryBase[T],
      functionBuilder: FunctionRegistryBase[T]#FunctionBuilder): Unit = {
    val func = funcDefinition.identifier
    if (registry.functionExists(func) && !overrideIfExists) {
      throw new FunctionAlreadyExistsException(func.nameParts)
    }
    val info = makeExprInfoForHiveFunction(funcDefinition)
    registry.registerFunction(func, info, functionBuilder)
  }

  private def makeExprInfoForHiveFunction(func: CatalogFunction): ExpressionInfo = {
    new ExpressionInfo(
      func.className,
      func.identifier.database.orNull,
      func.identifier.funcName,
      null,
      "",
      "",
      "",
      "",
      "",
      "",
      "hive")
  }

  private def makeFunctionBuilder(func: CatalogFunction): FunctionBuilder = {
    val className = func.className
    if (!PaimonUtils.classIsLoadable(className)) {
      throw new IllegalArgumentException(s"Cannot load class: $className")
    }
    val clazz = PaimonUtils.classForName(className)
    val name = func.identifier.unquotedString
    (input) => functionExpressionBuilder.makeExpression(name, clazz, input)
  }

  private def qualifyIdentifier(ident: FunctionIdentifier): FunctionIdentifier = {
    FunctionIdentifier(funcName = format(ident.funcName), database = ident.database)
  }

  protected def format(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
  }

  private def validateFunction(
      func: Expression,
      numArgs: Int,
      u: UnResolvedPaimonV1Function): Expression = {
    func match {
      // AggregateWindowFunctions are AggregateFunctions that can only be evaluated within
      // the context of a Window clause. They do not need to be wrapped in an
      // AggregateExpression.
      case wf: AggregateWindowFunction =>
        if (u.isDistinct) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(wf.prettyName, "DISTINCT")
        } else if (u.filter.isDefined) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            wf.prettyName,
            "FILTER clause")
        } else if (u.ignoreNulls) {
          wf match {
            case nthValue: NthValue =>
              nthValue.copy(ignoreNulls = u.ignoreNulls)
            case _ =>
              throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
                wf.prettyName,
                "IGNORE NULLS")
          }
        } else {
          wf
        }
      case owf: FrameLessOffsetWindowFunction =>
        if (u.isDistinct) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            owf.prettyName,
            "DISTINCT")
        } else if (u.filter.isDefined) {
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            owf.prettyName,
            "FILTER clause")
        } else if (u.ignoreNulls) {
          owf match {
            case lead: Lead =>
              lead.copy(ignoreNulls = u.ignoreNulls)
            case lag: Lag =>
              lag.copy(ignoreNulls = u.ignoreNulls)
          }
        } else {
          owf
        }
      // We get an aggregate function, we need to wrap it in an AggregateExpression.
      case agg: AggregateFunction =>
        // Note: PythonUDAF does not support these advanced clauses.
        // For compatibility with spark3.4
        if (agg.getClass.getName.equals("org.apache.spark.sql.catalyst.expressions.PythonUDAF"))
          checkUnsupportedAggregateClause(agg, u)

        u.filter match {
          case Some(filter) if !filter.deterministic =>
            throw new RuntimeException(
              "FILTER expression is non-deterministic, it cannot be used in aggregate functions.")
          case Some(filter) if filter.dataType != BooleanType =>
            throw new RuntimeException(
              "FILTER expression is not of type boolean. It cannot be used in an aggregate function.")
          case Some(filter) if filter.exists(_.isInstanceOf[AggregateExpression]) =>
            throw new RuntimeException(
              "FILTER expression contains aggregate. It cannot be used in an aggregate function.")
          case Some(filter) if filter.exists(_.isInstanceOf[WindowExpression]) =>
            throw new RuntimeException(
              "FILTER expression contains window function. It cannot be used in an aggregate function.")
          case _ =>
        }
        if (u.ignoreNulls) {
          val aggFunc = agg match {
            case first: First => first.copy(ignoreNulls = u.ignoreNulls)
            case last: Last => last.copy(ignoreNulls = u.ignoreNulls)
            case any_value: AnyValue => any_value.copy(ignoreNulls = u.ignoreNulls)
            case _ =>
              throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
                agg.prettyName,
                "IGNORE NULLS")
          }
          aggFunc.toAggregateExpression(u.isDistinct, u.filter)
        } else {
          agg.toAggregateExpression(u.isDistinct, u.filter)
        }
      // This function is not an aggregate function, just return the resolved one.
      case other =>
        checkUnsupportedAggregateClause(other, u)
        other
    }
  }

  private def checkUnsupportedAggregateClause(
      func: Expression,
      u: UnResolvedPaimonV1Function): Unit = {
    if (u.isDistinct) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(func.prettyName, "DISTINCT")
    }
    if (u.filter.isDefined) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        func.prettyName,
        "FILTER clause")
    }
    if (u.ignoreNulls) {
      throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
        func.prettyName,
        "IGNORE NULLS")
    }
  }
}
