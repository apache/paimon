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
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.CapturesConfig
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{withPosition, Analyzer, SQLFunctionExpression, SQLFunctionNode, SQLScalarFunction, SQLTableFunction, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, SQLFunction, UserDefinedFunction, UserDefinedFunctionErrors}
import org.apache.spark.sql.catalyst.catalog.UserDefinedFunction._
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, Generator, LateralSubquery, Literal, ScalarSubquery, SubqueryExpression, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{LateralJoin, LocalRelation, LogicalPlan, OneRowRelation, Project, Range, UnresolvedWith, View}
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CreateUserDefinedFunctionCommand._
import org.apache.spark.sql.execution.command.ViewHelper
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Adapted from Spark's CreateSQLFunctionCommand. Analyzes the function body, validates, derives
 * deterministic/containsSQL, then persists to the Paimon catalog instead of the session catalog.
 */
case class CreatePaimonSQLFunctionCommand(
    catalog: SupportV1Function,
    name: FunctionIdentifier,
    inputParamText: Option[String],
    returnTypeText: String,
    exprText: Option[String],
    queryText: Option[String],
    comment: Option[String],
    isDeterministic: Option[Boolean],
    containsSQL: Option[Boolean],
    isTableFunc: Boolean,
    ignoreIfExists: Boolean,
    replace: Boolean)
  extends PaimonLeafRunnableCommand
  with CapturesConfig {

  import SQLFunction._

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val parser = sparkSession.sessionState.sqlParser
    val analyzer = sparkSession.sessionState.analyzer
    val sessionCatalog = sparkSession.sessionState.catalog
    val conf = sparkSession.sessionState.conf

    val inputParam = inputParamText.map(UserDefinedFunction.parseRoutineParam(_, parser))
    val returnType = parseReturnTypeText(returnTypeText, isTableFunc, parser)

    val function = SQLFunction(
      name,
      inputParam,
      returnType.getOrElse(if (isTableFunc) Right(null) else Left(null)),
      exprText,
      queryText,
      comment,
      isDeterministic,
      containsSQL,
      isTableFunc,
      Map.empty
    )

    val newFunction = {
      val (expression, query) = function.getExpressionAndQuery(parser, isTableFunc)
      assert(query.nonEmpty || expression.nonEmpty)

      // Build function input.
      val inputPlan = if (inputParam.isDefined) {
        val param = inputParam.get
        checkParameterNotNull(param, inputParamText.get)
        checkParameterNameDuplication(param, conf, name)
        checkDefaultsTrailing(param, name)

        // Qualify the input parameters with the function name so that attributes referencing
        // the function input parameters can be resolved correctly.
        val qualifier = Seq(name.funcName)
        val input = param.map(
          p =>
            Alias(
              {
                val defaultExpr = p.getDefault()
                if (defaultExpr.isEmpty) {
                  Literal.create(null, p.dataType)
                } else {
                  val defaultPlan = parseDefault(defaultExpr.get, parser)
                  if (SubqueryExpression.hasSubquery(defaultPlan)) {
                    throw new AnalysisException(
                      errorClass = "USER_DEFINED_FUNCTIONS.NOT_A_VALID_DEFAULT_EXPRESSION",
                      messageParameters =
                        Map("functionName" -> name.funcName, "parameterName" -> p.name))
                  } else if (defaultPlan.containsPattern(UNRESOLVED_ATTRIBUTE)) {
                    // TODO(SPARK-50698): use parsed expression instead of expression string.
                    defaultPlan.collect {
                      case a: UnresolvedAttribute =>
                        throw QueryCompilationErrors.unresolvedAttributeError(
                          "UNRESOLVED_COLUMN",
                          a.sql,
                          Seq.empty,
                          a.origin)
                    }
                  }
                  Cast(defaultPlan, p.dataType)
                }
              },
              p.name
            )(qualifier = qualifier))
        Project(input, OneRowRelation())
      } else {
        OneRowRelation()
      }

      // Build the function body and check if the function body can be analyzed successfully.
      val (unresolvedPlan, analyzedPlan, inferredReturnType) = if (!isTableFunc) {
        // Build SQL scalar function plan.
        val outputExpr = if (query.isDefined) ScalarSubquery(query.get) else expression.get
        val plan: LogicalPlan = returnType
          .map {
            t =>
              val retType: DataType = t match {
                case Left(t) => t
                case _ =>
                  throw SparkException.internalError("Unexpected return type for a scalar SQL UDF.")
              }
              val outputCast = Seq(Alias(Cast(outputExpr, retType), name.funcName)())
              Project(outputCast, inputPlan)
          }
          .getOrElse {
            // If no explicit RETURNS clause is present, infer the result type from the function body.
            val outputAlias = Seq(Alias(outputExpr, name.funcName)())
            Project(outputAlias, inputPlan)
          }

        // Check cyclic function reference before running the analyzer.
        checkCyclicFunctionReference(sessionCatalog, name, plan)

        // Check the function body can be analyzed correctly.
        val analyzed = analyzer.execute(plan)
        val (resolved, resolvedReturnType) = analyzed match {
          case p @ Project(expr :: Nil, _) if expr.resolved =>
            (p, Left(expr.dataType))
          case other =>
            (other, function.returnType)
        }

        // Check if the SQL function body contains aggregate/window functions.
        // This check needs to be performed before checkAnalysis to provide better error messages.
        checkAggOrWindowOrGeneratorExpr(resolved)

        // Check if the SQL function body can be analyzed.
        checkFunctionBodyAnalysis(analyzer, function, resolved)

        (plan, resolved, resolvedReturnType)
      } else {
        // Build SQL table function plan.
        if (query.isEmpty) {
          throw UserDefinedFunctionErrors.bodyIsNotAQueryForSqlTableUdf(name.funcName)
        }
        // Check cyclic function reference before running the analyzer.
        checkCyclicFunctionReference(sessionCatalog, name, query.get)

        // Construct a lateral join to analyze the function body.
        val plan = LateralJoin(inputPlan, LateralSubquery(query.get), Inner, None)
        val analyzed = analyzer.execute(plan)
        val newPlan = analyzed match {
          case Project(_, j: LateralJoin) => j
          case j: LateralJoin => j
          case _ =>
            throw SparkException.internalError(
              "Unexpected plan returned when " +
                s"creating a SQL TVF: ${analyzed.getClass.getSimpleName}.")
        }
        val maybeResolved = newPlan.asInstanceOf[LateralJoin].right.plan

        // Check if the function body can be analyzed.
        checkFunctionBodyAnalysis(analyzer, function, maybeResolved)

        // Get the function's return schema.
        val returnParam: StructType = returnType
          .map {
            case Right(t) => t
            case Left(_) =>
              throw SparkException.internalError(
                "Unexpected return schema for a SQL table function.")
          }
          .getOrElse {
            query.get match {
              case Project(projectList, _) if projectList.exists(_.isInstanceOf[UnresolvedAlias]) =>
                throw UserDefinedFunctionErrors.missingColumnNamesForSqlTableUdf(name.funcName)
              case _ =>
                StructType(analyzed.asInstanceOf[LateralJoin].right.plan.output.map {
                  col => StructField(col.name, col.dataType)
                })
            }
          }

        // Check the return columns cannot have NOT NULL specified.
        checkParameterNotNull(returnParam, returnTypeText)

        // Check duplicated return column names.
        checkReturnsColumnDuplication(returnParam, conf, name)

        // Check if the actual output size equals to the number of return parameters.
        val outputSize = maybeResolved.output.size
        if (outputSize != returnParam.size) {
          throw new AnalysisException(
            errorClass = "USER_DEFINED_FUNCTIONS.RETURN_COLUMN_COUNT_MISMATCH",
            messageParameters = Map(
              "outputSize" -> s"$outputSize",
              "returnParamSize" -> s"${returnParam.size}",
              "name" -> s"$name"
            )
          )
        }

        (plan, analyzed, Right(returnParam))
      }

      // A permanent function is not allowed to reference temporary objects.
      verifyTemporaryObjectsNotExists(sessionCatalog, name, unresolvedPlan, analyzedPlan)

      // Generate function properties.
      val properties = generateFunctionProperties(sparkSession, unresolvedPlan, analyzedPlan)

      // Derive determinism of the SQL function.
      val deterministic = analyzedPlan.deterministic

      // Derive and check a SQL function with CONTAINS SQL data access should not reads SQL data.
      val readsSQLData = deriveSQLDataAccess(analyzedPlan)

      function.copy(
        // Assign the return type, inferring from the function body if needed.
        returnType = inferredReturnType,
        deterministic = Some(function.deterministic.getOrElse(deterministic)),
        containsSQL = Some(function.containsSQL.getOrElse(!readsSQLData)),
        properties = properties
      )
    }

    // ---- Paimon-specific: persist to Paimon catalog ----
    val resolvedReturnTypeText = newFunction.returnType match {
      case Left(dt) if dt != null => dt.sql
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot infer return type for SQL function ${name.funcName}. " +
            "Please add an explicit RETURNS clause.")
    }

    val paimonFunction = SQLFunctionConverter.toPaimonFunction(
      name,
      inputParamText,
      if (returnTypeText != null && returnTypeText.trim.nonEmpty) returnTypeText
      else resolvedReturnTypeText,
      exprText,
      queryText,
      comment,
      newFunction.deterministic,
      newFunction.containsSQL,
      parser,
      newFunction.properties
    )

    if (replace) {
      catalog.dropV1Function(name, true)
    }
    catalog.createV1Function(paimonFunction, ignoreIfExists)
    Nil
  }

  /** Check if the function body can be analyzed. */
  private def checkFunctionBodyAnalysis(
      analyzer: Analyzer,
      function: SQLFunction,
      body: LogicalPlan): Unit = {
    analyzer.checkAnalysis(SQLFunctionNode(function, body))
  }

  /** Collect all temporary views and functions and return the identifiers separately */
  private def collectTemporaryObjectsInUnresolvedPlan(
      catalog: SessionCatalog,
      child: LogicalPlan): (Seq[Seq[String]], Seq[String]) = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    def collectTempViews(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap {
        case UnresolvedRelation(nameParts, _, _) if catalog.isTempView(nameParts) =>
          Seq(nameParts)
        case w: UnresolvedWith if !w.resolved => w.innerChildren.flatMap(collectTempViews)
        case plan if !plan.resolved =>
          plan.expressions.flatMap(_.flatMap {
            case e: SubqueryExpression => collectTempViews(e.plan)
            case _ => Seq.empty
          })
        case _ => Seq.empty
      }.distinct
    }

    def collectTempFunctions(child: LogicalPlan): Seq[String] = {
      child.flatMap {
        case w: UnresolvedWith if !w.resolved => w.innerChildren.flatMap(collectTempFunctions)
        case plan if !plan.resolved =>
          plan.expressions.flatMap(_.flatMap {
            case e: SubqueryExpression => collectTempFunctions(e.plan)
            case e: UnresolvedFunction
                if catalog.isTemporaryFunction(e.nameParts.asFunctionIdentifier) =>
              Seq(e.nameParts.asFunctionIdentifier.funcName)
            case _ => Seq.empty
          })
        case _ => Seq.empty
      }.distinct
    }
    (collectTempViews(child), collectTempFunctions(child))
  }

  /**
   * Permanent functions are not allowed to reference temp objects, including temp functions and
   * temp views.
   */
  private def verifyTemporaryObjectsNotExists(
      catalog: SessionCatalog,
      name: FunctionIdentifier,
      child: LogicalPlan,
      analyzed: LogicalPlan): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val (tempViews, tempFunctions) = collectTemporaryObjectsInUnresolvedPlan(catalog, child)
    tempViews.foreach {
      nameParts =>
        throw UserDefinedFunctionErrors.invalidTempViewReference(
          routineName = name.asMultipart,
          tempViewName = nameParts)
    }
    tempFunctions.foreach {
      funcName =>
        throw UserDefinedFunctionErrors.invalidTempFuncReference(
          routineName = name.asMultipart,
          tempFuncName = funcName)
    }
    val tempVars = ViewHelper.collectTemporaryVariables(analyzed)
    tempVars.foreach {
      varName =>
        throw UserDefinedFunctionErrors.invalidTempVarReference(
          routineName = name.asMultipart,
          varName = varName)
    }
  }

  /** Check if the given plan contains cyclic function references. */
  private def checkCyclicFunctionReference(
      catalog: SessionCatalog,
      identifier: FunctionIdentifier,
      plan: LogicalPlan): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    def checkPlan(plan: LogicalPlan, path: Seq[FunctionIdentifier]): Unit = {
      plan.foreach {
        case u @ UnresolvedTableValuedFunction(nameParts, arguments, _) =>
          try {
            val funcId = nameParts.asFunctionIdentifier
            val info = catalog.lookupFunctionInfo(funcId)
            if (isSQLFunction(info.getClassName)) {
              val f = withPosition(u) {
                catalog.lookupTableFunction(funcId, arguments).asInstanceOf[SQLTableFunction]
              }
              val newPath = path :+ f.function.name
              if (f.function.name == name) {
                throw UserDefinedFunctionErrors.cyclicFunctionReference(newPath.mkString(" -> "))
              }
              val plan = catalog.makeSQLTableFunctionPlan(f.name, f.function, f.inputs, f.output)
              checkPlan(plan, newPath)
            }
          } catch {
            case _: AnalysisException =>
          }
        case p: LogicalPlan =>
          p.expressions.foreach(checkExpression(_, path))
      }
    }

    def checkExpression(expression: Expression, path: Seq[FunctionIdentifier]): Unit = {
      expression.foreach {
        case s: SubqueryExpression => checkPlan(s.plan, path)
        case u @ UnresolvedFunction(nameParts, arguments, _, _, _, _, _) =>
          try {
            val funcId = nameParts.asFunctionIdentifier
            val info = catalog.lookupFunctionInfo(funcId)
            if (isSQLFunction(info.getClassName)) {
              val f = withPosition(u) {
                catalog.lookupFunction(funcId, arguments).asInstanceOf[SQLFunctionExpression]
              }
              val newPath = path :+ f.function.name
              if (f.function.name == name) {
                throw UserDefinedFunctionErrors.cyclicFunctionReference(newPath.mkString(" -> "))
              }
              val plan = catalog.makeSQLFunctionPlan(f.name, f.function, f.inputs)
              checkPlan(plan, newPath)
            }
          } catch {
            case _: AnalysisException =>
          }
        case _ =>
      }
    }

    checkPlan(plan, Seq(identifier))
  }

  /**
   * Check if the SQL function body contains aggregate/window/generate functions. Note subqueries
   * inside the SQL function body can contain aggregate/window/generate functions.
   */
  private def checkAggOrWindowOrGeneratorExpr(plan: LogicalPlan): Unit = {
    if (plan.resolved) {
      plan.transformAllExpressions {
        case e
            if e.isInstanceOf[WindowExpression] || e.isInstanceOf[Generator] ||
              e.isInstanceOf[AggregateExpression] =>
          throw new AnalysisException(
            errorClass = "USER_DEFINED_FUNCTIONS.CANNOT_CONTAIN_COMPLEX_FUNCTIONS",
            messageParameters = Map("queryText" -> s"${exprText.orElse(queryText).get}")
          )
      }
    }
  }

  /**
   * Derive the SQL data access routine of the function and check if the SQL function matches its
   * data access routine. If the data access is CONTAINS SQL, the expression should not access
   * operators and expressions that read SQL data.
   *
   * Returns true is SQL data access routine is READS SQL DATA, otherwise returns false.
   */
  private def deriveSQLDataAccess(plan: LogicalPlan): Boolean = {
    // Find logical plan nodes that read SQL data.
    val readsSQLData = plan.find {
      case _: View => true
      case p if p.children.isEmpty =>
        p match {
          case _: OneRowRelation | _: LocalRelation | _: Range => false
          case _ => true
        }
      case f: SQLFunctionNode => f.function.containsSQL.contains(false)
      case p: LogicalPlan =>
        lazy val sub = p.subqueries.exists(deriveSQLDataAccess)
        // If the SQL function contains another SQL function that has SQL data access routine
        // to be READS SQL DATA, then this SQL function will also be READS SQL DATA.
        p.expressions.exists(
          expr =>
            expr.find {
              case f: SQLScalarFunction => f.function.containsSQL.contains(false)
              case sub: SubqueryExpression => deriveSQLDataAccess(sub.plan)
              case _ => false
            }.isDefined)
    }.isDefined

    if (containsSQL.contains(true) && readsSQLData) {
      throw new AnalysisException(
        errorClass = "INVALID_SQL_FUNCTION_DATA_ACCESS",
        messageParameters = Map.empty
      )
    }

    readsSQLData
  }

  /**
   * Generate the function properties, including:
   *   1. the SQL configs when creating the function.
   *   2. the catalog and database name when creating the function. This will be used to provide
   *      context during nested function resolution.
   *   3. referred temporary object names if the function is a temp function.
   */
  private def generateFunctionProperties(
      session: SparkSession,
      plan: LogicalPlan,
      analyzed: LogicalPlan): Map[String, String] = {
    val catalog = session.sessionState.catalog
    val conf = session.sessionState.conf
    val manager = session.sessionState.catalogManager

    val tempVars = ViewHelper.collectTemporaryVariables(analyzed)

    sqlConfigsToProps(conf, SQL_CONFIG_PREFIX) ++
      catalogAndNamespaceToProps(
        manager.currentCatalog.name,
        manager.currentNamespace.toIndexedSeq) ++
      referredTempNamesToProps(Nil, Nil, tempVars)
  }

  override def simpleString(maxFields: Int): String = {
    s"CreatePaimonSQLFunctionCommand: $name"
  }
}
