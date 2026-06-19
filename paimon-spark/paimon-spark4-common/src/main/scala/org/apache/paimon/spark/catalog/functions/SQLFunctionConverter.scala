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

package org.apache.paimon.spark.catalog.functions

import org.apache.paimon.function.{Function => PaimonFunction, FunctionDefinition, FunctionImpl}
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.types.DataField

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.SQLFunctionExpression
import org.apache.spark.sql.catalyst.catalog.{SQLFunction, UserDefinedFunction}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.{DataType => SparkDataType, StructType}

import java.util.{Collections, HashMap => JHashMap, List => JList}

/**
 * Converts between Spark's [[SQLFunction]] (`CREATE FUNCTION ... RETURN ...`) and a Paimon
 * [[PaimonFunction]] with a [[FunctionDefinition.SQLFunctionDefinition]] body. Scalar only; the raw
 * `inputParamText` / `returnTypeText` are stored in options and re-parsed on read so `DEFAULT` etc.
 * round-trip faithfully.
 */
object SQLFunctionConverter {

  // Stored in options() so the Spark SQLFunction can be rebuilt faithfully.
  private val IS_QUERY = "spark.sql-function.is-query"
  private val INPUT_PARAM = "spark.sql-function.input-param"
  private val RETURN_TYPE = "spark.sql-function.return-type"
  private val DETERMINISTIC = "spark.sql-function.deterministic"
  private val CONTAINS_SQL = "spark.sql-function.contains-sql"

  /** Build a Paimon function from a parsed `CREATE FUNCTION ... RETURN ...` statement. */
  def toPaimonFunction(
      funcIdent: FunctionIdentifier,
      inputParamText: Option[String],
      returnTypeText: String,
      exprText: Option[String],
      queryText: Option[String],
      comment: Option[String],
      isDeterministic: Option[Boolean],
      containsSQL: Option[Boolean],
      parser: ParserInterface): PaimonFunction = {
    require(
      returnTypeText != null && returnTypeText.trim.nonEmpty,
      s"SQL function $funcIdent must declare an explicit RETURNS type.")
    val identifier = FunctionIdentifierConverter.toPaimonIdentifier(funcIdent)

    // Parse to validate (e.g. DEFAULT / NOT NULL) and to populate typed params for interop.
    val inputParams: JList[DataField] = inputParamText.filter(_.trim.nonEmpty) match {
      case Some(text) =>
        SparkTypeUtils
          .toPaimonRowType(UserDefinedFunction.parseRoutineParam(text, parser))
          .getFields
      case None => Collections.emptyList[DataField]()
    }
    val returnSparkType = scalarReturnType(funcIdent, returnTypeText, parser)
    val returnParams: JList[DataField] =
      Collections.singletonList(
        new DataField(0, "result", SparkTypeUtils.toPaimonType(returnSparkType)))

    // Exactly one of exprText / queryText is set by the parser.
    val isQuery = exprText.isEmpty && queryText.isDefined
    val body = exprText
      .orElse(queryText)
      .getOrElse(throw new IllegalArgumentException(s"SQL function $funcIdent has an empty body."))
    val definitions =
      Collections.singletonMap[String, FunctionDefinition](
        FUNCTION_DEFINITION_NAME,
        FunctionDefinition.sql(body))

    val options = new JHashMap[String, String]()
    options.put(IS_QUERY, isQuery.toString)
    options.put(INPUT_PARAM, inputParamText.getOrElse(""))
    options.put(RETURN_TYPE, returnTypeText)
    isDeterministic.foreach(d => options.put(DETERMINISTIC, d.toString))
    containsSQL.foreach(c => options.put(CONTAINS_SQL, c.toString))

    new FunctionImpl(
      identifier,
      inputParams,
      returnParams,
      isDeterministic.getOrElse(true),
      definitions,
      comment.orNull,
      options)
  }

  /** Rebuild a Spark [[SQLFunction]] from a Paimon-stored SQL function. */
  def toSQLFunction(
      funcIdent: FunctionIdentifier,
      function: PaimonFunction,
      parser: ParserInterface): SQLFunction = {
    val options = function.options()

    val inputParam: Option[StructType] =
      Option(options.get(INPUT_PARAM))
        .filter(_.trim.nonEmpty)
        .map(UserDefinedFunction.parseRoutineParam(_, parser))

    val returnTypeText = options.get(RETURN_TYPE)
    require(
      returnTypeText != null && returnTypeText.trim.nonEmpty,
      s"SQL function $funcIdent is missing its return type.")
    val returnType: SparkDataType = scalarReturnType(funcIdent, returnTypeText, parser)

    val body = function.definition(FUNCTION_DEFINITION_NAME) match {
      case sql: FunctionDefinition.SQLFunctionDefinition => sql.definition()
      case other =>
        throw new IllegalStateException(
          s"Function $funcIdent is not a SQL function, found definition: $other")
    }

    val isQuery = java.lang.Boolean.parseBoolean(options.getOrDefault(IS_QUERY, "false"))
    val deterministic = Option(options.get(DETERMINISTIC)).map(_.toBoolean)
    val containsSQL = Option(options.get(CONTAINS_SQL)).map(_.toBoolean)

    SQLFunction(
      name = funcIdent,
      inputParam = inputParam,
      returnType = Left(returnType),
      exprText = if (isQuery) None else Some(body),
      queryText = if (isQuery) Some(body) else None,
      comment = Option(function.comment()),
      deterministic = deterministic,
      containsSQL = containsSQL,
      isTableFunc = false,
      properties = Map.empty
    )
  }

  /**
   * Resolve a Paimon-stored SQL function reference into a Spark [[SQLFunctionExpression]]. Spark's
   * own `ResolveSQLFunctions` analyzer rule then inlines the function body.
   */
  def toSQLFunctionExpression(
      funcIdent: FunctionIdentifier,
      function: PaimonFunction,
      arguments: Seq[Expression],
      parser: ParserInterface): Expression = {
    val sqlFunction = toSQLFunction(funcIdent, function, parser)
    SQLFunctionExpression(
      sqlFunction.name.unquotedString,
      sqlFunction,
      arguments,
      Some(sqlFunction.getScalarFuncReturnType))
  }

  private def scalarReturnType(
      funcIdent: FunctionIdentifier,
      returnTypeText: String,
      parser: ParserInterface): SparkDataType = {
    SQLFunction.parseReturnTypeText(returnTypeText, isTableFunc = false, parser) match {
      case Some(Left(dataType)) => dataType
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported return type '$returnTypeText' for scalar SQL function $funcIdent.")
    }
  }
}
