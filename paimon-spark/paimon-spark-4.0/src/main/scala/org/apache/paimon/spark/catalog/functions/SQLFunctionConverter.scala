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
import org.apache.paimon.types.{DataField, RowType}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.SQLFunctionExpression
import org.apache.spark.sql.catalyst.catalog.{SQLFunction, UserDefinedFunction}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.{DataType => SparkDataType, StructType}

import java.util.{Collections, HashMap => JHashMap, List => JList}

import scala.collection.JavaConverters._

/** Converts between Spark SQLFunction and Paimon Function with a SQLFunctionDefinition body. */
object SQLFunctionConverter {

  // Paimon-specific option keys (prefixed to avoid collision with Spark properties).
  private val PAIMON_OPTION_PREFIX = "spark.sql-function."
  private val IS_QUERY = PAIMON_OPTION_PREFIX + "is-query"
  private val DETERMINISTIC = PAIMON_OPTION_PREFIX + "deterministic"
  private val CONTAINS_SQL = PAIMON_OPTION_PREFIX + "contains-sql"

  /** Build a Paimon function from a parsed CREATE FUNCTION ... RETURN statement. */
  def toPaimonFunction(
      funcIdent: FunctionIdentifier,
      inputParamText: Option[String],
      returnTypeText: String,
      exprText: Option[String],
      queryText: Option[String],
      comment: Option[String],
      isDeterministic: Option[Boolean],
      containsSQL: Option[Boolean],
      parser: ParserInterface,
      properties: Map[String, String] = Map.empty): PaimonFunction = {
    require(
      returnTypeText != null && returnTypeText.trim.nonEmpty,
      s"SQL function $funcIdent must have a return type (explicit or inferred).")
    val identifier = FunctionIdentifierConverter.toPaimonIdentifier(funcIdent)

    val inputParams: JList[DataField] = inputParamText.filter(_.trim.nonEmpty) match {
      case Some(text) =>
        SparkTypeUtils
          .toPaimonRowType(UserDefinedFunction.parseRoutineParam(text, parser))
          .getFields
      case None => Collections.emptyList[DataField]()
    }
    val returnSparkType = parseScalarReturnType(funcIdent, returnTypeText, parser)
    val returnParams: JList[DataField] =
      Collections.singletonList(
        new DataField(0, funcIdent.funcName, SparkTypeUtils.toPaimonType(returnSparkType)))

    // Exactly one of exprText / queryText is set by the parser.
    val isQuery = exprText.isEmpty && queryText.isDefined
    val body = exprText
      .orElse(queryText)
      .getOrElse(throw new IllegalArgumentException(s"SQL function $funcIdent has an empty body."))

    val options = new JHashMap[String, String]()
    options.put(IS_QUERY, isQuery.toString)
    isDeterministic.foreach(d => options.put(DETERMINISTIC, d.toString))
    containsSQL.foreach(c => options.put(CONTAINS_SQL, c.toString))
    properties.foreach { case (k, v) => options.put(k, v) }

    new FunctionImpl(
      identifier,
      inputParams,
      returnParams,
      isDeterministic.getOrElse(true), // caller should always pass Some after analysis
      Collections.singletonMap(FUNCTION_DEFINITION_NAME, FunctionDefinition.sql(body)),
      comment.orNull,
      options
    )
  }

  /** Resolve a Paimon-stored SQL function into a Spark SQLFunctionExpression. */
  def toSQLFunctionExpression(
      funcIdent: FunctionIdentifier,
      function: PaimonFunction,
      arguments: Seq[Expression],
      parser: ParserInterface): Expression = {
    val options = function.options()

    val body = function.definition(FUNCTION_DEFINITION_NAME) match {
      case sql: FunctionDefinition.SQLFunctionDefinition => sql.definition()
      case other =>
        throw new IllegalStateException(
          s"Function $funcIdent is not a SQL function, found definition: $other")
    }

    val inputParam: Option[StructType] = {
      val ip = function.inputParams()
      if (ip.isPresent && !ip.get().isEmpty) {
        Some(SparkTypeUtils.fromPaimonType(new RowType(ip.get())).asInstanceOf[StructType])
      } else None
    }

    val rp = function.returnParams()
    require(
      rp.isPresent && !rp.get().isEmpty,
      s"SQL function $funcIdent has no return type in returnParams.")
    val returnType: SparkDataType = SparkTypeUtils.fromPaimonType(rp.get().get(0).`type`())

    val isQuery = Option(options.get(IS_QUERY))
      .map(java.lang.Boolean.parseBoolean)
      .getOrElse {
        try { parser.parseExpression(body); false }
        catch { case _: Exception => true }
      }

    val deterministic = Option(options.get(DETERMINISTIC))
      .map(_.toBoolean)
      .orElse(Some(function.isDeterministic))

    val sqlFunction = SQLFunction(
      name = funcIdent,
      inputParam = inputParam,
      returnType = Left(returnType),
      exprText = if (isQuery) None else Some(body),
      queryText = if (isQuery) Some(body) else None,
      comment = Option(function.comment()),
      deterministic = deterministic,
      containsSQL = Option(options.get(CONTAINS_SQL)).map(_.toBoolean),
      isTableFunc = false,
      properties = options.asScala.filterNot(_._1.startsWith(PAIMON_OPTION_PREFIX)).toMap
    )

    SQLFunctionExpression(
      sqlFunction.name.unquotedString,
      sqlFunction,
      arguments,
      Some(sqlFunction.getScalarFuncReturnType))
  }

  private def parseScalarReturnType(
      funcIdent: FunctionIdentifier,
      returnTypeText: String,
      parser: ParserInterface): SparkDataType =
    SQLFunction.parseReturnTypeText(returnTypeText, isTableFunc = false, parser) match {
      case Some(Left(dataType)) => dataType
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported return type '$returnTypeText' for scalar SQL function $funcIdent.")
    }
}
