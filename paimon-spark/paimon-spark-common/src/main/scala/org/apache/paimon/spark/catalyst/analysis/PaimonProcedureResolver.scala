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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.catalog.ProcedureCatalog
import org.apache.paimon.spark.catalyst.plans.logical.{PaimonCallArgument, PaimonCallCommand, PaimonCallStatement, PaimonNamedArgument, PaimonPositionalArgument}
import org.apache.paimon.spark.procedure.ProcedureParameter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, PaimonLookupCatalog}

import java.util.Locale

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Resolution of stored procedures rule.
 *
 * <p>Most of the content of this class is referenced from Iceberg's ResolveProcedures.
 *
 * @param sparkSession
 *   The Spark session.
 */
case class PaimonProcedureResolver(sparkSession: SparkSession)
  extends Rule[LogicalPlan]
  with PaimonLookupCatalog {

  protected lazy val catalogManager = sparkSession.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case PaimonCallStatement(CatalogAndIdentifier(catalog, identifier), arguments) =>
      val procedure = catalog.asProcedureCatalog.loadProcedure(identifier)
      val parameters = procedure.parameters
      val normalizedParameters = normalizeParameters(parameters)
      validateParameters(normalizedParameters)
      val normalizedArguments = normalizeArguments(arguments)
      PaimonCallCommand(
        procedure,
        args = buildArgumentExpressions(normalizedParameters, normalizedArguments))

    case call @ PaimonCallCommand(procedure, arguments) if call.resolved =>
      val parameters = procedure.parameters
      val newArguments = arguments.zipWithIndex.map {
        case (argument, index) =>
          val parameter = parameters(index)
          val parameterType = parameter.dataType
          val argumentType = argument.dataType
          if (parameterType != argumentType && !Cast.canUpCast(argumentType, parameterType)) {
            throw new RuntimeException(
              s"Cannot cast $argumentType to $parameterType of ${parameter.name}.")
          }
          if (parameterType != argumentType) {
            Cast(argument, parameterType)
          } else {
            argument
          }
      }

      if (newArguments != arguments) {
        call.copy(args = newArguments)
      } else {
        call
      }
  }

  /**
   * Normalizes input parameters of stored procedure.
   *
   * @param parameters
   *   Input parameters of stored procedure.
   * @return
   *   Normalized input parameters.
   */
  private def normalizeParameters(parameters: Seq[ProcedureParameter]): Seq[ProcedureParameter] = {
    parameters.map {
      case parameter if parameter.required =>
        val normalizedName = parameter.name.toLowerCase(Locale.ROOT)
        ProcedureParameter.required(normalizedName, parameter.dataType)
      case param =>
        val normalizedName = param.name.toLowerCase(Locale.ROOT)
        ProcedureParameter.optional(normalizedName, param.dataType)
    }
  }

  /**
   * Validates input parameters of stored procedure.
   *
   * @param parameters
   *   Input parameters of stored procedure.
   */
  private def validateParameters(parameters: Seq[ProcedureParameter]): Unit = {
    val duplicateParamNames = parameters.groupBy(_.name).collect {
      case (name, matchingParams) if matchingParams.length > 1 => name
    }
    if (duplicateParamNames.nonEmpty) {
      throw new RuntimeException(
        s"Parameter names ${duplicateParamNames.mkString("[", ",", "]")} are duplicated.")
    }
    parameters.sliding(2).foreach {
      case Seq(previousParam, currentParam) if !previousParam.required && currentParam.required =>
        throw new RuntimeException(
          s"Optional parameters should be after required ones but $currentParam is after $previousParam.")
      case _ =>
    }
  }

  /**
   * Normalizes arguments of stored procedure.
   *
   * @param arguments
   *   Arguments of stored procedure.
   * @return
   *   Normalized arguments.
   */
  private def normalizeArguments(arguments: Seq[PaimonCallArgument]): Seq[PaimonCallArgument] = {
    arguments.map {
      case a @ PaimonNamedArgument(name, _) => a.copy(name = name.toLowerCase(Locale.ROOT))
      case other => other
    }
  }

  /**
   * Builds argument expressions.
   *
   * @param parameters
   *   Normalized input parameters.
   * @param arguments
   *   Normalized arguments.
   * @return
   *   Argument expressions.
   */
  private def buildArgumentExpressions(
      parameters: Seq[ProcedureParameter],
      arguments: Seq[PaimonCallArgument]): Seq[Expression] = {
    val nameToPositionMap = parameters.map(_.name).zipWithIndex.toMap
    val nameToArgumentMap = buildNameToArgumentMap(parameters, arguments, nameToPositionMap)
    val missingParamNames = parameters.filter(_.required).collect {
      case parameter if !nameToArgumentMap.contains(parameter.name) => parameter.name
    }
    if (missingParamNames.nonEmpty) {
      throw new RuntimeException(
        s"Required parameters ${missingParamNames.mkString("[", ",", "]")} is missed.")
    }
    val argumentExpressions = new Array[Expression](parameters.size)
    nameToArgumentMap.foreach {
      case (name, argument) => argumentExpressions(nameToPositionMap(name)) = argument.expr
    }
    parameters.foreach {
      case parameter if !parameter.required && !nameToArgumentMap.contains(parameter.name) =>
        argumentExpressions(nameToPositionMap(parameter.name)) =
          Literal.create(null, parameter.dataType)
      case _ =>
    }
    argumentExpressions
  }

  /**
   * Builds mapping between name and position.
   *
   * @param parameters
   *   Normalized input parameters.
   * @param arguments
   *   Normalized arguments.
   * @param nameToPositionMap
   *   Mapping between name and position.
   * @return
   *   Mapping between name and position.
   */
  private def buildNameToArgumentMap(
      parameters: Seq[ProcedureParameter],
      arguments: Seq[PaimonCallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, PaimonCallArgument] = {
    val isNamedArgument = arguments.exists(_.isInstanceOf[PaimonNamedArgument])
    val isPositionalArgument = arguments.exists(_.isInstanceOf[PaimonPositionalArgument])

    if (isNamedArgument && isPositionalArgument) {
      throw new RuntimeException("Cannot mix named and positional arguments.")
    }

    if (isNamedArgument) {
      val namedArguments = arguments.asInstanceOf[Seq[PaimonNamedArgument]]
      val validationErrors = namedArguments.groupBy(_.name).collect {
        case (name, procedureArguments) if procedureArguments.size > 1 =>
          s"Procedure argument $name is duplicated."
        case (name, _) if !nameToPositionMap.contains(name) => s"Argument $name is unknown."
      }
      if (validationErrors.nonEmpty) {
        throw new RuntimeException(
          s"Builds name to argument map ${validationErrors.mkString(", ")} error.")
      }
      namedArguments.map(arg => arg.name -> arg).toMap
    } else {
      if (arguments.size > parameters.size) {
        throw new RuntimeException("Too many arguments for procedure")
      }
      arguments.zipWithIndex.map {
        case (argument, position) =>
          val param = parameters(position)
          param.name -> argument
      }.toMap
    }
  }

  /**
   * Procedure catalog validator whether catalog plugin is procedure catalog.
   *
   * @param catalogPlugin
   *   Spark catalog plugin.
   */
  implicit class CatalogValidator(catalogPlugin: CatalogPlugin) {
    def asProcedureCatalog: ProcedureCatalog = catalogPlugin match {
      case procedureCatalog: ProcedureCatalog =>
        procedureCatalog
      case _ =>
        throw new RuntimeException(s"${catalogPlugin.name} is not a ProcedureCatalog.")
    }
  }
}
