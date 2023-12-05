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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PaimonCallCommand}
import org.apache.spark.sql.catalyst.rules.Rule

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Coercion of procedure arguments rule.
 *
 * <p>Most of the content of this class is referenced from Iceberg's ProcedureArgumentCoercion.
 */
object PaimonCoerceArguments extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case call @ PaimonCallCommand(procedure, arguments) if call.resolved =>
      val parameters = procedure.parameters
      val newArguments = arguments.zipWithIndex.map {
        case (argument, index) =>
          val parameter = parameters(index)
          val parameterType = parameter.dataType
          val argumentType = argument.dataType
          if (parameterType != argumentType && !Cast.canUpCast(argumentType, parameterType)) {
            throw new AnalysisException(
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
}
