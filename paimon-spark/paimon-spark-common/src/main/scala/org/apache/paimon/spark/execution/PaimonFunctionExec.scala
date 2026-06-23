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

package org.apache.paimon.spark.execution

import org.apache.paimon.function.{Function => PaimonFunction, FunctionDefinition}
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Create a Paimon v1 function (file or SQL) from an already-built Paimon [[PaimonFunction]]. */
case class CreatePaimonV1FunctionCommand(
    catalog: SupportV1Function,
    funcIdent: FunctionIdentifier,
    function: PaimonFunction,
    ignoreIfExists: Boolean,
    replace: Boolean)
  extends PaimonLeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    // replace = drop then create (non-atomic).
    if (replace) {
      catalog.dropV1Function(funcIdent, true)
    }
    catalog.createV1Function(function, ignoreIfExists)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreatePaimonV1FunctionCommand: $funcIdent"
  }
}

case class DropPaimonV1FunctionCommand(
    catalog: SupportV1Function,
    funcIdent: FunctionIdentifier,
    ifExists: Boolean)
  extends PaimonLeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    catalog.dropV1Function(funcIdent, ifExists)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"DropPaimonV1FunctionCommand: $funcIdent"
  }
}

case class DescribePaimonV1FunctionCommand(
    function: org.apache.paimon.function.Function,
    isExtended: Boolean)
  extends PaimonLeafRunnableCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("function_desc", StringType, nullable = false)())
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val rows = new ArrayBuffer[Row]()
    function.definition(FUNCTION_DEFINITION_NAME) match {
      case functionDefinition: FunctionDefinition.FileFunctionDefinition =>
        rows += Row(s"Function: ${function.fullName()}")
        rows += Row(s"Class: ${functionDefinition.className()}")
        if (isExtended) {
          rows += Row(
            s"File Resources: ${functionDefinition.fileResources().asScala.map(_.uri()).mkString(", ")}")
        }
      case sqlFunctionDefinition: FunctionDefinition.SQLFunctionDefinition =>
        rows += Row(s"Function: ${function.fullName()}")
        rows += Row("Type: SCALAR")
        val inputParams = function.inputParams()
        if (inputParams.isPresent && !inputParams.get().isEmpty) {
          val params = inputParams
            .get()
            .asScala
            .map(field => s"${field.name()} ${field.`type`().asSQLString()}")
            .mkString(", ")
          rows += Row(s"Input: $params")
        }
        val returnParams = function.returnParams()
        if (returnParams.isPresent && !returnParams.get().isEmpty) {
          rows += Row(s"Returns: ${returnParams.get().get(0).`type`().asSQLString()}")
        }
        if (isExtended) {
          Option(function.comment()).foreach(c => rows += Row(s"Comment: $c"))
          rows += Row(s"Body: ${sqlFunctionDefinition.definition()}")
        }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported function definition $other")
    }

    rows.toSeq
  }

  override def simpleString(maxFields: Int): String = {
    s"DescribePaimonV1FunctionCommand: ${function.fullName()}"
  }
}
