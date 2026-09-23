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
        val buffer = new ArrayBuffer[(String, String)]
        buffer += ("Function:" -> function.fullName())
        buffer += ("Type:" -> "SCALAR")
        val inputParams = function.inputParams()
        if (inputParams.isPresent && !inputParams.get().isEmpty) {
          val params = formatInputParams(inputParams.get().asScala)
          buffer += ("Input:" -> params.head)
          params.tail.foreach(s => buffer += ("" -> s))
        } else {
          buffer += ("Input:" -> "()")
        }
        val returnParams = function.returnParams()
        if (returnParams.isPresent && !returnParams.get().isEmpty) {
          buffer += ("Returns:" -> returnParams.get().get(0).`type`().asSQLString())
        }
        if (isExtended) {
          Option(function.comment()).foreach(c => buffer += ("Comment:" -> c))
          buffer += ("Deterministic:" -> function.isDeterministic.toString)
          val options = function.options()
          Option(options.get("spark.sql-function.contains-sql"))
            .map(_.toBoolean)
            .foreach {
              c =>
                val dataAccess = if (c) "CONTAINS SQL" else "READS SQL DATA"
                buffer += ("Data Access:" -> dataAccess)
            }
          val configs = options.asScala
            .filter(_._1.startsWith("sqlConfig."))
            .toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"${k.stripPrefix("sqlConfig.")}=$v" }
          if (configs.nonEmpty) {
            buffer += ("Configs:" -> configs.head)
            configs.tail.foreach(s => buffer += ("" -> s))
          }
          buffer += ("Body:" -> sqlFunctionDefinition.definition())
        }
        val keys = tabulate(buffer.map(_._1).toSeq)
        val values = buffer.map(_._2)
        keys.zip(values).foreach { case (key, value) => rows += Row(s"$key $value") }
      case other =>
        throw new UnsupportedOperationException(s"Unsupported function definition $other")
    }

    rows.toSeq
  }

  private def tabulate(inputs: Seq[String]): Seq[String] = {
    val maxLen = inputs.map(_.length).max
    inputs.map(_.padTo(maxLen, ' '))
  }

  private def formatInputParams(
      params: Iterable[org.apache.paimon.types.DataField]): Seq[String] = {
    val fields = params.toSeq
    val names = tabulate(fields.map(_.name()))
    val types = tabulate(fields.map(_.`type`().asSQLString()))
    val defaults = fields.map {
      f => if (isExtended) Option(f.defaultValue()).map(d => s" DEFAULT $d").getOrElse("") else ""
    }
    val comments = fields.map {
      f => if (isExtended) Option(f.description()).map(c => s" '$c'").getOrElse("") else ""
    }
    names.zip(types).zip(defaults).zip(comments).map {
      case (((name, dataType), default), comment) => s"$name $dataType$default$comment"
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"DescribePaimonV1FunctionCommand: ${function.fullName()}"
  }
}
