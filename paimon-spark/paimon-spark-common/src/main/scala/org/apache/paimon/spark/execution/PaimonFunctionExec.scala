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

import org.apache.paimon.function.FunctionDefinition
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogFunction
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class CreatePaimonV1FunctionCommand(
    catalog: SupportV1Function,
    v1Function: CatalogFunction,
    ignoreIfExists: Boolean,
    replace: Boolean)
  extends PaimonLeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Note: for replace just drop then create ,this operation is non-atomic.
    if (replace) {
      catalog.dropV1Function(v1Function.identifier, true)
    }
    catalog.createV1Function(v1Function, ignoreIfExists)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreatePaimonV1FunctionCommand: ${v1Function.identifier}"
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
      case other =>
        throw new UnsupportedOperationException(s"Unsupported function definition $other")
    }

    rows.toSeq
  }

  override def simpleString(maxFields: Int): String = {
    s"DescribePaimonV1FunctionCommand: ${function.fullName()}"
  }
}
