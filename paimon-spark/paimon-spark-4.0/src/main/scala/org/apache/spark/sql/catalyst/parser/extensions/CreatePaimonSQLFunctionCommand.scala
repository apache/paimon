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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier

/**
 * Simplified version of CreatePaimonSQLFunctionCommand for Spark 4.0. Persists the function without
 * full body analysis (no type inference, no validation). The full version in spark4-common is used
 * by Spark 4.1+.
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
  extends PaimonLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    require(
      returnTypeText != null && returnTypeText.trim.nonEmpty,
      s"SQL function $name requires an explicit RETURNS clause on Spark 4.0.")

    val parser = sparkSession.sessionState.sqlParser
    val paimonFunction = SQLFunctionConverter.toPaimonFunction(
      name,
      inputParamText,
      returnTypeText,
      exprText,
      queryText,
      comment,
      isDeterministic,
      containsSQL,
      parser)

    if (replace) {
      catalog.dropV1Function(name, true)
    }
    catalog.createV1Function(paimonFunction, ignoreIfExists)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreatePaimonSQLFunctionCommand: $name"
  }
}
