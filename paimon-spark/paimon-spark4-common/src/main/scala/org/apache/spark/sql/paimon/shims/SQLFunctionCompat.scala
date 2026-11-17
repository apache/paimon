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

package org.apache.spark.sql.paimon.shims

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{SQLFunction, UserDefinedFunction}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.{DataType, StructType}

/** Spark 4.2 adds optional collation to SQL function construction and parsing. */
object SQLFunctionCompat {
  private def invoke(target: AnyRef, name: String, arguments: Seq[AnyRef]): AnyRef = {
    val method = target.getClass.getMethods.find(_.getName == name).get
    val args =
      if (method.getParameterTypes.length == arguments.size) arguments else arguments :+ None
    try { method.invoke(target, args: _*) }
    catch { case e: java.lang.reflect.InvocationTargetException => throw e.getCause }
  }

  def parseParams(text: String, parser: ParserInterface): StructType =
    invoke(UserDefinedFunction, "parseRoutineParam", Seq(text, parser)).asInstanceOf[StructType]

  def parseReturn(
      text: String,
      isTableFunc: Boolean,
      parser: ParserInterface): Option[Either[DataType, StructType]] =
    invoke(
      SQLFunction,
      "parseReturnTypeText",
      Seq(text, java.lang.Boolean.valueOf(isTableFunc), parser))
      .asInstanceOf[Option[Either[DataType, StructType]]]

  def create(
      name: FunctionIdentifier,
      inputParam: Option[StructType],
      returnType: Either[DataType, StructType],
      exprText: Option[String],
      queryText: Option[String],
      comment: Option[String],
      deterministic: Option[Boolean],
      containsSQL: Option[Boolean],
      isTableFunc: Boolean,
      properties: Map[String, String]): SQLFunction = {
    val base: Seq[AnyRef] = Seq(
      name,
      inputParam,
      returnType,
      exprText,
      queryText,
      comment,
      deterministic,
      containsSQL,
      java.lang.Boolean.valueOf(isTableFunc),
      properties,
      None,
      java.lang.Long.valueOf(System.currentTimeMillis())
    )
    val constructor = classOf[SQLFunction].getConstructors.head
    val args =
      if (constructor.getParameterTypes.length == base.size) base
      else base.take(6) ++ Seq(None) ++ base.drop(6)
    constructor.newInstance(args: _*).asInstanceOf[SQLFunction]
  }
}
