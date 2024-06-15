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

package org.apache.paimon.spark.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.{universe => ru}

object EncoderUtils {

  private val mirror = ru.runtimeMirror {
    getClass.getClassLoader
  }

  lazy val (module, method) = {
    val expressionEncoder = mirror.reflectModule(
      mirror.staticModule("org.apache.spark.sql.catalyst.encoders.ExpressionEncoder"))
    val term = ru.TermName("apply")
    val structType = List(List("org.apache.spark.sql.types.StructType"));
    val method = expressionEncoder.symbol.info
      .decl(term)
      .asTerm
      .alternatives
      .find(s => s.asMethod.paramLists.map(_.map(_.typeSignature.toString)) == structType)
    if (method.isEmpty) {
      val rowEncoder =
        mirror.reflectModule(
          mirror.staticModule("org.apache.spark.sql.catalyst.encoders.RowEncoder"))
      (
        rowEncoder,
        rowEncoder.symbol.info
          .decl(term)
          .asTerm
          .alternatives
          .find(s => s.asMethod.paramLists.map(_.map(_.typeSignature.toString)) == structType)
          .get
          .asMethod)
    } else {
      (expressionEncoder, method.get.asMethod)
    }
  }

  def encode(schema: StructType): ExpressionEncoder[Row] = {
    mirror
      .reflect(module.instance)
      .reflectMethod(method)(schema)
      .asInstanceOf[ExpressionEncoder[Row]]
  }
}
