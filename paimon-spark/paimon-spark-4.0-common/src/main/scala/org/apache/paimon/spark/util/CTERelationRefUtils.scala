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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.CTERelationRef

object CTERelationRefUtils {

  private val (ctorm, hasStreamingField) = init()

  private def init() = {
    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classC = ru.typeOf[CTERelationRef].typeSymbol.asClass
    val cm = m.reflectClass(classC)
    val ctorC = ru.typeOf[CTERelationRef].decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctorC)
    // SPARK-46062 add isStreaming param
    val hasStreamingField =
      ctorC.paramLists.head.exists(_.name.encodedName.toString == "isStreaming")
    (ctorm, hasStreamingField)
  }

  def createCTERelationRef(
      cteId: Long,
      _resolved: Boolean,
      output: Seq[Attribute]): CTERelationRef = {
    val value = if (hasStreamingField) {
      ctorm(cteId, _resolved, output, false, None)
    } else {
      ctorm(cteId, _resolved, output, None)
    }
    value.asInstanceOf[CTERelationRef]
  }
}
