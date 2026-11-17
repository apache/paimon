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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructType}

/** Checks on SQL function parameter and return types that Paimon cannot persist. */
object SQLFunctionTypes {

  /**
   * Throws `UnsupportedOperationException` if `dataType` has a string collated other than
   * UTF8_BINARY. Paimon stores a Spark string type without its collation, so it would be lost.
   */
  def requireNoCollation(funcIdent: FunctionIdentifier, what: String, dataType: DataType): Unit =
    if (hasCollation(dataType)) {
      throw new UnsupportedOperationException(
        s"Paimon does not support collated string types in SQL functions yet: $funcIdent $what " +
          s"has type ${dataType.sql}")
    }

  private def hasCollation(dataType: DataType): Boolean = dataType match {
    case s: StringType => s.collationId != CollationFactory.UTF8_BINARY_COLLATION_ID
    case a: ArrayType => hasCollation(a.elementType)
    case m: MapType => hasCollation(m.keyType) || hasCollation(m.valueType)
    case s: StructType => s.fields.exists(f => hasCollation(f.dataType))
    case _ => false
  }
}
