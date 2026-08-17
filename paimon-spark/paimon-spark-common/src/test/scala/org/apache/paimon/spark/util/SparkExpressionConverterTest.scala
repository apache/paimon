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

import org.apache.paimon.data.GenericRow
import org.apache.paimon.predicate.FieldTransform
import org.apache.paimon.types.DataTypes

import org.apache.spark.sql.connector.expressions.Expressions
import org.scalatest.funsuite.AnyFunSuite

class SparkExpressionConverterTest extends AnyFunSuite {

  test("convert nested named reference") {
    val rowType = DataTypes.ROW(
      DataTypes.FIELD(
        0,
        "profile",
        DataTypes.ROW(
          DataTypes.FIELD(1, "name", DataTypes.STRING()),
          DataTypes.FIELD(2, "address", DataTypes.ROW(DataTypes.FIELD(3, "zip", DataTypes.INT()))))
      ))

    val transform = SparkExpressionConverter
      .toPaimonTransform(Expressions.column("profile.address.zip"), rowType)
      .get
      .asInstanceOf[FieldTransform]
    val fieldRef = transform.fieldRef()

    assert(fieldRef.name() == "profile.address.zip")
    assert(fieldRef.index() == 0)
    assert(fieldRef.nestedIndexes().sameElements(Array(1, 0)))
    assert(fieldRef.nestedArities().sameElements(Array(2, 1)))
    assert(
      transform.transform(
        GenericRow.of(GenericRow.of("Alice", GenericRow.of(Integer.valueOf(100))))) == 100)
    assert(transform.transform(GenericRow.of(null)) == null)
  }
}
