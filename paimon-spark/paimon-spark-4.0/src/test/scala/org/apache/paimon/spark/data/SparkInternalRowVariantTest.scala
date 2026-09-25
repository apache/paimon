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

package org.apache.paimon.spark.data

import org.apache.paimon.data.GenericRow
import org.apache.paimon.data.variant.GenericVariant
import org.apache.paimon.types.{RowType, VariantType}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.VariantVal

class SparkInternalRowVariantTest extends SparkFunSuite {

  test("get variant with generic data type access") {
    val variant = GenericVariant.fromJson("""{"id":1}""")
    val rowType = RowType.of(new VariantType())
    val row = SparkInternalRow.create(rowType).replace(GenericRow.of(variant))

    val actual = row.get(0, DataTypes.VariantType).asInstanceOf[VariantVal]

    assert(actual.getValue.sameElements(variant.value()))
    assert(actual.getMetadata.sameElements(variant.metadata()))
  }
}
