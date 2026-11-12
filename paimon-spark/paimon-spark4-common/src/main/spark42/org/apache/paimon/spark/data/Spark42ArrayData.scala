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

import org.apache.paimon.types.{DataType, GeographyType, GeometryType}

import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.unsafe.types.{BinaryView, VariantVal}

/** Spark 4.2 counterpart of Spark4ArrayData's specialized spatial getters. */
class Spark4ArrayData(override val elementType: DataType) extends AbstractSparkArrayData {
  override def getVariant(ordinal: Int): VariantVal = {
    val value = paimonArray.getVariant(ordinal)
    new VariantVal(value.value(), value.metadata())
  }

  override def getBinaryView(ordinal: Int): BinaryView = {
    val bytes = paimonArray.getBinary(ordinal)
    val value = elementType match {
      case geometry: GeometryType =>
        SparkShimLoader.shim.toSparkGeometry(bytes, geometry.getCrs)
      case geography: GeographyType =>
        SparkShimLoader.shim.toSparkGeography(
          bytes,
          geography.getCrs,
          geography.getAlgorithm.toString)
      case other => throw new UnsupportedOperationException(s"No BinaryView for $other")
    }
    value.asInstanceOf[BinaryView]
  }
}
