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
import org.apache.spark.unsafe.types.{GeographyVal, GeometryVal, VariantVal}

class Spark4ArrayData(override val elementType: DataType) extends AbstractSparkArrayData {

  override def getVariant(ordinal: Int): VariantVal = {
    val v = paimonArray.getVariant(ordinal)
    new VariantVal(v.value(), v.metadata())
  }

  override def getGeography(ordinal: Int): GeographyVal =
    SparkShimLoader.shim
      .toSparkGeography(
        paimonArray.getBinary(ordinal),
        elementType.asInstanceOf[GeographyType].getCrs,
        elementType.asInstanceOf[GeographyType].getAlgorithm.toString)
      .asInstanceOf[GeographyVal]

  override def getGeometry(ordinal: Int): GeometryVal =
    SparkShimLoader.shim
      .toSparkGeometry(
        paimonArray.getBinary(ordinal),
        elementType.asInstanceOf[GeometryType].getCrs)
      .asInstanceOf[GeometryVal]
}
