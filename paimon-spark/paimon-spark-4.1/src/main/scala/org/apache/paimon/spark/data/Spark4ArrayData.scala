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

/**
 * Spark 4.1-compatible override of the `paimon-spark4-common` `Spark4ArrayData`. Spark 4.2
 * (SPARK-57058) replaced `SpecializedGetters`' `getGeography` / `getGeometry` with a single
 * `getBinaryView` and removed the `GeographyVal` / `GeometryVal` value classes, so the
 * `paimon-spark4-common` copy implements the 4.2 shape. Spark 4.1 still declares the older pair as
 * abstract, which this copy implements. Shade writes this module's classes before the ones pulled
 * in from `paimon-spark4-common`, so this copy wins on Spark 4.1.
 */
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
