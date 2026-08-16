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

package org.apache.paimon.spark.read

import org.apache.paimon.spark.SparkTypeUtils

import org.apache.spark.sql.connector.read.{SupportsPushDownVariantExtractions, VariantExtraction}
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.types.VariantType

/** Spark 4.1+ binding; shadows the no-op `paimon-spark-common` trait by FQN. */
trait PaimonSupportsPushDownVariantExtractions extends SupportsPushDownVariantExtractions {
  protected var acceptedVariantExtractions: Map[Seq[String], Seq[VariantExtractionInfo]] = Map.empty

  override def pushVariantExtractions(extractions: Array[VariantExtraction]): Array[Boolean] = {
    val decoded = extractions.iterator.map {
      ex =>
        val vm = VariantMetadata.fromMetadata(ex.metadata())
        val info = VariantExtractionInfo(
          paimonType = SparkTypeUtils.toPaimonType(ex.expectedDataType()),
          path = vm.path,
          failOnError = vm.failOnError,
          timeZoneId = vm.timeZoneId)
        (ex.columnName().toSeq, info, ex.expectedDataType() == VariantType)
    }.toIndexedSeq
    val (newMap, accepted) = VariantPushDownUtils.acceptByPath(decoded)
    acceptedVariantExtractions = newMap
    accepted
  }
}
