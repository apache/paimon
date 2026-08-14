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

import org.apache.paimon.partition.PartitionStatistics

import scala.util.Try

/** Formatting helpers for Paimon partition statistics in Spark display commands. */
object PartitionStatisticsDisplay {

  /** Label for a statistic or creation time that is not known. */
  val UNKNOWN: String = "UNKNOWN"

  /** The statistic fields Paimon puts into a Spark partition parameter map. */
  private val STATISTIC_FIELDS: Set[String] = Set(
    PartitionStatistics.FIELD_RECORD_COUNT,
    PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES,
    PartitionStatistics.FIELD_FILE_COUNT,
    PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME
  )

  /** Returns true for a recognized Paimon statistic with a negative numeric value. */
  def isUnreported(field: String, value: String): Boolean =
    STATISTIC_FIELDS.contains(field) &&
      asLong(value).exists(count => !PartitionStatistics.isKnown(count))

  /** Returns the numeric value, or [[UNKNOWN]] when it is absent, nonnumeric, or negative. */
  def render(parameters: collection.Map[String, String], field: String): String =
    parameters
      .get(field)
      .flatMap(asLong)
      .filter(count => PartitionStatistics.isKnown(count))
      .map(_.toString)
      .getOrElse(UNKNOWN)

  private def asLong(value: String): Option[Long] = Try(value.trim.toLong).toOption
}
