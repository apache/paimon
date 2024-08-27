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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DatetimeType, DecimalType, DoubleType, FloatType, IntegralType, StringType}

import java.net.URI

/**
 * Some classes or methods defined in the spark project are marked as private under
 * [[org.apache.spark.sql]] package, Hence, use this class to adapt then so that we can use them
 * indirectly.
 */
object PaimonStatsUtils {

  def calculateTotalSize(
      sessionState: SessionState,
      catalogName: String,
      identifier: Identifier,
      locationUri: Option[URI]): Long = {
    CommandUtils.calculateSingleLocationSize(
      sessionState,
      new TableIdentifier(identifier.name(), Some(identifier.namespace().head), Some(catalogName)),
      locationUri)
  }

  def computeColumnStats(
      sparkSession: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    CommandUtils.computeColumnStats(sparkSession, relation, columns)
  }

  /** [[IntegralType]] is private in spark, therefore we need add it here. */
  def analyzeSupportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case BinaryType | StringType => true
    case _ => false
  }

  def hasMinMax(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case _: DatetimeType => true
    case _ => false
  }
}
