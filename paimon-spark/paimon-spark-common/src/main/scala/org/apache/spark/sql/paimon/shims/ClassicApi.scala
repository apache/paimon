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

package org.apache.spark.sql.paimon.shims

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.FunctionResourceLoader
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan

trait ClassicApi {

  def column(expression: Expression): Column

  def expression(spark: SparkSession, column: Column): Expression

  def createDataset(data: DataFrame): DataFrame

  def createDataset(sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[Row]

  def prepareExecutedPlan(spark: SparkSession, logicalPlan: LogicalPlan): SparkPlan

  def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit

  def computeColumnStats(
      sparkSession: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat])

  def sessionResourceLoader(session: SparkSession): FunctionResourceLoader
}
