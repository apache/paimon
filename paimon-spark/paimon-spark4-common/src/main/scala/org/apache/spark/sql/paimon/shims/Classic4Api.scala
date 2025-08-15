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
import org.apache.spark.sql.classic.{ClassicConversions, Dataset => ClassicDataset, ExpressionUtils}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.internal.SessionResourceLoader

/**
 * This class is used to implement the conversion from sql-api to classic one. Make sure this is the
 * only class that implements [[org.apache.spark.sql.classic.ClassicConversions]] in Paimon-Spark.
 */
class Classic4Api extends ClassicApi with ClassicConversions {

  override def column(expression: Expression): Column = ExpressionUtils.column(expression)

  override def expression(spark: SparkSession, column: Column): Expression = {
    spark.expression(column)
  }

  override def createDataset(data: DataFrame): DataFrame = {
    data.sqlContext
      .internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
  }

  override def createDataset(spark: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    ClassicDataset.ofRows(spark, logicalPlan)
  }

  override def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, plan)
  }

  override def prepareExecutedPlan(spark: SparkSession, logicalPlan: LogicalPlan): SparkPlan = {
    QueryExecution.prepareExecutedPlan(spark, logicalPlan)
  }

  override def computeColumnStats(
      spark: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    CommandUtils.computeColumnStats(spark, relation, columns.toSeq)
  }

  override def sessionResourceLoader(session: SparkSession): FunctionResourceLoader = {
    new SessionResourceLoader(session)
  }
}
