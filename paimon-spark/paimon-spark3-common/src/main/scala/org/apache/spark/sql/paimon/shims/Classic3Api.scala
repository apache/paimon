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
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.internal.SessionResourceLoader
class Classic3Api extends ClassicApi {

  override def column(expression: Expression): Column = new Column(expression)

  override def expression(spark: SparkSession, column: Column): Expression = column.expr

  override def createDataset(data: DataFrame): DataFrame = {
    data.sqlContext
      .internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
  }

  override def createDataset(spark: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    Dataset.ofRows(spark, logicalPlan)
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
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) =
    CommandUtils.computeColumnStats(spark, relation, columns)

  override def sessionResourceLoader(session: SparkSession): FunctionResourceLoader = {
    new SessionResourceLoader(session)
  }
}
