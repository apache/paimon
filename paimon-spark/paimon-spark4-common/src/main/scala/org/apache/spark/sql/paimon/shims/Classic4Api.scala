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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.classic.{DataFrame => ClassicDataFrame, Dataset => ClassicDataset, ExpressionUtils, SparkSession => ClassicSparkSession, SQLContext => ClassicSQLContext}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.CommandUtils

class Classic4Api extends ClassicApi {

  override def column(expression: Expression): Column = ExpressionUtils.column(expression)

  override def expression(spark: SparkSession, column: Column): Expression = {
    spark match {
      case classicSpark: ClassicSparkSession =>
        classicSpark.expression(column)
      case _ =>
        throw new RuntimeException(s"This doesn't belong to classic family: $spark")
    }
  }

  override def createDataset(data: DataFrame): DataFrame = {
    data match {
      case classicDataFrame: ClassicDataFrame =>
        classicDataFrame.sqlContext
          .asInstanceOf[ClassicSQLContext]
          .internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
      case _ =>
        throw new RuntimeException(s"This doesn't belong to classic family: $data")
    }
  }

  override def createDataset(spark: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    spark match {
      case classicSpark: ClassicSparkSession =>
        ClassicDataset.ofRows(classicSpark, logicalPlan)
      case _ =>
        throw new RuntimeException(s"This doesn't belong to classic family: $spark")
    }
  }

  override def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = {
    spark match {
      case classicSpark: ClassicSparkSession =>
        classicSpark.sharedState.cacheManager.recacheByPlan(classicSpark, plan)
      case _ =>
        throw new RuntimeException(s"This doesn't belong to classic family: $spark")
    }
  }

  override def prepareExecutedPlan(spark: SparkSession, logicalPlan: LogicalPlan): SparkPlan = {
    spark match {
      case classicSpark: ClassicSparkSession =>
        QueryExecution.prepareExecutedPlan(classicSpark, logicalPlan)
      case _ =>
        throw new RuntimeException(s"This doesn't belong to classic family: $spark")
    }
  }

  override def computeColumnStats(
      sparkSession: SparkSession,
      relation: LogicalPlan,
      columns: Seq[Attribute]): (Long, Map[Attribute, ColumnStat]) = {
    sparkSession match {
      case classicSpark: ClassicSparkSession =>
        CommandUtils.computeColumnStats(classicSpark, relation, columns.toSeq)
      case _ =>
        throw new RuntimeException("This SparkSession instance is not classic.SparkSession.")
    }
  }
}
