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

package org.apache.paimon.spark.catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.ui.SQLPlanMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

object Compatibility {

  def withNewQuery(o: V2WriteCommand, query: LogicalPlan): V2WriteCommand = {
    o.withNewQuery(query)
  }

  def castByTableInsertionTag: TreeNodeTag[Unit] = {
    TreeNodeTag[Unit]("by_table_insertion")
  }

  def cast(
      child: Expression,
      dataType: DataType,
      timeZoneId: Option[String] = None,
      ansiEnabled: Boolean = SQLConf.get.ansiEnabled): Cast = {
    Cast(child, dataType, timeZoneId, ansiEnabled)
  }

  def getExecutionMetrics(spark: SparkSession, executionId: Long): Seq[SQLPlanMetric] = {
    spark.sharedState.statusStore.execution(executionId).get.metrics.toSeq
  }
}
