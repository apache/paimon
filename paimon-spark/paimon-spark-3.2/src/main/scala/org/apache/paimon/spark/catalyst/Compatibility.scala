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
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.ui.SQLPlanMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

object Compatibility {

  def resolveTableOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      conf: SQLConf): LogicalPlan = {
    // SPARK-38228 fixed this separation in 3.3: LEGACY assignment must use non-ANSI casts even
    // when ANSI expression evaluation is enabled. Scope the override to this resolution only.
    val assignmentConf = if (conf.storeAssignmentPolicy == SQLConf.StoreAssignmentPolicy.LEGACY) {
      val legacyConf = conf.clone()
      legacyConf.setConf(SQLConf.ANSI_ENABLED, false)
      legacyConf
    } else {
      conf
    }
    SQLConf.withExistingConf(assignmentConf) {
      TableOutputResolver.resolveOutputColumns(tableName, expected, query, byName, assignmentConf)
    }
  }

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
