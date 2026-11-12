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
    // Spark 4.2 replaced the boolean with DefaultValueFillMode. Preserve the old false
    // behavior (NONE) without linking 3.x/4.1 builds to the new enumeration.
    val method = TableOutputResolver.getClass.getMethods
      .find(m => m.getName == "resolveOutputColumns" && m.getParameterTypes.length == 6)
      .get
    val fillMode: AnyRef = if (method.getParameterTypes.last == java.lang.Boolean.TYPE) {
      java.lang.Boolean.FALSE
    } else {
      val cls = Class.forName(
        "org.apache.spark.sql.catalyst.analysis.TableOutputResolver$DefaultValueFillMode$")
      cls.getMethod("NONE").invoke(cls.getField("MODULE$").get(null))
    }
    try {
      method
        .invoke(
          TableOutputResolver,
          tableName,
          expected,
          query,
          java.lang.Boolean.valueOf(byName),
          conf,
          fillMode)
        .asInstanceOf[LogicalPlan]
    } catch {
      case e: java.lang.reflect.InvocationTargetException => throw e.getCause
    }
  }

  def withNewQuery(o: V2WriteCommand, query: LogicalPlan): V2WriteCommand = {
    o.withNewQuery(query)
  }

  def castByTableInsertionTag: TreeNodeTag[Unit] = {
    Cast.BY_TABLE_INSERTION
  }

  def cast(
      child: Expression,
      dataType: DataType,
      timeZoneId: Option[String] = None,
      ansiEnabled: Boolean = SQLConf.get.ansiEnabled): Cast = {
    Cast(child, dataType, timeZoneId, ansiEnabled)
  }

  def getExecutionMetrics(spark: SparkSession, executionId: Long): Seq[SQLPlanMetric] = {
    // The SQL status store is updated asynchronously and may not have this execution yet.
    spark.sharedState.statusStore.execution(executionId).map(_.metrics.toSeq).getOrElse(Seq.empty)
  }
}
