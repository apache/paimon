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

package org.apache.paimon.spark.statistics

import org.apache.paimon.spark.PaimonColumnStats

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.{SQLConfHelper, StructFilters}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.sources.{And, Filter}
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

trait StatisticsHelperBase extends SQLConfHelper {

  val requiredStatsSchema: StructType

  private lazy val replacedStatsSchema =
    CharVarcharUtils.replaceCharVarcharWithStringInSchema(requiredStatsSchema)

  def filterStatistics(v2Stats: Statistics, filters: Seq[Filter]): Statistics = {
    val attrs: Seq[AttributeReference] =
      replacedStatsSchema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    val condition = filterToCondition(filters, attrs)

    if (condition.isDefined && v2Stats.numRows().isPresent) {
      val filteredStats = FilterEstimation(
        logical.Filter(condition.get, FakePlanWithStats(toV1Stats(v2Stats, attrs)))).estimate.get
      toV2Stats(filteredStats)
    } else {
      v2Stats
    }
  }

  private def filterToCondition(filters: Seq[Filter], attrs: Seq[Attribute]): Option[Expression] = {
    StructFilters.filterToExpression(filters.reduce(And), toRef).map {
      expression =>
        expression.transform {
          case ref: BoundReference =>
            attrs.find(_.name == replacedStatsSchema(ref.ordinal).name).get
        }
    }
  }

  private def toRef(attr: String): Option[BoundReference] = {
    val index = replacedStatsSchema.fieldIndex(attr)
    val field = replacedStatsSchema(index)
    Option.apply(BoundReference(index, field.dataType, field.nullable))
  }

  protected def toV1Stats(v2Stats: Statistics, attrs: Seq[Attribute]): logical.Statistics

  private def toV2Stats(v1Stats: logical.Statistics): Statistics = {
    new Statistics() {
      override def sizeInBytes(): OptionalLong = if (v1Stats.sizeInBytes != null)
        OptionalLong.of(v1Stats.sizeInBytes.longValue)
      else OptionalLong.empty()

      override def numRows(): OptionalLong = if (v1Stats.rowCount.isDefined)
        OptionalLong.of(v1Stats.rowCount.get.longValue)
      else OptionalLong.empty()

      override def columnStats(): java.util.Map[NamedReference, ColumnStatistics] = {
        val columnStatsMap = new java.util.HashMap[NamedReference, ColumnStatistics]()
        v1Stats.attributeStats.foreach {
          case (attr, v1ColStats) =>
            columnStatsMap.put(
              PaimonUtils.fieldReference(attr.name),
              PaimonColumnStats(v1ColStats)
            )
        }
        columnStatsMap
      }
    }
  }
}

case class FakePlanWithStats(v1Stats: logical.Statistics) extends LogicalPlan {
  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = throw new UnsupportedOperationException
  override def stats: logical.Statistics = v1Stats
}
