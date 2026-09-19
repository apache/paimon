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

package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.spark.{SparkTable, SparkV2FilterConverter}
import org.apache.paimon.spark.catalyst.plans.logical.PaimonTableValuedFunctions
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.{FullTextSearchTable, HybridSearchTable, InnerTable, VectorSearchTable}

import org.apache.spark.sql.PaimonUtils.translateFilterV2
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

/**
 * Fails a static vector / hybrid / full-text search whose WHERE carries a residual on
 * searched-table columns that cannot be pushed into Paimon. Such a residual stays a Spark filter
 * above the search, whose result is already truncated to the top-K, so post-filtering it can only
 * drop rows and never refill the ones displaced out of the top-K. The lateral form is handled by
 * [[PushDownLateralVectorSearchFilter]].
 */
object CheckUnpushableSearchFilter extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case filter @ Filter(condition, child) if condition.resolved =>
      relationTableAndOutput(child).foreach {
        case (table, output) =>
          searchInnerTable(table).foreach {
            innerTable =>
              val converter = SparkV2FilterConverter(innerTable.rowType())
              val dataColumns = AttributeSet(
                output.filterNot(
                  attr => PaimonMetadataColumn.VECTOR_SEARCH_META_COLUMN_NAMES.contains(attr.name)))
              val residuals = splitConjunctivePredicates(condition).filter {
                predicate =>
                  predicate.references.nonEmpty &&
                  predicate.references.intersect(dataColumns).nonEmpty &&
                  translateFilterV2(predicate).flatMap(converter.convert(_)).isEmpty
              }
              if (residuals.nonEmpty) {
                PaimonTableValuedFunctions.failUnpushableSearchFilter(residuals.map(_.sql))
              }
          }
      }
      filter
  }

  private def relationTableAndOutput(plan: LogicalPlan): Option[(Table, Seq[Attribute])] =
    plan match {
      case relation: DataSourceV2Relation => Some((relation.table, relation.output))
      case scan: DataSourceV2ScanRelation => Some((scan.relation.table, scan.output))
      case _ => None
    }

  private def searchInnerTable(table: Table): Option[InnerTable] = table match {
    case st: SparkTable =>
      st.table match {
        case vst: VectorSearchTable => Some(vst.origin())
        case hst: HybridSearchTable => Some(hst.origin())
        case ftst: FullTextSearchTable => Some(ftst.origin())
        case _ => None
      }
    case _ => None
  }
}
