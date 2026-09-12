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

import org.apache.paimon.spark.PaimonScan

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, ExprId, ScalarSubquery, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

object MergePaimonScalarSubqueries extends MergePaimonScalarSubqueriesBase {

  override def tryMergeDataSourceV2ScanRelation(
      newV2ScanRelation: DataSourceV2ScanRelation,
      cachedV2ScanRelation: DataSourceV2ScanRelation)
      : Option[(LogicalPlan, AttributeMap[Attribute])] = {
    // Match by type and read fields through named accessors: Spark 4.2 (SPARK-56385) added a
    // sixth `pushedFilters` parameter, which breaks positional patterns.
    (newV2ScanRelation.scan, cachedV2ScanRelation.scan) match {
      case (newScan: PaimonScan, cachedScan: PaimonScan) =>
        val newRelation = newV2ScanRelation.relation
        val newOutput = newV2ScanRelation.output
        val newPartitioning = newV2ScanRelation.keyGroupedPartitioning
        val newOrdering = newV2ScanRelation.ordering
        val cachedRelation = cachedV2ScanRelation.relation
        val cachedPartitioning = cachedV2ScanRelation.keyGroupedPartitioning
        val cacheOrdering = cachedV2ScanRelation.ordering

        checkIdenticalPlans(newRelation, cachedRelation).flatMap {
          outputMap =>
            if (
              samePartitioning(newPartitioning, cachedPartitioning, outputMap) && sameOrdering(
                newOrdering,
                cacheOrdering,
                outputMap)
            ) {
              mergePaimonScan(newScan, cachedScan).map {
                mergedScan =>
                  val mergedAttributes = mergedScan
                    .readSchema()
                    .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
                  val cachedOutputNameMap = cachedRelation.output.map(a => a.name -> a).toMap
                  val mergedOutput =
                    mergedAttributes.map(a => cachedOutputNameMap.getOrElse(a.name, a))
                  val mergedV2ScanRelation =
                    cachedV2ScanRelation.copy(scan = mergedScan, output = mergedOutput)

                  val mergedOutputNameMap = mergedOutput.map(a => a.name -> a).toMap
                  val newOutputMap =
                    AttributeMap(newOutput.map(a => a -> mergedOutputNameMap(a.name).toAttribute))

                  mergedV2ScanRelation -> newOutputMap
              }
            } else {
              None
            }
        }

      case _ => None
    }
  }

  private def sameOrdering(
      newOrdering: Option[Seq[SortOrder]],
      cachedOrdering: Option[Seq[SortOrder]],
      outputAttrMap: AttributeMap[Attribute]): Boolean = {
    val mappedNewOrdering = newOrdering.map(_.map(mapAttributes(_, outputAttrMap)))
    mappedNewOrdering.map(_.map(_.canonicalized)) == cachedOrdering.map(_.map(_.canonicalized))
  }

  override protected def createScalarSubquery(plan: LogicalPlan, exprId: ExprId): ScalarSubquery = {
    ScalarSubquery(plan, exprId = exprId)
  }
}
