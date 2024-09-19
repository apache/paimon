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
    (newV2ScanRelation, cachedV2ScanRelation) match {
      case (
            DataSourceV2ScanRelation(
              newRelation,
              newScan: PaimonScan,
              newOutput,
              newPartitioning,
              newOrdering),
            DataSourceV2ScanRelation(
              cachedRelation,
              cachedScan: PaimonScan,
              _,
              cachedPartitioning,
              cacheOrdering)) =>
        checkIdenticalPlans(newRelation, cachedRelation).flatMap {
          outputMap =>
            if (
              samePartitioning(newPartitioning, cachedPartitioning, outputMap) && sameOrdering(
                newOrdering.map(t=>t.toIndexedSeq),
                cacheOrdering.map(t=>t.toIndexedSeq),
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
                  val newV2ScanRelation = DataSourceV2ScanRelation(
                    cachedRelation,
                    mergedScan,
                    mergedOutput,
                    cachedPartitioning)

                  val mergedOutputNameMap = mergedOutput.map(a => a.name -> a).toMap
                  val newOutputMap =
                    AttributeMap(newOutput.map(a => a -> mergedOutputNameMap(a.name).toAttribute))

                  newV2ScanRelation -> newOutputMap
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
