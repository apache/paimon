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

package org.apache.paimon.spark.execution.adaptive

import org.apache.paimon.spark.PaimonScan

import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeLike}

// spotless:off
/**
 * This rule is inspired from Spark [[DisableUnnecessaryBucketedScan]] but work for v2 scan.
 *
 * Disable unnecessary bucketed table scan based on actual physical query plan.
 * NOTE: this rule is designed to be applied right after [[EnsureRequirements]],
 * where all [[ShuffleExchangeLike]] and [[SortExec]] have been added to plan properly.
 *
 * When BUCKETING_ENABLED and AUTO_BUCKETED_SCAN_ENABLED are set to true, go through
 * query plan to check where bucketed table scan is unnecessary, and disable bucketed table
 * scan if:
 *
 * 1. The sub-plan from root to bucketed table scan, does not contain
 *    [[hasInterestingPartition]] operator.
 *
 * 2. The sub-plan from the nearest downstream [[hasInterestingPartition]] operator
 *    to the bucketed table scan and at least one [[ShuffleExchangeLike]].
 *
 * Examples:
 * 1. no [[hasInterestingPartition]] operator:
 *                Project
 *                   |
 *                 Filter
 *                   |
 *             Scan(t1: i, j)
 *  (bucketed on column j, DISABLE bucketed scan)
 *
 * 2. join:
 *         SortMergeJoin(t1.i = t2.j)
 *            /            \
 *        Sort(i)        Sort(j)
 *          /               \
 *      Shuffle(i)       Scan(t2: i, j)
 *        /         (bucketed on column j, enable bucketed scan)
 *   Scan(t1: i, j)
 * (bucketed on column j, DISABLE bucketed scan)
 *
 * 3. aggregate:
 *         HashAggregate(i, ..., Final)
 *                      |
 *                  Shuffle(i)
 *                      |
 *         HashAggregate(i, ..., Partial)
 *                      |
 *                    Filter
 *                      |
 *                  Scan(t1: i, j)
 *  (bucketed on column j, DISABLE bucketed scan)
 *
 * The idea of [[hasInterestingPartition]] is inspired from "interesting order" in
 * the paper "Access Path Selection in a Relational Database Management System"
 * (https://dl.acm.org/doi/10.1145/582095.582099).
 */
// spotless:on
object DisableUnnecessaryPaimonBucketedScan extends Rule[SparkPlan] {

  /**
   * Disable bucketed table scan with pre-order traversal of plan.
   *
   * @param hashInterestingPartition
   *   The traversed plan has operator with interesting partition.
   * @param hasExchange
   *   The traversed plan has [[Exchange]] operator.
   */
  private def disableBucketScan(
      plan: SparkPlan,
      hashInterestingPartition: Boolean,
      hasExchange: Boolean): SparkPlan = {
    plan match {
      case p if hasInterestingPartition(p) =>
        // Operator with interesting partition, propagates `hashInterestingPartition` as true
        // to its children, and resets `hasExchange`.
        p.mapChildren(disableBucketScan(_, hashInterestingPartition = true, hasExchange = false))
      case exchange: ShuffleExchangeLike =>
        // Exchange operator propagates `hasExchange` as true to its child.
        exchange.mapChildren(disableBucketScan(_, hashInterestingPartition, hasExchange = true))
      case batch: BatchScanExec =>
        val paimonBucketedScan = extractPaimonBucketedScan(batch)
        if (paimonBucketedScan.isDefined && (!hashInterestingPartition || hasExchange)) {
          val (batch, paimonScan) = paimonBucketedScan.get
          val newBatch = batch.copy(scan = paimonScan.withDisabledBucketedScan())
          newBatch.copyTagsFrom(batch)
          newBatch
        } else {
          batch
        }
      case p if canPassThrough(p) =>
        p.mapChildren(disableBucketScan(_, hashInterestingPartition, hasExchange))
      case other =>
        other.mapChildren(
          disableBucketScan(_, hashInterestingPartition = false, hasExchange = false))
    }
  }

  private def hasInterestingPartition(plan: SparkPlan): Boolean = {
    plan.requiredChildDistribution.exists {
      case _: ClusteredDistribution | AllTuples => true
      case _ => false
    }
  }

  /**
   * Check if the operator is allowed single-child operator. We may revisit this method later as we
   * probably can remove this restriction to allow arbitrary operator between bucketed table scan
   * and operator with interesting partition.
   */
  private def canPassThrough(plan: SparkPlan): Boolean = {
    plan match {
      case _: ProjectExec | _: FilterExec => true
      case s: SortExec if !s.global => true
      case partialAgg: BaseAggregateExec =>
        partialAgg.requiredChildDistributionExpressions.isEmpty
      case _ => false
    }
  }

  def extractPaimonBucketedScan(plan: SparkPlan): Option[(BatchScanExec, PaimonScan)] =
    plan match {
      case batch: BatchScanExec =>
        batch.scan match {
          case scan: PaimonScan if scan.lazyInputPartitions.forall(_.bucketed) =>
            Some((batch, scan))
          case _ => None
        }
      case _ => None
    }

  def apply(plan: SparkPlan): SparkPlan = {
    lazy val hasBucketedScan = plan.exists {
      case p if extractPaimonBucketedScan(p).isDefined => true
      case _ => false
    }

    if (!conf.v2BucketingEnabled || !conf.autoBucketedScanEnabled || !hasBucketedScan) {
      plan
    } else {
      disableBucketScan(plan, hashInterestingPartition = false, hasExchange = false)
    }
  }
}
