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

import org.apache.paimon.spark.PostponeMergeInputScan
import org.apache.paimon.spark.execution.PostponeMergeOnReadExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REPARTITION_BY_NUM, ShuffleExchangeExec}

/** Prevents AQE from coalescing reducers which read real buckets behind carrier markers. */
object DisablePostponeCarrierShuffleCoalescing extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case merge: PostponeMergeOnReadExec =>
        merge.mapChildren(disableCoalescing)
    }
  }

  private def disableCoalescing(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case exchange: ShuffleExchangeExec
          if exchange.shuffleOrigin == ENSURE_REQUIREMENTS &&
            containsPostponeCarrierScan(exchange.child) =>
        withFixedPartitionCount(exchange)
    }
  }

  private def containsPostponeCarrierScan(plan: SparkPlan): Boolean = {
    plan.find {
      case scan: BatchScanExec => scan.scan.isInstanceOf[PostponeMergeInputScan]
      case _ => false
    }.isDefined
  }

  private def withFixedPartitionCount(exchange: ShuffleExchangeExec): ShuffleExchangeExec = {
    // Replace only shuffleOrigin, preserving version-specific constructor fields.
    val arguments = exchange.productIterator.map(_.asInstanceOf[AnyRef]).toArray
    arguments(2) = REPARTITION_BY_NUM
    val copied = exchange.makeCopy(arguments).asInstanceOf[ShuffleExchangeExec]
    copied.copyTagsFrom(exchange)
    copied
  }
}
