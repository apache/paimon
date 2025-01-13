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

package org.apache.paimon.spark

import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.aggregate.LocalAggregator
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates, SupportsPushDownLimit}

import scala.collection.JavaConverters._

class PaimonScanBuilder(table: Table)
  extends PaimonBaseScanBuilder(table)
  with SupportsPushDownLimit
  with SupportsPushDownAggregates {
  private var localScan: Option[Scan] = None

  override def pushLimit(limit: Int): Boolean = {
    // It is safe, since we will do nothing if it is the primary table and the split is not `rawConvertible`
    pushDownLimit = Some(limit)
    // just make the best effort to push down limit
    false
  }

  override def supportCompletePushDown(aggregation: Aggregation): Boolean = {
    // for now, we only support complete push down, so there is no difference with `pushAggregation`
    pushAggregation(aggregation)
  }

  // Spark does not support push down aggregation for streaming scan.
  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (localScan.isDefined) {
      return true
    }

    // Only support with push down partition filter
    if (postScanFilters.nonEmpty) {
      return false
    }

    val aggregator = new LocalAggregator(table)
    if (!aggregator.pushAggregation(aggregation)) {
      return false
    }

    val readBuilder = table.newReadBuilder
    if (pushedPredicates.nonEmpty) {
      val pushedPartitionPredicate = PredicateBuilder.and(pushedPredicates.map(_._2): _*)
      readBuilder.withFilter(pushedPartitionPredicate)
    }
    val dataSplits = readBuilder.newScan().plan().splits().asScala.map(_.asInstanceOf[DataSplit])
    if (!dataSplits.forall(_.mergedRowCountAvailable())) {
      return false
    }
    dataSplits.foreach(aggregator.update)
    localScan = Some(
      PaimonLocalScan(
        aggregator.result(),
        aggregator.resultSchema(),
        table,
        pushedPredicates.map(_._1)))
    true
  }

  override def build(): Scan = {
    if (localScan.isDefined) {
      localScan.get
    } else {
      super.build()
    }
  }
}
