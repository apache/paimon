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

import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate, PredicateBuilder}
import org.apache.paimon.spark.aggregate.{AggregatePushDownUtils, LocalAggregator}
import org.apache.paimon.table.{FileStoreTable, Table}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates, SupportsPushDownLimit, SupportsPushDownV2Filters}
import org.apache.spark.sql.sources.Filter

import scala.collection.JavaConverters._
import scala.collection.mutable

class PaimonScanBuilder(table: Table)
  extends PaimonBaseScanBuilder(table)
  with SupportsPushDownV2Filters
  with SupportsPushDownLimit
  with SupportsPushDownAggregates {

  private var localScan: Option[Scan] = None

  private var pushedSparkPredicates = Array.empty[SparkPredicate]

  /** Pushes down filters, and returns filters that need to be evaluated after scanning. */
  override def pushPredicates(predicates: Array[SparkPredicate]): Array[SparkPredicate] = {
    val pushable = mutable.ArrayBuffer.empty[(SparkPredicate, Predicate)]
    val postScan = mutable.ArrayBuffer.empty[SparkPredicate]
    val reserved = mutable.ArrayBuffer.empty[Filter]

    val converter = SparkV2FilterConverter(table.rowType)
    val visitor = new PartitionPredicateVisitor(table.partitionKeys())
    predicates.foreach {
      predicate =>
        converter.convert(predicate) match {
          case Some(paimonPredicate) =>
            pushable.append((predicate, paimonPredicate))
            if (paimonPredicate.visit(visitor)) {
              // We need to filter the stats using filter instead of predicate.
              reserved.append(PaimonUtils.filterV2ToV1(predicate).get)
            } else {
              postScan.append(predicate)
            }
          case None =>
            postScan.append(predicate)
        }
    }

    if (pushable.nonEmpty) {
      this.pushedSparkPredicates = pushable.map(_._1).toArray
      this.pushedPaimonPredicates = pushable.map(_._2).toArray
    }
    if (reserved.nonEmpty) {
      this.reservedFilters = reserved.toArray
    }
    if (postScan.nonEmpty) {
      this.hasPostScanPredicates = true
    }
    postScan.toArray
  }

  override def pushedPredicates: Array[SparkPredicate] = {
    pushedSparkPredicates
  }

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

    if (!table.isInstanceOf[FileStoreTable]) {
      return false
    }

    // Only support when there is no post scan predicates.
    if (hasPostScanPredicates) {
      return false
    }

    val readBuilder = table.newReadBuilder
    if (pushedPaimonPredicates.nonEmpty) {
      val pushedPartitionPredicate = PredicateBuilder.and(pushedPaimonPredicates.toList.asJava)
      readBuilder.withFilter(pushedPartitionPredicate)
    }
    val dataSplits = if (AggregatePushDownUtils.hasMinMaxAggregation(aggregation)) {
      readBuilder.newScan().plan().splits().asScala.map(_.asInstanceOf[DataSplit])
    } else {
      readBuilder.dropStats().newScan().plan().splits().asScala.map(_.asInstanceOf[DataSplit])
    }
    if (AggregatePushDownUtils.canPushdownAggregation(table, aggregation, dataSplits.toSeq)) {
      val aggregator = new LocalAggregator(table.asInstanceOf[FileStoreTable])
      aggregator.initialize(aggregation)
      dataSplits.foreach(aggregator.update)
      localScan = Some(
        PaimonLocalScan(
          aggregator.result(),
          aggregator.resultSchema(),
          table,
          pushedPaimonPredicates)
      )
      true
    } else {
      false
    }
  }

  override def build(): Scan = {
    if (localScan.isDefined) {
      localScan.get
    } else {
      super.build()
    }
  }
}
