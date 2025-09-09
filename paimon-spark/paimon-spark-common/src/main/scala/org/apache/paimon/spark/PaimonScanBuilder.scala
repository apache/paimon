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

import org.apache.paimon.predicate._
import org.apache.paimon.predicate.SortValue.{NullOrdering, SortDirection}
import org.apache.paimon.spark.aggregate.AggregatePushDownUtils.tryPushdownAggregation
import org.apache.paimon.table.{FileStoreTable, InnerTable}

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions
import org.apache.spark.sql.connector.expressions.{NamedReference, SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

class PaimonScanBuilder(table: InnerTable)
  extends PaimonBaseScanBuilder(table)
  with SupportsPushDownV2Filters
  with SupportsPushDownLimit
  with SupportsPushDownAggregates
  with SupportsPushDownTopN {

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

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    if (hasPostScanPredicates) {
      return false
    }

    if (!table.isInstanceOf[FileStoreTable]) {
      return false
    }

    val sorts: List[SortValue] = orders
      .map(
        order => {
          val fieldName = order.expression() match {
            case nr: NamedReference => nr.fieldNames.mkString(".")
            case _ => return false
          }

          val rowType = table.rowType()
          if (rowType.notContainsField(fieldName)) {
            return false
          }

          val field = rowType.getField(fieldName)
          val ref = new FieldRef(field.id(), field.name(), field.`type`())

          val nullOrdering = order.nullOrdering() match {
            case expressions.NullOrdering.NULLS_LAST => NullOrdering.NULLS_LAST
            case expressions.NullOrdering.NULLS_FIRST => NullOrdering.NULLS_FIRST
            case _ => return false
          }

          val direction = order.direction() match {
            case expressions.SortDirection.DESCENDING => SortDirection.DESCENDING
            case expressions.SortDirection.ASCENDING => SortDirection.ASCENDING
            case _ => return false
          }

          new SortValue(ref, direction, nullOrdering)
        })
      .toList

    pushDownTopN = Some(new TopN(sorts.asJava, limit))

    // just make the best effort to push down TopN
    false
  }

  override def isPartiallyPushed: Boolean = super.isPartiallyPushed

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

    tryPushdownAggregation(table.asInstanceOf[FileStoreTable], aggregation, readBuilder) match {
      case Some(agg) =>
        localScan = Some(
          PaimonLocalScan(agg.result(), agg.resultSchema(), table, pushedPaimonPredicates)
        )
        true
      case None => false
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
