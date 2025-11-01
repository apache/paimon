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

import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.{SupportsPushDownLimit, SupportsPushDownV2Filters}
import org.apache.spark.sql.sources.Filter

import java.util.{List => JList}

import scala.collection.mutable

/** Base trait for Paimon scan push down. */
trait PaimonBasePushDown extends SupportsPushDownV2Filters with SupportsPushDownLimit {

  protected var partitionKeys: JList[String]
  protected var rowType: RowType

  private var pushedSparkPredicates = Array.empty[SparkPredicate]
  protected var pushedPaimonPredicates: Array[Predicate] = Array.empty
  protected var reservedFilters: Array[Filter] = Array.empty
  protected var hasPostScanPredicates = false
  protected var pushDownLimit: Option[Int] = None

  override def pushPredicates(predicates: Array[SparkPredicate]): Array[SparkPredicate] = {
    val pushable = mutable.ArrayBuffer.empty[(SparkPredicate, Predicate)]
    val postScan = mutable.ArrayBuffer.empty[SparkPredicate]
    val reserved = mutable.ArrayBuffer.empty[Filter]

    val converter = SparkV2FilterConverter(rowType)
    val visitor = new PartitionPredicateVisitor(partitionKeys)
    predicates.foreach {
      predicate =>
        converter.convert(predicate) match {
          case Some(paimonPredicate) =>
            pushable.append((predicate, paimonPredicate))
            if (paimonPredicate.visit(visitor)) {
              // We need to filter the stats using filter instead of predicate.
              PaimonUtils.filterV2ToV1(predicate).map(reserved.append(_))
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
}
