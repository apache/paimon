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

import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate, RowIdPredicateVisitor}
import org.apache.paimon.types.{DataField, DataTypes, RowType}

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.{SupportsPushDownLimit, SupportsPushDownV2Filters}
import org.apache.spark.sql.sources.Filter

import java.lang.{Long => JLong}
import java.util.{ArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Base trait for Paimon scan push down. */
trait PaimonBasePushDown extends SupportsPushDownV2Filters with SupportsPushDownLimit {

  protected var partitionKeys: JList[String]
  protected var rowType: RowType

  private var pushedSparkPredicates = Array.empty[SparkPredicate]
  protected var pushedRowIds: Array[JLong] = null
  protected var pushedPaimonPredicates: Array[Predicate] = Array.empty
  protected var reservedFilters: Array[Filter] = Array.empty
  protected var hasPostScanPredicates = false
  protected var pushDownLimit: Option[Int] = None

  override def pushPredicates(predicates: Array[SparkPredicate]): Array[SparkPredicate] = {
    val pushableSparkFilters = mutable.ArrayBuffer.empty[SparkPredicate]
    val pushablePaimonPredicates = mutable.ArrayBuffer.empty[Predicate]
    var pushableRowIds: mutable.Set[JLong] = null
    val postScan = mutable.ArrayBuffer.empty[SparkPredicate]
    val reserved = mutable.ArrayBuffer.empty[Filter]

    val dataFieldWithRowId = new ArrayList[DataField](rowType.getFields)
    dataFieldWithRowId.add(new DataField(rowType.getFieldCount, "_ROW_ID", DataTypes.BIGINT()))
    val rowTypeWithRowId = rowType.copy(dataFieldWithRowId)
    val converter = SparkV2FilterConverter(rowTypeWithRowId)
    val partitionVisitor = new PartitionPredicateVisitor(partitionKeys)
    val rowIdVisitor = new RowIdPredicateVisitor

    predicates.foreach {
      predicate =>
        converter.convert(predicate) match {
          case Some(paimonPredicate) =>
            if (paimonPredicate.visit(partitionVisitor)) {
              pushableSparkFilters.append(predicate)
              pushablePaimonPredicates.append(paimonPredicate)
              // We need to filter the stats using filter instead of predicate.
              PaimonUtils.filterV2ToV1(predicate).map(reserved.append(_))
            } else if (paimonPredicate.visit(rowIdVisitor) != null) {
              pushableSparkFilters.append(predicate)
              if (pushableRowIds == null) {
                pushableRowIds = paimonPredicate.visit(rowIdVisitor).asScala
              } else {
                pushableRowIds.retain(paimonPredicate.visit(rowIdVisitor).asScala)
              }
            } else {
              postScan.append(predicate)
            }
          case None =>
            postScan.append(predicate)
        }
    }

    if (pushableSparkFilters.nonEmpty) {
      this.pushedSparkPredicates = pushableSparkFilters.toArray
    }
    if (pushablePaimonPredicates.nonEmpty) {
      this.pushedPaimonPredicates = pushablePaimonPredicates.toArray
    }
    if (pushableRowIds != null) {
      this.pushedRowIds = pushableRowIds.toArray
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
