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

import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.sources.Filter

import java.lang.{Long => JLong}
import java.util.{ArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Base trait for Paimon scan filter push down. */
trait PaimonBasePushDown extends SupportsPushDownFilters {

  protected var partitionKeys: JList[String]
  protected var rowType: RowType

  private var pushedSparkFilters = Array.empty[Filter]
  protected var pushedRowIds: Array[JLong] = null
  protected var pushedPaimonPredicates: Array[Predicate] = Array.empty
  protected var reservedFilters: Array[Filter] = Array.empty
  protected var hasPostScanPredicates = false

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning. <p> Rows
   * should be returned from the data source if and only if all the filters match. That is, filters
   * must be interpreted as ANDed together.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val pushableSparkFilters = mutable.ArrayBuffer.empty[Filter]
    val pushablePaimonPredicates = mutable.ArrayBuffer.empty[Predicate]
    var pushableRowIds: mutable.Set[JLong] = null
    val postScan = mutable.ArrayBuffer.empty[Filter]
    val reserved = mutable.ArrayBuffer.empty[Filter]

    val dataFieldWithRowId = new ArrayList[DataField](rowType.getFields)
    dataFieldWithRowId.add(new DataField(rowType.getFieldCount, "_ROW_ID", DataTypes.BIGINT()))
    val rowTypeWithRowId = rowType.copy(dataFieldWithRowId)
    val converter = new SparkFilterConverter(rowTypeWithRowId)
    val partitionVisitor = new PartitionPredicateVisitor(partitionKeys)
    val rowIdVisitor = new RowIdPredicateVisitor

    filters.foreach {
      filter =>
        val predicate = converter.convertIgnoreFailure(filter)
        if (predicate == null) {
          postScan.append(filter)
        } else {
          if (predicate.visit(partitionVisitor)) {
            pushableSparkFilters.append(filter)
            pushablePaimonPredicates.append(predicate)
            reserved.append(filter)
          } else if (predicate.visit(rowIdVisitor) != null) {
            pushableSparkFilters.append(filter)
            if (pushableRowIds == null) {
              pushableRowIds = predicate.visit(rowIdVisitor).asScala
            } else {
              pushableRowIds.retain(predicate.visit(rowIdVisitor).asScala)
            }
          } else {
            postScan.append(filter)
          }
        }
    }

    if (pushableSparkFilters.nonEmpty) {
      this.pushedSparkFilters = pushableSparkFilters.toArray
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

  override def pushedFilters(): Array[Filter] = {
    pushedSparkFilters
  }
}
