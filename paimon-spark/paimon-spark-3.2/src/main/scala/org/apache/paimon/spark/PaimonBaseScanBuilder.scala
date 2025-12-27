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

import org.apache.paimon.CoreOptions
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates
import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate}
import org.apache.paimon.table.SpecialFields.rowTypeWithRowTracking
import org.apache.paimon.table.Table
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.read.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Base Scan builder. */
abstract class PaimonBaseScanBuilder
  extends SupportsPushDownRequiredColumns
  with SupportsPushDownFilters {

  val table: Table
  val partitionKeys: JList[String] = table.partitionKeys()
  val rowType: RowType = table.rowType()
  val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  private var pushedSparkFilters = Array.empty[Filter]
  protected var hasPostScanPredicates = false

  protected var pushedPartitionFilters: Array[PartitionPredicate] = Array.empty
  protected var pushedDataFilters: Array[Predicate] = Array.empty

  protected var requiredSchema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning. <p> Rows
   * should be returned from the data source if and only if all the filters match. That is, filters
   * must be interpreted as ANDed together.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val pushable = mutable.ArrayBuffer.empty[Filter]
    val pushablePartitionDataFilters = mutable.ArrayBuffer.empty[Predicate]
    val pushableDataFilters = mutable.ArrayBuffer.empty[Predicate]
    val postScan = mutable.ArrayBuffer.empty[Filter]

    var newRowType = rowType
    if (coreOptions.rowTrackingEnabled() && coreOptions.dataEvolutionEnabled()) {
      newRowType = rowTypeWithRowTracking(newRowType);
    }
    val converter = new SparkFilterConverter(newRowType)
    val partitionPredicateVisitor = new PartitionPredicateVisitor(partitionKeys)
    filters.foreach {
      filter =>
        val predicate = converter.convertIgnoreFailure(filter)
        if (predicate == null) {
          postScan.append(filter)
        } else {
          pushable.append(filter)
          if (predicate.visit(partitionPredicateVisitor)) {
            pushablePartitionDataFilters.append(predicate)
          } else {
            pushableDataFilters.append(predicate)
            postScan.append(filter)
          }
        }
    }

    if (pushable.nonEmpty) {
      this.pushedSparkFilters = pushable.toArray
    }
    if (pushablePartitionDataFilters.nonEmpty) {
      val pair = splitPartitionPredicatesAndDataPredicates(
        pushablePartitionDataFilters.asJava,
        rowType,
        partitionKeys)
      assert(pair.getRight.isEmpty)
      assert(pair.getLeft.isPresent)
      this.pushedPartitionFilters = Array(pair.getLeft.get())
    }
    if (pushableDataFilters.nonEmpty) {
      this.pushedDataFilters = pushableDataFilters.toArray
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
