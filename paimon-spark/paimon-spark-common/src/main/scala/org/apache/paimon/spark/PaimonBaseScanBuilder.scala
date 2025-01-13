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
import org.apache.paimon.table.Table

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

abstract class PaimonBaseScanBuilder(table: Table)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with Logging {

  protected var requiredSchema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())

  protected var pushedPredicates: Array[(Filter, Predicate)] = Array.empty

  protected var partitionFilters: Array[Filter] = Array.empty

  protected var postScanFilters: Array[Filter] = Array.empty

  protected var pushDownLimit: Option[Int] = None

  override def build(): Scan = {
    PaimonScan(table, requiredSchema, pushedPredicates.map(_._2), partitionFilters, pushDownLimit)
  }

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning. <p> Rows
   * should be returned from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val pushable = mutable.ArrayBuffer.empty[(Filter, Predicate)]
    val postScan = mutable.ArrayBuffer.empty[Filter]
    val partitionFilter = mutable.ArrayBuffer.empty[Filter]

    val converter = new SparkFilterConverter(table.rowType)
    val visitor = new PartitionPredicateVisitor(table.partitionKeys())
    filters.foreach {
      filter =>
        val predicate = converter.convertIgnoreFailure(filter)
        if (predicate == null) {
          postScan.append(filter)
        } else {
          pushable.append((filter, predicate))
          if (predicate.visit(visitor)) {
            partitionFilter.append(filter)
          } else {
            postScan.append(filter)
          }
        }
    }

    if (pushable.nonEmpty) {
      this.pushedPredicates = pushable.toArray
    }
    if (partitionFilter.nonEmpty) {
      this.partitionFilters = partitionFilter.toArray
    }
    if (postScan.nonEmpty) {
      this.postScanFilters = postScan.toArray
    }
    postScan.toArray
  }

  override def pushedFilters(): Array[Filter] = {
    pushedPredicates.map(_._1)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
