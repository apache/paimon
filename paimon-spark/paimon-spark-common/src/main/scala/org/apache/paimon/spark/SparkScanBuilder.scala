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
import org.apache.paimon.table.{AppendOnlyFileStoreTable, Table}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class SparkScanBuilder(table: Table)
  extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit
  with Logging {

  private var predicates: Option[Predicate] = None

  private var pushed: Option[Array[Filter]] = None

  private var projectedIndexes: Option[Array[Int]] = None

  private var pushDownLimit: Option[Int] = None

  override def build(): Scan = {
    val readBuilder = table.newReadBuilder()

    projectedIndexes.foreach(readBuilder.withProjection)
    predicates.foreach(readBuilder.withFilter)
    pushDownLimit.foreach(readBuilder.withLimit)

    new SparkScan(table, readBuilder);
  }

  /**
   * Pushes down filters, and returns filters that need to be evaluated after scanning. <p> Rows
   * should be returned from the data source if and only if all of the filters match. That is,
   * filters must be interpreted as ANDed together.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val pushable = mutable.ArrayBuffer.empty[Filter]
    val postScan = mutable.ArrayBuffer.empty[Filter]
    val predicates = mutable.ArrayBuffer.empty[Predicate]

    val converter = new SparkFilterConverter(table.rowType)
    val visitor = new PartitionPredicateVisitor(table.partitionKeys())
    filters.foreach {
      filter =>
        try {
          val predicate = converter.convert(filter)
          pushable.append(filter)
          predicates.append(predicate)
          if (!predicate.visit(visitor)) postScan.append(filter)
        } catch {
          case e: UnsupportedOperationException =>
            logWarning(e.getMessage)
            postScan.append(filter)
        }
    }

    if (predicates.nonEmpty) {
      this.predicates = Some(PredicateBuilder.and(predicates: _*))
    }
    this.pushed = Some(pushable.toArray)
    postScan.toArray
  }

  /**
   * Returns the filters that are pushed to the data source via {@link # pushFilters ( Filter [ ]
   * )}. <p> There are 3 kinds of filters: <ol> <li>pushable filters which don't need to be
   * evaluated again after scanning.</li> <li>pushable filters which still need to be evaluated
   * after scanning, e.g. parquet row group filter.</li> <li>non-pushable filters.</li> </ol> <p>
   * Both case 1 and 2 should be considered as pushed filters and should be returned by this method.
   * <p> It's possible that there is no filters in the query and {@link # pushFilters ( Filter [ ]
   * )} is never called, empty array should be returned for this case.
   */
  override def pushedFilters(): Array[Filter] = {
    pushed.getOrElse(Array.empty)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val pruneFields = requiredSchema.fieldNames
    val fieldNames = table.rowType.getFieldNames
    val projected = pruneFields.map(field => fieldNames.indexOf(field))
    this.projectedIndexes = Some(projected)
  }

  override def pushLimit(limit: Int): Boolean = {
    if (table.isInstanceOf[AppendOnlyFileStoreTable]) {
      pushDownLimit = Some(limit)
    }
    // just make a best effort to push down limit
    false
  }
}
