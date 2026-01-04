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
import org.apache.paimon.predicate.{PartitionPredicateVisitor, Predicate, TopN}
import org.apache.paimon.table.{SpecialFields, Table}
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.{SupportsPushDownLimit, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.types.StructType

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Base Scan builder. */
abstract class PaimonBaseScanBuilder
  extends SupportsPushDownRequiredColumns
  with SupportsPushDownV2Filters
  with SupportsPushDownLimit {

  val table: Table
  val partitionKeys: JList[String] = table.partitionKeys()
  val rowType: RowType = table.rowType()
  val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  private var pushedSparkPredicates = Array.empty[SparkPredicate]
  protected var hasPostScanPredicates = false

  protected var pushedPartitionFilters: Array[PartitionPredicate] = Array.empty
  protected var pushedDataFilters: Array[Predicate] = Array.empty
  protected var pushedLimit: Option[Int] = None
  protected var pushedTopN: Option[TopN] = None

  protected var requiredSchema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushPredicates(predicates: Array[SparkPredicate]): Array[SparkPredicate] = {
    val pushable = mutable.ArrayBuffer.empty[SparkPredicate]
    val pushablePartitionDataFilters = mutable.ArrayBuffer.empty[Predicate]
    val pushableDataFilters = mutable.ArrayBuffer.empty[Predicate]
    val postScan = mutable.ArrayBuffer.empty[SparkPredicate]

    var newRowType = rowType
    if (coreOptions.rowTrackingEnabled() && coreOptions.dataEvolutionEnabled()) {
      newRowType = SpecialFields.rowTypeWithRowTracking(newRowType);
    }
    val converter = SparkV2FilterConverter(newRowType)
    val partitionPredicateVisitor = new PartitionPredicateVisitor(partitionKeys)
    predicates.foreach {
      predicate =>
        converter.convert(predicate) match {
          case Some(paimonPredicate) =>
            pushable.append(predicate)
            if (paimonPredicate.visit(partitionPredicateVisitor)) {
              pushablePartitionDataFilters.append(paimonPredicate)
            } else {
              pushableDataFilters.append(paimonPredicate)
              postScan.append(predicate)
            }
          case None =>
            postScan.append(predicate)
        }
    }

    if (pushable.nonEmpty) {
      this.pushedSparkPredicates = pushable.toArray
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

  override def pushedPredicates: Array[SparkPredicate] = {
    pushedSparkPredicates
  }

  override def pushLimit(limit: Int): Boolean = {
    // It is safe, since we will do nothing if it is the primary table and the split is not `rawConvertible`
    pushedLimit = Some(limit)
    // just make the best effort to push down limit
    false
  }
}
