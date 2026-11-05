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

package org.apache.paimon.spark.read

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.PredicateVisitor
import org.apache.paimon.spark.{PaimonInputPartition, SparkFilterConverter}
import org.apache.paimon.table.source.Split

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.sources.Filter

import scala.collection.JavaConverters._

trait PaimonSupportsRuntimeFiltering extends BaseScan with SupportsRuntimeFiltering {

  private var _inputSplits: Array[Split] = _
  private var _inputPartitions: Seq[PaimonInputPartition] = _

  final override def inputSplits: Array[Split] = {
    if (_inputSplits == null) {
      _inputSplits = getInputSplits
    }
    _inputSplits
  }

  final override def inputPartitions: Seq[PaimonInputPartition] = {
    if (_inputPartitions == null) {
      _inputPartitions = getInputPartitions(inputSplits)
    }
    _inputPartitions
  }

  // Determine runtime-filterable fields once from the initial scan plan.
  // Do not recompute them from the remaining splits after runtime pruning.
  private lazy val runtimeFilterFields: Set[String] = {
    // Only ordinary scan output columns are eligible. Exclude partition columns
    // read solely to produce metadata.
    val requiredFields = readTableRowType.getFieldNames.asScala
    val requiredPartitionFields =
      table.partitionKeys().asScala.filter(requiredFields.contains).toSet
    if (coreOptions.queryAuthEnabled() && requiredPartitionFields.nonEmpty) {
      QueryAuthRuntimeFiltering.filterableFields(inputSplits, requiredPartitionFields)
    } else {
      requiredPartitionFields
    }
  }

  override def filterAttributes(): Array[NamedReference] = {
    table.partitionKeys().asScala.filter(runtimeFilterFields.contains).map(fieldReference).toArray
  }

  // Updates metrics after runtime pruning of the cached authorized split plan.
  protected def updateRuntimePruningMetrics(splits: Array[Split]): Unit = {}

  override def filter(filters: Array[Filter]): Unit = {
    val partitionType = table.rowType().project(table.partitionKeys())
    val converter = new SparkFilterConverter(partitionType)
    val runtimePartitionFilters = filters.toSeq
      .map(converter.convertIgnoreFailure)
      .filter(_ != null)
      .filter(
        predicate =>
          !coreOptions.queryAuthEnabled() ||
            PredicateVisitor
              .collectFieldNames(predicate)
              .asScala
              .forall(runtimeFilterFields.contains))
      .map(PartitionPredicate.fromPredicate(partitionType, _))
    if (runtimePartitionFilters.nonEmpty) {
      pushedRuntimePartitionFilters.appendAll(runtimePartitionFilters)
      if (coreOptions.queryAuthEnabled()) {
        // Reuse the authorized plan and retain its wrappers and chain dependencies. Re-planning
        // would authorize again and route visible join keys through raw partition selection.
        _inputSplits = QueryAuthRuntimeFiltering.filter(inputSplits, runtimePartitionFilters)
        updateRuntimePruningMetrics(_inputSplits)
      } else {
        readBuilder.withPartitionFilter(
          PartitionPredicate.and(
            (pushedPartitionFilters ++ pushedRuntimePartitionFilters).toList.asJava))
        _inputSplits = null
      }
      _inputPartitions = null
    }
  }
}
