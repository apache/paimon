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

  override def filterAttributes(): Array[NamedReference] = {
    val requiredFields = readBuilder.readType().getFieldNames.asScala
    table
      .partitionKeys()
      .asScala
      .toArray
      .filter(requiredFields.contains)
      .map(fieldReference)
  }

  override def filter(filters: Array[Filter]): Unit = {
    val partitionType = table.rowType().project(table.partitionKeys())
    val converter = new SparkFilterConverter(partitionType)
    val runtimePartitionFilters = filters.toSeq
      .map(converter.convertIgnoreFailure)
      .filter(_ != null)
      .map(PartitionPredicate.fromPredicate(partitionType, _))
    if (runtimePartitionFilters.nonEmpty) {
      pushedRuntimePartitionFilters.appendAll(runtimePartitionFilters)
      readBuilder.withPartitionFilter(
        PartitionPredicate.and(
          (pushedPartitionFilters ++ pushedRuntimePartitionFilters).toList.asJava))
      // set inputPartitions null to trigger to get the new splits.
      _inputPartitions = null
      _inputSplits = null
    }
  }
}
