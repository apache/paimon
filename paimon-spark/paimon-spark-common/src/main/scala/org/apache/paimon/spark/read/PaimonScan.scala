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
import org.apache.paimon.predicate.{Predicate, TopN, VectorSearch}
import org.apache.paimon.spark.SparkV2FilterConverter
import org.apache.paimon.table.{FileStoreTable, InnerTable}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.{SupportsReportOrdering, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class PaimonScan(
    table: InnerTable,
    requiredSchema: StructType,
    pushedPartitionFilters: Seq[PartitionPredicate],
    pushedDataFilters: Seq[Predicate],
    override val pushedLimit: Option[Int],
    override val pushedTopN: Option[TopN],
    override val pushedVectorSearch: Option[VectorSearch],
    bucketedScanDisabled: Boolean = false)
  extends PaimonBaseScan(table)
  with SupportsRuntimeV2Filtering
  with SupportsReportOrdering {

  override def filterAttributes(): Array[NamedReference] = {
    val requiredFields = readBuilder.readType().getFieldNames.asScala
    table
      .partitionKeys()
      .asScala
      .toArray
      .filter(requiredFields.contains)
      .map(fieldReference)
  }

  override def filter(predicates: Array[SparkPredicate]): Unit = {
    val partitionType = table.rowType().project(table.partitionKeys())
    val converter = SparkV2FilterConverter(partitionType)
    val runtimePartitionFilters = predicates.toSeq
      .flatMap(converter.convert(_))
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

  // Since Spark 3.4
  override def outputOrdering(): Array[SortOrder] = {
    if (
      !shouldDoBucketedScan || inputPartitions.exists(!_.isInstanceOf[PaimonBucketedInputPartition])
    ) {
      return Array.empty
    }

    val primaryKeys = table match {
      case fileStoreTable: FileStoreTable => fileStoreTable.primaryKeys().asScala
      case _ => Seq.empty
    }
    if (primaryKeys.isEmpty) {
      return Array.empty
    }

    val allSplitsKeepOrdering = inputPartitions.toSeq
      .map(_.asInstanceOf[PaimonBucketedInputPartition])
      .map(_.splits.asInstanceOf[Seq[DataSplit]])
      .forall {
        splits =>
          // Only support report ordering if all matches:
          // - one `Split` per InputPartition (TODO: Re-construct splits using minKey/maxKey)
          // - `Split` is not rawConvertible so that the merge read can happen
          // - `Split` only contains one data file so it always sorted even without merge read
          splits.size < 2 && splits.forall {
            split => !split.rawConvertible() || split.dataFiles().size() < 2
          }
      }
    if (!allSplitsKeepOrdering) {
      return Array.empty
    }

    // Multi-primary keys are fine:
    // `Array(a, b)` satisfies the required ordering `Array(a)`
    primaryKeys
      .map(Expressions.identity)
      .map {
        sortExpr =>
          // Primary key can not be null, the null ordering is no matter.
          Expressions.sort(sortExpr, SortDirection.ASCENDING)
      }
      .toArray
  }
}
