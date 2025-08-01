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

import org.apache.paimon.CoreOptions.BucketFunctionType
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.spark.commands.BucketExpression.quote
import org.apache.paimon.table.{BucketMode, FileStoreTable, InnerTable, Table}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.{SupportsReportOrdering, SupportsReportPartitioning, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class PaimonScan(
    table: InnerTable,
    requiredSchema: StructType,
    filters: Seq[Predicate],
    reservedFilters: Seq[Filter],
    override val pushDownLimit: Option[Int],
    bucketedScanDisabled: Boolean = false)
  extends PaimonBaseScan(table, requiredSchema, filters, reservedFilters, pushDownLimit)
  with SupportsRuntimeV2Filtering
  with SupportsReportPartitioning
  with SupportsReportOrdering {

  def disableBucketedScan(): PaimonScan = {
    copy(bucketedScanDisabled = true)
  }

  @transient
  private lazy val extractBucketTransform: Option[Transform] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val bucketSpec = fileStoreTable.bucketSpec()
        // todo introduce bucket transform for different bucket function type
        if (
          bucketSpec.getBucketMode != BucketMode.HASH_FIXED || coreOptions
            .bucketFunctionType() != BucketFunctionType.DEFAULT
        ) {
          None
        } else if (bucketSpec.getBucketKeys.size() > 1) {
          None
        } else {
          // Spark does not support bucket with several input attributes,
          // so we only support one bucket key case.
          assert(bucketSpec.getNumBuckets > 0)
          assert(bucketSpec.getBucketKeys.size() == 1)
          extractBucketNumber() match {
            case Some(num) =>
              val bucketKey = bucketSpec.getBucketKeys.get(0)
              if (requiredSchema.exists(f => conf.resolver(f.name, bucketKey))) {
                Some(Expressions.bucket(num, quote(bucketKey)))
              } else {
                None
              }

            case _ => None
          }
        }

      case _ => None
    }
  }

  /**
   * Extract the bucket number from the splits only if all splits have the same totalBuckets number.
   */
  private def extractBucketNumber(): Option[Int] = {
    val splits = getOriginSplits
    if (splits.exists(!_.isInstanceOf[DataSplit])) {
      None
    } else {
      val deduplicated =
        splits.map(s => Option(s.asInstanceOf[DataSplit].totalBuckets())).toSeq.distinct

      deduplicated match {
        case Seq(Some(num)) => Some(num)
        case _ => None
      }
    }
  }

  private def shouldDoBucketedScan: Boolean = {
    !bucketedScanDisabled && conf.v2BucketingEnabled && extractBucketTransform.isDefined
  }

  // Since Spark 3.3
  override def outputPartitioning: Partitioning = {
    extractBucketTransform
      .map(bucket => new KeyGroupedPartitioning(Array(bucket), lazyInputPartitions.size))
      .getOrElse(new UnknownPartitioning(0))
  }

  // Since Spark 3.4
  override def outputOrdering(): Array[SortOrder] = {
    if (
      !shouldDoBucketedScan || lazyInputPartitions.exists(
        !_.isInstanceOf[PaimonBucketedInputPartition])
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

    val allSplitsKeepOrdering = lazyInputPartitions.toSeq
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

  override def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
    if (!shouldDoBucketedScan || splits.exists(!_.isInstanceOf[DataSplit])) {
      return super.getInputPartitions(splits)
    }

    splits
      .map(_.asInstanceOf[DataSplit])
      .groupBy(_.bucket())
      .map {
        case (bucket, groupedSplits) =>
          PaimonBucketedInputPartition(groupedSplits, bucket)
      }
      .toSeq
  }

  // Since Spark 3.2
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
    val converter = SparkV2FilterConverter(table.rowType())
    val partitionKeys = table.partitionKeys().asScala.toSeq
    val partitionFilter = predicates.flatMap {
      case p if SparkV2FilterConverter.isSupportedRuntimeFilter(p, partitionKeys) =>
        converter.convert(p)
      case _ => None
    }
    if (partitionFilter.nonEmpty) {
      readBuilder.withFilter(partitionFilter.toList.asJava)
      // set inputPartitions null to trigger to get the new splits.
      inputPartitions = null
      inputSplits = null
    }
  }
}
