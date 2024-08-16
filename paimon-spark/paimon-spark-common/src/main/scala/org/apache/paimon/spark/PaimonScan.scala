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

import org.apache.paimon.predicate.Predicate
import org.apache.paimon.table.{BucketMode, FileStoreTable, Table}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference, Transform}
import org.apache.spark.sql.connector.read.{SupportsReportPartitioning, SupportsRuntimeFiltering}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.sources.{Filter, In}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class PaimonScan(
    table: Table,
    requiredSchema: StructType,
    filters: Seq[Predicate],
    reservedFilters: Seq[Filter],
    pushDownLimit: Option[Int],
    bucketedScanDisabled: Boolean = false)
  extends PaimonBaseScan(table, requiredSchema, filters, reservedFilters, pushDownLimit)
  with SupportsRuntimeFiltering
  with SupportsReportPartitioning {

  def disableBucketedScan(): PaimonScan = {
    copy(bucketedScanDisabled = true)
  }

  @transient
  private lazy val extractBucketTransform: Option[Transform] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val bucketSpec = fileStoreTable.bucketSpec()
        if (bucketSpec.getBucketMode != BucketMode.HASH_FIXED) {
          None
        } else if (bucketSpec.getBucketKeys.size() > 1) {
          None
        } else {
          // Spark does not support bucket with several input attributes,
          // so we only support one bucket key case.
          assert(bucketSpec.getNumBuckets > 0)
          assert(bucketSpec.getBucketKeys.size() == 1)
          val bucketKey = bucketSpec.getBucketKeys.get(0)
          if (requiredSchema.exists(f => conf.resolver(f.name, bucketKey))) {
            Some(Expressions.bucket(bucketSpec.getNumBuckets, bucketKey))
          } else {
            None
          }
        }

      case _ => None
    }
  }

  override def outputPartitioning: Partitioning = {
    extractBucketTransform
      .map(bucket => new KeyGroupedPartitioning(Array(bucket), lazyInputPartitions.size))
      .getOrElse(new UnknownPartitioning(0))
  }

  override def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
    if (
      bucketedScanDisabled || !conf.v2BucketingEnabled || extractBucketTransform.isEmpty ||
      splits.exists(!_.isInstanceOf[DataSplit])
    ) {
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
    val converter = new SparkFilterConverter(table.rowType())
    val partitionFilter = filters.flatMap {
      case in @ In(attr, _) if table.partitionKeys().contains(attr) =>
        Some(converter.convert(in))
      case _ => None
    }
    if (partitionFilter.nonEmpty) {
      this.runtimeFilters = filters
      readBuilder.withFilter(partitionFilter.head)
      // set inputPartitions null to trigger to get the new splits.
      inputPartitions = null
    }
  }

}
