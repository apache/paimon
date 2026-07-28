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
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.PostponeMergeOnRead.{MergePlan, RealScanInfo}
import org.apache.paimon.table.{BucketMode, FileStoreTable, Table}
import org.apache.paimon.table.source.{PostponeMergePlan, PostponeMergeReadBuilder}

import scala.collection.JavaConverters._

final private[spark] class PostponeMergeOnRead(scan: PaimonBaseScan) {

  @transient private lazy val mergeReadBuilder = {
    val builder =
      PostponeMergeOnRead.createReadBuilder(scan.table, scan.pushedPartitionFilters.toArray)
    if (
      builder.isDefined &&
      (scan.pushedVectorSearch.isDefined ||
        scan.pushedHybridSearch.isDefined ||
        scan.pushedFullTextSearch.isDefined)
    ) {
      throw new UnsupportedOperationException(
        "Option 'postpone.merge-on-read' does not support vector, hybrid or full-text search.")
    }
    builder
  }

  @transient private var mergePlan: MergePlan = _

  def enabled: Boolean = PostponeMergeOnRead.usesCustomSource(scan.table)

  def plan(defaultBucketNum: Int): Option[MergePlan] = synchronized {
    if (!enabled) {
      return None
    }

    mergeReadBuilder.map {
      builder =>
        if (mergePlan == null) {
          if (scan.metadataFields.nonEmpty) {
            throw new UnsupportedOperationException(
              "Option 'postpone.merge-on-read' does not support metadata columns: " +
                scan.metadataFields.map(_.name).mkString(", "))
          }

          builder
            .withReadType(scan.readTableRowType)
            .withDefaultBucketNum(defaultBucketNum)
            .withMetricRegistry(scan.paimonMetricsRegistry)
          if (scan.pushedDataFilters.nonEmpty) {
            builder.withFilter(PredicateBuilder.and(scan.pushedDataFilters.toList.asJava))
          }

          val corePlan = builder.plan()
          scan.registerReadProtectionTagCleanup(builder.readProtectionTagName())
          val postponeFiles =
            corePlan.postponeSplits().asScala.iterator.map(_.dataFiles().size().toLong).sum
          scan.ensureNoFullScan(postponeFiles)
          val realScanInfo = RealScanInfo(
            scan.table.fullName,
            scan.description(),
            scan
              .reportDriverMetrics()
              .map(metric => metric.name() -> metric.value())
              .toMap)
          mergePlan =
            MergePlan(builder, corePlan, scan.coreOptions.blobAsDescriptor(), realScanInfo)
        }
        mergePlan
    }
  }
}

private[spark] object PostponeMergeOnRead {

  private[spark] def enabled(table: Table): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable => configured(fileStoreTable)
      case _ => false
    }
  }

  private[spark] def usesCustomSource(table: Table): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        configured(fileStoreTable) &&
        fileStoreTable.coreOptions().startupMode() != CoreOptions.StartupMode.COMPACTED_FULL
      case _ => false
    }
  }

  private[spark] def createReadBuilder(
      table: Table,
      partitionFilters: Array[PartitionPredicate]): Option[PostponeMergeReadBuilder] = {
    table match {
      case fileStoreTable: FileStoreTable if configured(fileStoreTable) =>
        val partitionFilter =
          if (partitionFilters.isEmpty) null
          else PartitionPredicate.and(partitionFilters.toList.asJava)
        val result = PostponeMergeReadBuilder.createSnapshotBound(fileStoreTable, partitionFilter)
        if (result.isPresent) Some(result.get) else None
      case _ => None
    }
  }

  private def configured(table: FileStoreTable): Boolean = {
    table.coreOptions().postponeMergeOnRead() &&
    table.bucketMode() == BucketMode.POSTPONE_MODE &&
    !table.primaryKeys().isEmpty
  }

  private[spark] case class MergePlan(
      readBuilder: PostponeMergeReadBuilder,
      corePlan: PostponeMergePlan,
      blobAsDescriptor: Boolean,
      realScanInfo: RealScanInfo)

  private[spark] case class RealScanInfo(
      tableName: String,
      description: String,
      driverMetrics: Map[String, Long])
}
