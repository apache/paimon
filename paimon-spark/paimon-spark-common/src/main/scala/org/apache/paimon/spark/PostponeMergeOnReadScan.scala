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

import org.apache.paimon.predicate.{FullTextSearch, HybridSearch, PredicateBuilder, TopN, VectorSearch}
import org.apache.paimon.spark.PostponeMergeOnReadScan.MergePlan
import org.apache.paimon.spark.read.{PaimonStatistics, VariantExtractionInfo}
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.source.{PostponeMergePlan, PostponeMergeReadBuilder, Split}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.{Batch, Statistics}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Explicit DSv2 scan selected only when the early manifest probe finds postpone files. */
private[spark] case class PostponeMergeOnReadScan(
    delegate: PaimonScan,
    mergeReadBuilder: PostponeMergeReadBuilder)
  extends PaimonBaseScan(delegate.table) {

  override def table = delegate.table

  override def requiredSchema: StructType = delegate.requiredSchema
  override def pushedPartitionFilters = delegate.pushedPartitionFilters
  override def pushedDataFilters = delegate.pushedDataFilters
  override def pushedLimit: Option[Int] = delegate.pushedLimit
  override def pushedTopN: Option[TopN] = delegate.pushedTopN
  override def pushedVectorSearch: Option[VectorSearch] = delegate.pushedVectorSearch
  override def pushedHybridSearch: Option[HybridSearch] = delegate.pushedHybridSearch
  override def pushedFullTextSearch: Option[FullTextSearch] = delegate.pushedFullTextSearch
  override def pushedVariantExtractions: Map[Seq[String], Seq[VariantExtractionInfo]] =
    delegate.pushedVariantExtractions
  override def pushedMapSelectedKeys: Map[String, Seq[String]] = delegate.pushedMapSelectedKeys

  @transient private var plannedMerge: MergePlan = _

  // Spark's runtime-filter callback is tied to BatchScanExec. This scan is executed by the
  // dedicated postpone merge node, so advertising partition attributes would freeze a plan before
  // the callback can update it.
  override def filterAttributes(): Array[NamedReference] = Array.empty

  override def toBatch: Batch = {
    throw new UnsupportedOperationException(
      "PostponeMergeOnReadScan must be executed by PostponeMergeOnReadExec.")
  }

  override def estimateStatistics: Statistics = {
    val corePlan =
      planPostponeMerge(SparkSession.active.sparkContext.defaultParallelism).corePlan
    val rawSplits: Array[Split] = (
      corePlan.realSplits().asScala.map(split => split: Split) ++
        corePlan.postponeFileTasks().asScala.map(task => task.split(): Split)
    ).toArray
    PaimonStatistics(rawSplits, readTableRowType, table.rowType(), table.statistics())
  }

  private[spark] def planPostponeMerge(defaultBucketNum: Int): MergePlan = synchronized {
    if (plannedMerge == null) {
      if (metadataFields.nonEmpty) {
        throw new UnsupportedOperationException(
          "Option 'postpone.merge-on-read' does not support metadata columns: " +
            metadataFields.map(_.name).mkString(", "))
      }
      if (
        pushedVectorSearch.isDefined || pushedHybridSearch.isDefined ||
        pushedFullTextSearch.isDefined
      ) {
        throw new UnsupportedOperationException(
          "Option 'postpone.merge-on-read' does not support vector, hybrid or full-text search.")
      }
      ensureUnfilteredPostponeFullScanAllowed()

      mergeReadBuilder
        .withReadType(readTableRowType)
        .withDefaultBucketNum(defaultBucketNum)
        .withMetricRegistry(paimonMetricsRegistry)

      if (pushedDataFilters.nonEmpty) {
        mergeReadBuilder.withFilter(PredicateBuilder.and(pushedDataFilters.toList.asJava))
      }

      val corePlan = mergeReadBuilder.plan()
      registerReadProtectionTagCleanup(mergeReadBuilder.readProtectionTagName())
      // Filters may still fail to prune any real files. That decision needs planning metrics and
      // therefore cannot share the early, structurally unfiltered check above.
      ensureNoFullScan()
      plannedMerge = MergePlan(mergeReadBuilder, corePlan, delegate.coreOptions.blobAsDescriptor())
    }

    plannedMerge
  }

  private def ensureUnfilteredPostponeFullScanAllowed(): Unit = {
    if (
      !OptionUtils.readAllowFullScan() && !table.partitionKeys().isEmpty &&
      pushedPartitionFilters.isEmpty &&
      pushedDataFilters.isEmpty
    ) {
      // The real-bucket scan metric counts selected postpone files as skipped. An unfiltered
      // combined read is nevertheless a full-table scan, so handle that case explicitly.
      throw new RuntimeException("Full scan is not supported.")
    }
  }
}

private[spark] object PostponeMergeOnReadScan {

  /** Spark execution metadata around the engine-neutral Core plan and read builder. */
  private[spark] case class MergePlan(
      readBuilder: PostponeMergeReadBuilder,
      corePlan: PostponeMergePlan,
      blobAsDescriptor: Boolean)
}
