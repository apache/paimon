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

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.globalindex.{DataEvolutionBatchScan, GlobalIndexResult}
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{Predicate, PredicateBuilder, VectorSearch}
import org.apache.paimon.spark.PostponeMergeOnRead.MergePlan
import org.apache.paimon.spark.metric.SparkMetricRegistry
import org.apache.paimon.spark.read.{BaseScan, BatchReadTagCleanupListener, PaimonStatistics, PaimonSupportsRuntimeFiltering, SparkHybridSearchBuilderImpl, SparkVectorSearchBuilderImpl}
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.{DataTable, FileStoreTable, InnerTable}
import org.apache.paimon.table.source.{FullTextSearchBuilder, InnerTableScan, Split, VectorSearchBuilder}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Statistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.SQLExecution

import scala.collection.JavaConverters._

abstract class PaimonBaseScan(table: InnerTable)
  extends BaseScan
  with PaimonSupportsRuntimeFiltering
  with SQLConfHelper {

  import PaimonBaseScan._

  private[spark] lazy val paimonMetricsRegistry: SparkMetricRegistry = SparkMetricRegistry()

  @transient private lazy val postponeMergeOnRead = new PostponeMergeOnRead(this)

  protected def getInputSplits: Array[Split] = {
    val baseScan = readBuilder.newScan().asInstanceOf[InnerTableScan]
    val scan = evalGlobalIndexSearch()
      .map(search => withSearchResult(baseScan, search.result, search.snapshot))
      .getOrElse(baseScan)
      .withMetricRegistry(paimonMetricsRegistry)

    val plan = scan.plan()
    registerReadProtectionTagCleanup(scan.readProtectionTagName)
    plan.splits().asScala.toArray
  }

  final private[spark] def registerReadProtectionTagCleanup(tagName: String): Unit = {
    Option(tagName).foreach {
      name =>
        val spark = SparkSession.active
        val executionId =
          Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).map(_.toLong)
        BatchReadTagCleanupListener.getOrCreate(spark).registerCleanup(executionId, name, table)
    }
  }

  private def evalGlobalIndexSearch(): Option[SearchResult] = {
    val globalSearchCount =
      Seq(pushedVectorSearch, pushedHybridSearch, pushedFullTextSearch).count(_.isDefined)
    if (globalSearchCount > 1) {
      throw new UnsupportedOperationException(
        "Cannot push down vector search, hybrid search and full-text search simultaneously.")
    }
    pushedVectorSearch
      .map(
        search =>
          evalVectorSearchWithSnapshot(table, search, pushedPartitionFilters, pushedDataFilters))
      .orElse(pushedHybridSearch.map(_ => evalHybridSearch()))
      .orElse(pushedFullTextSearch.map(_ => evalFullTextSearch()))
  }

  override def filterAttributes(): Array[NamedReference] = {
    if (postponeMergeOnRead.enabled) Array.empty else super.filterAttributes()
  }

  private def evalHybridSearch(): SearchResult = {
    val hybridSearch = pushedHybridSearch.get
    val hybridSearchBuilder =
      if (CoreOptions.fromMap(table.options).vectorSearchDistributeEnabled()) {
        new SparkHybridSearchBuilderImpl(table)
      } else {
        table.newHybridSearchBuilder()
      }
    val builder = hybridSearchBuilder
      .withLimit(hybridSearch.limit())
      .withRanker(hybridSearch.ranker())
    hybridSearch.routes().asScala.foreach(route => builder.addRoute(route))
    if (pushedPartitionFilters.nonEmpty) {
      builder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      builder.withFilter(PredicateBuilder.and(pushedDataFilters.asJava))
    }
    val routeResults = builder.routeBuilders().asScala.map {
      route =>
        val search =
          if (route.route().isVector()) {
            readVectorSearch(route.vectorSearchBuilder())
          } else {
            readFullTextSearch(route.fullTextSearchBuilder())
          }
        (builder.toRouteResult(route, search.result), search.snapshot)
    }
    val snapshots = routeResults.flatMap { case (_, snapshot) => Option(snapshot) }
    require(snapshots.map(_.id()).distinct.size <= 1, "Hybrid search routes must use one snapshot.")
    SearchResult(builder.rank(routeResults.map(_._1).asJava), snapshots.headOption.orNull)
  }

  private def evalFullTextSearch(): SearchResult = {
    val fullTextSearch = pushedFullTextSearch.get
    val ftBuilder = table
      .newFullTextSearchBuilder()
      .withQuery(fullTextSearch.column(), fullTextSearch.query())
      .withLimit(fullTextSearch.limit())
    if (pushedPartitionFilters.nonEmpty) {
      ftBuilder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      ftBuilder.withFilter(PredicateBuilder.and(pushedDataFilters.asJava))
    }
    readFullTextSearch(ftBuilder)
  }

  override def toBatch: Batch = {
    if (postponeMergeOnRead.enabled) {
      throw new UnsupportedOperationException(
        "Postpone merge-on-read must be executed by PostponeMergeOnReadExec.")
    } else {
      ensureNoFullScan()
      super.toBatch
    }
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    if (PostponeMergeOnRead.enabled(table)) {
      throw new UnsupportedOperationException(
        "Option 'postpone.merge-on-read' is only supported for batch reads.")
    }
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def estimateStatistics: Statistics = {
    if (postponeMergeOnRead.enabled) {
      val splits =
        planPostponeMerge()
          .map(_.corePlan.splits().asScala.toArray)
          .getOrElse(Array.empty[Split])
      PaimonStatistics(splits, readTableRowType, table.rowType(), table.statistics())
    } else {
      super.estimateStatistics
    }
  }

  final private[spark] def planPostponeMerge(): Option[MergePlan] = {
    postponeMergeOnRead.plan()
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    super.supportedCustomMetrics ++
      Array(
        PaimonPlanningDurationMetric(),
        PaimonScannedSnapshotIdMetric(),
        PaimonScannedManifestsMetric(),
        PaimonSkippedTableFilesMetric()
      )
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    paimonMetricsRegistry.buildSparkScanMetrics()
  }

  final protected def ensureNoFullScan(): Unit = ensureNoFullScan(0L)

  final private[spark] def ensureNoFullScan(externallyReadFiles: Long): Unit = {
    if (OptionUtils.readAllowFullScan()) {
      return
    }

    table match {
      case t: FileStoreTable if !t.partitionKeys().isEmpty =>
        val skippedFiles = paimonMetricsRegistry.buildSparkScanMetrics().collectFirst {
          case m: PaimonSkippedTableFilesTaskMetric => m.value
        }
        if (skippedFiles.exists(_ <= externallyReadFiles)) {
          throw new RuntimeException("Full scan is not supported.")
        }
      case _ =>
    }
  }
}

object PaimonBaseScan {

  private case class SearchResult(result: GlobalIndexResult, snapshot: Snapshot)

  private[spark] def withSearchResult(
      scan: InnerTableScan,
      result: GlobalIndexResult,
      snapshot: Snapshot): InnerTableScan = scan match {
    case dataEvolution: DataEvolutionBatchScan =>
      dataEvolution.withGlobalIndexResult(result, snapshot)
    case _ => scan.withGlobalIndexResult(result)
  }

  private[spark] def evalVectorSearch(
      table: InnerTable,
      vectorSearch: VectorSearch,
      pushedPartitionFilters: Seq[PartitionPredicate],
      pushedDataFilters: Seq[Predicate]): GlobalIndexResult = {
    evalVectorSearchWithSnapshot(
      table,
      vectorSearch,
      pushedPartitionFilters,
      pushedDataFilters).result
  }

  private def evalVectorSearchWithSnapshot(
      table: InnerTable,
      vectorSearch: VectorSearch,
      pushedPartitionFilters: Seq[PartitionPredicate],
      pushedDataFilters: Seq[Predicate]): SearchResult = {
    val vectorSearchBuilder =
      if (CoreOptions.fromMap(table.options).vectorSearchDistributeEnabled()) {
        new SparkVectorSearchBuilderImpl(table)
      } else {
        table.newVectorSearchBuilder()
      }
    val vectorBuilder = vectorSearchBuilder
      .withVectorColumn(vectorSearch.fieldName())
      .withLimit(vectorSearch.limit())
      .withOptions(vectorSearch.options())
    val matrix = vectorSearch.multiVector()
    if (matrix == null) {
      vectorBuilder.withVector(vectorSearch.vector())
    } else {
      vectorBuilder.withMultiVector(matrix)
    }
    if (pushedPartitionFilters.nonEmpty) {
      vectorBuilder.withPartitionFilter(PartitionPredicate.and(pushedPartitionFilters.asJava))
    }
    if (pushedDataFilters.nonEmpty) {
      vectorBuilder.withFilter(PredicateBuilder.and(pushedDataFilters.asJava))
    }
    readVectorSearch(vectorBuilder)
  }

  private def readVectorSearch(builder: VectorSearchBuilder): SearchResult = {
    val reader = builder.newVectorRead()
    val plan = builder.newVectorScan().scan()
    SearchResult(reader.read(plan), plan.snapshot())
  }

  private def readFullTextSearch(builder: FullTextSearchBuilder): SearchResult = {
    val reader = builder.newFullTextRead()
    val plan = builder.newFullTextScan().scan()
    SearchResult(reader.read(plan), plan.snapshot())
  }
}
