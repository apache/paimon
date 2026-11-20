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

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.CoreOptions.{GlobalIndexSearchMode, StartupMode}
import org.apache.paimon.globalindex.{DataEvolutionGlobalIndexCoverage, DataEvolutionGlobalIndexRefreshPlanner}
import org.apache.paimon.index.DataEvolutionIndexSourceMeta
import org.apache.paimon.manifest.IndexManifestEntry
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan, SparkTypeUtils, SparkV2FilterConverter}
import org.apache.paimon.table.{FallbackReadFileStoreTable, FileStoreTable, Table}
import org.apache.paimon.table.source.Split
import org.apache.paimon.types.DataTypeRoot

import org.apache.spark.sql.PaimonUtils.fieldReference
import org.apache.spark.sql.connector.expressions.{Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

/**
 * Supports runtime filtering by caching splits and invalidating them when new filters are applied.
 * This allows dynamic re-planning of input splits based on runtime predicates.
 */
trait PaimonSupportsRuntimeFiltering extends BaseScan with SupportsRuntimeV2Filtering {

  private var _inputSplits: Array[Split] = _
  private var _inputPartitions: Seq[PaimonInputPartition] = _

  // Pin metadata discovery and every subsequent scan to the same snapshot. In particular,
  // replanning after the build side completes must not pick up a concurrent append.
  private lazy val runtimeReadSnapshot: Option[(FileStoreTable, Snapshot)] = {
    val eligibleScan = this match {
      case scan: PaimonScan => !scan.preserveDataGrouping
      case _ => false
    }
    if (
      !eligibleScan || pushedLimit.isDefined || pushedTopN.isDefined ||
      pushedVectorSearch.isDefined || pushedHybridSearch.isDefined ||
      pushedFullTextSearch.isDefined || !coreOptions.dataEvolutionEnabled() ||
      !coreOptions.globalIndexEnabled() || coreOptions.queryAuthEnabled() ||
      coreOptions.ignoreIndexColumnUpdate() ||
      coreOptions.startupMode() != StartupMode.LATEST_FULL
    ) {
      None
    } else {
      table match {
        // A fallback wrapper selects its branches independently, not at main's commit time.
        case _: FallbackReadFileStoreTable => None
        case fileTable: FileStoreTable if fileTable.primaryKeys().isEmpty =>
          Option(fileTable.snapshotManager().latestSnapshot()).map {
            snapshot =>
              val pinned = fileTable.copyWithoutTimeTravel(
                Map(
                  CoreOptions.SCAN_MODE.key() -> "from-snapshot",
                  CoreOptions.SCAN_SNAPSHOT_ID.key() -> snapshot.id().toString).asJava)
              pinned -> snapshot
          }
        case _ => None
      }
    }
  }

  private lazy val runtimeIndexEntries: Seq[IndexManifestEntry] =
    runtimeReadSnapshot.toSeq.flatMap {
      case (fileTable, snapshot) =>
        fileTable
          .store()
          .newIndexFileHandler()
          .scan(
            snapshot,
            (entry: IndexManifestEntry) =>
              Set("btree", "bitmap").contains(entry.indexFile().indexType()) &&
                entry.indexFile().globalIndexMeta() != null)
          .asScala
    }

  // Spark can ask for capabilities even when it never applies a runtime predicate.
  // Only inspect index metadata here; scan data manifests for freshness in filter().
  private lazy val runtimeIndexFields: Set[String] = runtimeReadSnapshot.toSeq.flatMap {
    case (fileTable, snapshot) =>
      val compositeFields = runtimeIndexEntries
        .filter(_.indexFile().globalIndexMeta().getIndexedFieldIds.size() > 1)
        .flatMap(_.indexFile().globalIndexMeta().getIndexedFieldIds.asScala)
        .toSet
      readTableRowType.getFields.asScala
        .filter {
          field =>
            val supportedType = field.`type`().getTypeRoot match {
              case DataTypeRoot.BOOLEAN | DataTypeRoot.TINYINT | DataTypeRoot.SMALLINT |
                  DataTypeRoot.INTEGER | DataTypeRoot.BIGINT | DataTypeRoot.CHAR |
                  DataTypeRoot.VARCHAR | DataTypeRoot.DATE =>
                true
              case _ => false
            }
            val fieldEntries =
              runtimeIndexEntries.filter(
                _.indexFile().globalIndexMeta().indexFieldId() == field.id())
            supportedType && fieldEntries.nonEmpty && !compositeFields.contains(field.id()) &&
            !fileTable.partitionKeys().contains(field.name()) &&
            // The planner can choose a different definition after adding runtime predicates.
            // Prove each eligible definition complete, never the union of their coverage.
            fieldEntries.groupBy(_.indexFile().indexType()).values.forall {
              entries =>
                new DataEvolutionGlobalIndexCoverage(
                  fileTable,
                  snapshot,
                  null,
                  entries.map(_.indexFile()).asJava,
                  GlobalIndexSearchMode.FULL).unindexedRanges(field.id()).isEmpty
            } &&
            fieldEntries.forall(
              e =>
                DataEvolutionIndexSourceMeta.isDataEvolutionMeta(
                  e.indexFile().globalIndexMeta().sourceMeta()))
        }
        .map(_.name())
        .toSet
  }.toSet

  override protected def readTable: Table = runtimeReadSnapshot.map(_._1).getOrElse(table)

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
    // Fields read only to construct metadata are not ordinary attributes in the scan output.
    val requiredFields = readTableRowType.getFieldNames.asScala
    (table.partitionKeys().asScala ++ runtimeIndexFields.toSeq.sorted)
      .filter(requiredFields.contains)
      .map(fieldReference)
      .toArray
  }

  override def filter(predicates: Array[SparkPredicate]): Unit = {
    val partitionType = table.rowType().project(table.partitionKeys())
    val converter = SparkV2FilterConverter(partitionType)
    val runtimePartitionFilters = predicates.toSeq
      .flatMap(converter.convert(_))
      .map(PartitionPredicate.fromPredicate(partitionType, _))
    val scalarFilters =
      if (runtimeIndexFields.isEmpty) Seq.empty
      else {
        val dataConverter = SparkV2FilterConverter(table.rowType())
        predicates
          .filter {
            predicate =>
              val children = predicate.children()
              // Bound index work after Spark has collected the keys; the join remains intact.
              predicate.name() == "IN" && children.nonEmpty && children.length <= 10001 &&
              (children.head match {
                case ref: NamedReference =>
                  ref.fieldNames().length == 1 && runtimeIndexFields.contains(
                    ref.fieldNames().head) &&
                  children.tail.forall {
                    case literal: Literal[_] =>
                      literal.dataType() == SparkTypeUtils.fromPaimonType(
                        table.rowType().getField(ref.fieldNames().head).`type`())
                    case _ => false
                  }
                case _ => false
              }) && children.tail.forall(_.isInstanceOf[Literal[_]]) &&
              children.tail.iterator.map {
                case literal: Literal[_] =>
                  Option(literal.value())
                    .map(_.toString.getBytes(StandardCharsets.UTF_8).length.toLong)
                    .getOrElse(0L)
              }.sum <= 1024 * 1024
          }
          .flatMap {
            predicate =>
              val name = predicate.children().head.asInstanceOf[NamedReference].fieldNames().head
              val field = table.rowType().getField(name)
              val (fileTable, snapshot) = runtimeReadSnapshot.get
              val entries = runtimeIndexEntries.filter(
                _.indexFile().globalIndexMeta().indexFieldId() == field.id())
              // A past writer may have used IGNORE only as a dynamic option. Do this
              // manifest scan only for accepted predicates and the static partition scope.
              val stale = DataEvolutionGlobalIndexRefreshPlanner.findIndexesToRefresh(
                fileTable,
                snapshot,
                PartitionPredicate.and(pushedPartitionFilters.asJava),
                entries.asJava,
                java.util.Collections.singletonList(field))
              if (!stale.isEmpty) {
                None
              } else if (predicate.children().length == 1) {
                Some(
                  new PredicateBuilder(table.rowType()).in(
                    table.rowType().getFieldIndex(name),
                    java.util.Collections.emptyList[Object]()))
              } else {
                dataConverter.convert(predicate)
              }
          }
          .toSeq
      }
    if (runtimePartitionFilters.nonEmpty) {
      pushedRuntimePartitionFilters.appendAll(runtimePartitionFilters)
      readBuilder.withPartitionFilter(
        PartitionPredicate.and(
          (pushedPartitionFilters ++ pushedRuntimePartitionFilters).toList.asJava))
    }
    if (scalarFilters.nonEmpty) {
      // Keep the same builder: Spark may retain the original batch's reader factory.
      readBuilder.withFilter(scalarFilters.asJava)
    }
    if (runtimePartitionFilters.nonEmpty || scalarFilters.nonEmpty) {
      _inputPartitions = null
      _inputSplits = null
    }
  }
}
