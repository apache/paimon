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
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan, SparkTypeUtils, SparkV2FilterConverter}
import org.apache.paimon.table.{FileStoreTable, Table}
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

  // Ordinary scans do not pay for coverage and freshness discovery. Spark requests these
  // attributes only when considering runtime filtering for a join.
  private lazy val runtimeIndexFields: Set[String] = runtimeReadSnapshot.toSeq.flatMap {
    case (fileTable, snapshot) =>
      val indexEntries = Seq("btree", "bitmap")
        .flatMap {
          indexType => fileTable.store().newIndexFileHandler().scan(snapshot, indexType).asScala
        }
        .filter(
          e =>
            e.indexFile().globalIndexMeta() != null &&
              e.indexFile().globalIndexMeta().getIndexedFieldIds.size() == 1)
      val indexFiles = indexEntries.map(_.indexFile())
      val indexedIds = indexFiles.map(_.globalIndexMeta().indexFieldId()).toSet
      val coverage = new DataEvolutionGlobalIndexCoverage(
        fileTable,
        snapshot,
        null,
        indexFiles.asJava,
        GlobalIndexSearchMode.FULL)
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
              indexEntries.filter(_.indexFile().globalIndexMeta().indexFieldId() == field.id())
            supportedType && indexedIds.contains(field.id()) &&
            !fileTable.partitionKeys().contains(field.name()) &&
            coverage.unindexedRanges(field.id()).isEmpty &&
            fieldEntries.forall(
              e =>
                DataEvolutionIndexSourceMeta.isDataEvolutionMeta(
                  e.indexFile().globalIndexMeta().sourceMeta())) &&
            // A past writer may have used IGNORE only as a dynamic option.
            DataEvolutionGlobalIndexRefreshPlanner
              .findIndexesToRefresh(
                fileTable,
                snapshot,
                null,
                fieldEntries.asJava,
                java.util.Collections.singletonList(field))
              .isEmpty
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
              if (predicate.children().length == 1) {
                val name = predicate.children().head.asInstanceOf[NamedReference].fieldNames().head
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
