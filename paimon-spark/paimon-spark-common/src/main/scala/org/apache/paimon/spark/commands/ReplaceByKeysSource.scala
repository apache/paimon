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

package org.apache.paimon.spark.commands

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.fs.Path
import org.apache.paimon.manifest.PartitionEntry
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.schema.TableSchema
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine
import org.apache.paimon.spark.{PaimonScanBuilder, SparkTable}
import org.apache.paimon.table.{ChainGroupReadTable, DelegatedFileStoreTable, FallbackReadFileStoreTable, FileStoreTable}
import org.apache.paimon.table.source.{DataFilePlan, DataTableScan, Split, TableScan}
import org.apache.paimon.table.source.snapshot.TimeTravelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.ComputeCurrentTime
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, CTERelationRef, Deduplicate, GlobalLimit, LocalLimit, LocalRelation, LogicalPlan, OneRowRelation, ParsedStatement, Range, Sample, Tail, Window}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, Map => JMap}

import scala.collection.mutable

/** A repeatable source for both replacement preparation jobs; no source rows or splits are cached. */
private[spark] object ReplaceByKeysSource {
  def bind(
      spark: SparkSession,
      table: FileStoreTable,
      sourceQuery: String,
      targetSnapshot: Snapshot): LogicalPlan = {
    require(sourceQuery != null && sourceQuery.trim.nonEmpty, "source_query must not be empty")
    val parsed = spark.sessionState.sqlParser.parsePlan(sourceQuery)
    parsed.foreach {
      case _: Command | _: ParsedStatement =>
        throw new IllegalArgumentException("source_query must be a query, not a command")
      case _ =>
    }
    // Dataset construction eagerly executes commands. Analyze without creating one first.
    val analyzed = spark.sessionState.executePlan(parsed).analyzed
    analyzed.foreach {
      case _: Command =>
        throw new IllegalArgumentException("source_query must be a query, not a command")
      case _ =>
    }
    require(!analyzed.isStreaming, "Streaming source queries are not supported")
    val selectors = Seq(
      CoreOptions.SCAN_SNAPSHOT_ID.key(),
      CoreOptions.SCAN_TAG_NAME.key(),
      CoreOptions.SCAN_WATERMARK.key(),
      CoreOptions.SCAN_TIMESTAMP.key(),
      CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
      CoreOptions.SCAN_VERSION.key()
    )
    val snapshots = mutable.Map
      .empty[SnapshotKey, Option[Snapshot]]
    if (!table.isInstanceOf[FallbackReadFileStoreTable])
      snapshots.put(
        (
          table.location().toString,
          table.coreOptions().branch(),
          selectors.map(k => Option(table.options().get(k))),
          CoreOptions.StartupMode.LATEST_FULL),
        Option(targetSnapshot)
      )
    // Query-time values must not be evaluated afresh by the two preparation jobs.
    ComputeCurrentTime(analyzed).transformUpWithSubqueries {
      case r: DataSourceV2Relation =>
        require(r.expressions.forall(_.deterministic), "Source query must be deterministic")
        r.table match {
          case SparkTable(fileTable: FileStoreTable) =>
            val effective = fileTable.copyWithoutTimeTravel(r.options.asCaseSensitiveMap())
            val mode = effective.coreOptions().startupMode()
            require(
              Set(
                CoreOptions.StartupMode.LATEST_FULL,
                CoreOptions.StartupMode.LATEST,
                CoreOptions.StartupMode.FROM_TIMESTAMP,
                CoreOptions.StartupMode.FROM_SNAPSHOT,
                CoreOptions.StartupMode.FROM_SNAPSHOT_FULL
              ).contains(mode),
              "Source tables must use a full snapshot scan, not an incremental or file-time scan"
            )
            require(
              effective.coreOptions().toConfiguration.get(CoreOptions.BATCH_SCAN_MODE) ==
                CoreOptions.BatchScanMode.NONE,
              "Source monitoring scans are not supported"
            )
            val normalizedMode = mode match {
              case CoreOptions.StartupMode.LATEST => CoreOptions.StartupMode.LATEST_FULL
              case CoreOptions.StartupMode.FROM_SNAPSHOT_FULL =>
                CoreOptions.StartupMode.FROM_SNAPSHOT
              case other => other
            }
            val historicalKeys =
              if (normalizedMode == CoreOptions.StartupMode.FROM_TIMESTAMP)
                Seq(CoreOptions.SCAN_TIMESTAMP.key(), CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
              else
                selectors.filterNot(
                  k =>
                    k == CoreOptions.SCAN_TIMESTAMP.key() ||
                      k == CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
            require(
              normalizedMode == CoreOptions.StartupMode.LATEST_FULL ||
                historicalKeys.exists(k => effective.options().get(k) != null),
              "Historical source mode requires an explicit matching snapshot selector"
            )
            val selected = bindTable(effective, selectors, snapshots)
            val scanTable = new PinnedSourceTable(selected.table)
            SparkShimLoader.shim.copyDataSourceV2Relation(r, scanTable, r.output)
          case _ =>
            throw new IllegalArgumentException("Source tables must support Paimon snapshots")
        }
      case p =>
        require(p.expressions.forall(_.deterministic), "Source query must be deterministic")
        val unstable = p match {
          case _: Aggregate | _: Deduplicate | _: GlobalLimit | _: LocalLimit | _: Sample |
              _: Tail | _: Window =>
            true
          // OFFSET is unavailable in older Spark compilation targets.
          case _ => p.nodeName == "Offset"
        }
        require(
          !unstable,
          "Source query must be repeatable across projections; materialize aggregate, window, " +
            "limit, offset, deduplication or sampled batches into a Paimon table first"
        )
        require(
          p.children.nonEmpty || p.isInstanceOf[LocalRelation] || p.isInstanceOf[CTERelationRef] ||
            p.isInstanceOf[OneRowRelation] || p.isInstanceOf[Range],
          "Source leaves must be Paimon tables or immutable SQL values"
        )
        p
    }
  }

  private type SnapshotKey = (String, String, Seq[Option[String]], CoreOptions.StartupMode)

  private case class BoundRead(table: FileStoreTable)

  // Share only Snapshot selections: each alias keeps its own analyzed schema and read options.
  private def bindTable(
      table: FileStoreTable,
      selectors: Seq[String],
      snapshots: mutable.Map[SnapshotKey, Option[Snapshot]]): BoundRead = {
    require(
      !table.isInstanceOf[ChainGroupReadTable],
      "Chain-group replacement sources are not supported")
    table match {
      case fallback: FallbackReadFileStoreTable =>
        // Keep Core's partition-level precedence and fallback reader. Pin every component now,
        // including an empty current branch, rather than treating the composite as empty.
        val components = Seq(fallback.wrapped(), fallback.other())
          .filterNot {
            component =>
              // Core represents an absent historical fallback branch with this sentinel and skips it.
              (component eq fallback.other()) &&
              Option(component.coreOptions().scanSnapshotId()).exists(_.longValue() == -1L)
          }
          .map(component => component -> bindTable(component, selectors, snapshots))
        BoundRead(
          new PinnedReadTable(
            fallback,
            () =>
              fallback.newFallbackScan(
                new java.util.function.Function[FileStoreTable, DataTableScan] {
                  override def apply(component: FileStoreTable): DataTableScan =
                    components.find(_._1 eq component).get._2.table.newScan()
                })
          ))
      case _ =>
        require(
          table.coreOptions().toConfiguration.get(CoreOptions.BATCH_SCAN_MODE) ==
            CoreOptions.BatchScanMode.NONE,
          "Source monitoring scans are not supported")
        require(
          Set(
            CoreOptions.StartupMode.LATEST_FULL,
            CoreOptions.StartupMode.LATEST,
            CoreOptions.StartupMode.FROM_TIMESTAMP,
            CoreOptions.StartupMode.FROM_SNAPSHOT,
            CoreOptions.StartupMode.FROM_SNAPSHOT_FULL
          ).contains(table.coreOptions().startupMode()),
          "Source tables must use a full snapshot scan, not an incremental or file-time scan"
        )
        val mode = table.coreOptions().startupMode() match {
          case CoreOptions.StartupMode.LATEST => CoreOptions.StartupMode.LATEST_FULL
          case CoreOptions.StartupMode.FROM_SNAPSHOT_FULL => CoreOptions.StartupMode.FROM_SNAPSHOT
          case other => other
        }
        val key = (
          table.location().toString,
          table.coreOptions().branch(),
          selectors.map(k => Option(table.options().get(k))),
          mode)
        val snapshot = snapshots.getOrElseUpdate(
          key,
          if (mode == CoreOptions.StartupMode.LATEST_FULL)
            Option(table.snapshotManager().latestSnapshot())
          else {
            val resolved = TimeTravelUtil.tryTravelToSnapshot(table)
            require(resolved.isPresent, "Could not bind the requested source snapshot")
            Some(resolved.get())
          }
        )
        ordinary(table, snapshot)
    }
  }

  private def ordinary(table: FileStoreTable, snapshot: Option[Snapshot]): BoundRead = {
    val options = new java.util.HashMap[String, String]()
    Seq(
      CoreOptions.SCAN_SNAPSHOT_ID,
      CoreOptions.SCAN_TAG_NAME,
      CoreOptions.SCAN_WATERMARK,
      CoreOptions.SCAN_TIMESTAMP,
      CoreOptions.SCAN_TIMESTAMP_MILLIS,
      CoreOptions.SCAN_VERSION
    )
      .foreach(k => options.put(k.key(), null))
    options.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_SNAPSHOT.toString)
    // Copy only options: snapshot selection must not replace the analyzed schema.
    val pinned = table.copyWithoutTimeTravel(options)
    snapshot match {
      case Some(selected) =>
        // A private non-evicting Core cache retains the resolved object even after the ordinary
        // snapshot JSON expires. It is never installed on the caller's shared table/cache.
        val cache = Caffeine.newBuilder().build[Path, Snapshot]()
        cache.put(pinned.snapshotManager().snapshotPath(selected.id()), selected)
        pinned.setSnapshotCache(cache)
        val configured = pinned.copyWithoutTimeTravel(
          Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), selected.id().toString))
        BoundRead(configured)
      case None =>
        BoundRead(new PinnedReadTable(pinned, () => new EmptyScan))
    }
  }

  private class EmptyScan extends DataTableScan {
    override def withShard(index: Int, count: Int): DataTableScan = this
    override def withFilter(predicate: Predicate): DataTableScan = this
    override def plan(): TableScan.Plan = new DataFilePlan[Split](Collections.emptyList[Split]())
    override def listPartitionEntries(): java.util.List[PartitionEntry] = Collections.emptyList()
  }

  // Only planning is replaced; Core still owns filters, authorization, split construction and reads.
  private class PinnedReadTable(origin: FileStoreTable, scan: () => DataTableScan)
    extends DelegatedFileStoreTable(origin) {
    override def newScan(): DataTableScan = scan()
    override def copy(options: JMap[String, String]): FileStoreTable =
      throw new UnsupportedOperationException("A bound replacement source cannot be reconfigured")
    override def copy(schema: TableSchema): FileStoreTable =
      throw new UnsupportedOperationException("A bound replacement source cannot be reconfigured")
    override def copyWithoutTimeTravel(options: JMap[String, String]): FileStoreTable = copy(
      options)
    override def copyWithoutQueryAuth(): FileStoreTable =
      throw new UnsupportedOperationException("A bound replacement source cannot be reconfigured")
    override def copyWithLatestSchema(): FileStoreTable =
      throw new UnsupportedOperationException("A bound replacement source cannot be reconfigured")
    override def switchToBranch(branch: String): FileStoreTable =
      throw new UnsupportedOperationException("A bound replacement source cannot be reconfigured")
  }

  private class PinnedSourceTable(pinned: FileStoreTable) extends SparkTable(pinned) {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new PaimonScanBuilder(pinned)
  }

}
