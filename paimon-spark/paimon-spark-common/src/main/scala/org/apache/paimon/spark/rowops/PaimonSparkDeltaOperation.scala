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

package org.apache.paimon.spark.rowops

import org.apache.paimon.CoreOptions
import org.apache.paimon.Snapshot
import org.apache.paimon.spark.PaimonScanBuilder
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{FILE_PATH_COLUMN, ROW_INDEX_COLUMN}
import org.apache.paimon.spark.write.PaimonDeltaWriteBuilder
import org.apache.paimon.table.{FileStoreTable, InnerTable}

import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{DeltaWriteBuilder, LogicalWriteInfo, RowLevelOperation, RowLevelOperationInfo, SupportsDelta}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{HashMap => JHashMap}

/**
 * A delta-based [[RowLevelOperation]] for deletion-vector enabled append tables: instead of
 * rewriting whole files like [[PaimonSparkCopyOnWriteOperation]], deleted rows are marked in
 * deletion vectors keyed by the `(__paimon_file_path, __paimon_row_index)` row ID.
 *
 * The scan side is a regular pushdown scan: Spark's runtime group filtering only applies to
 * group-based (`ReplaceData`) plans, delta plans converge on matching rows through the rewrite
 * condition and ordinary predicate pushdown.
 */
class PaimonSparkDeltaOperation(table: FileStoreTable, info: RowLevelOperationInfo)
  extends RowLevelOperation
  with SupportsDelta {

  // Captured before the scan is planned. The deletion-vector maintainer must merge with the
  // deletion vectors of the snapshot the job read, never a newer one: initializing it from the
  // commit-time latest snapshot would silently absorb concurrent deletion-vector changes that
  // commit conflict detection is supposed to reject (and an UPDATE/MERGE could resurrect a
  // concurrently deleted row as its new version). Same contract as the V1 commands' readSnapshot.
  private val readSnapshotId: Option[Long] =
    Option(table.snapshotManager().latestSnapshot()).map(_.id())

  override def command(): RowLevelOperation.Command = info.command()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Pin the scan to the captured snapshot so the read, the deletion-vector maintainer and the
    // commit-time file mapping all observe the same version; a commit that lands in between (e.g.
    // a concurrent compaction) then surfaces as a commit conflict instead of a spurious mismatch
    // between the scanned files and the read snapshot.
    val conf = new JHashMap[String, String](options.asCaseSensitiveMap())
    readSnapshotId.foreach(id => conf.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), id.toString))
    new PaimonScanBuilder(table.copy(conf).asInstanceOf[InnerTable])
  }

  override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder =
    new PaimonDeltaWriteBuilder(
      table,
      info,
      Snapshot.Operation.valueOf(command().toString),
      readSnapshotId)

  override def rowId(): Array[NamedReference] =
    Array(Expressions.column(FILE_PATH_COLUMN), Expressions.column(ROW_INDEX_COLUMN))

  // Paimon cannot update rows in place: an update is a deletion-vector mark on the old row plus
  // an appended new row. Still answer false so that Spark plans a single UPDATE operation per row
  // (no Expand doubling); the delta writer decomposes it internally.
  override def representUpdateAsDeleteAndInsert(): Boolean = false

  override def requiredMetadataAttributes(): Array[NamedReference] = Array.empty
}
