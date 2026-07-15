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

package org.apache.paimon.spark.write

import org.apache.paimon.Snapshot
import org.apache.paimon.spark.{PaimonAddedTableFilesMetric, PaimonCommitDurationMetric, PaimonDeletedRecordsMetric, PaimonInsertedRecordsMetric, PaimonNumWritersMetric, PaimonUpdatedRecordsMetric}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{DeltaBatchWrite, DeltaWrite}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * A [[DeltaWrite]] for deletion-vector enabled append tables. `toBatch` goes through the shim so
 * the `DeltaBatchWrite` mixin is compiled against the right Spark minor version, see
 * [[PaimonDeltaWriteBase]].
 */
class PaimonDeltaWrite(
    table: FileStoreTable,
    rowSchema: StructType,
    rowIdSchema: StructType,
    operationType: Snapshot.Operation,
    readSnapshotId: Option[Long])
  extends DeltaWrite {

  override def toBatch: DeltaBatchWrite = {
    // The shim factory is declared to return `BatchWrite` so that loading the shim class on
    // Spark < 3.4 runtimes never links against the delta write API; this class itself is only
    // loaded behind the `supportsV2DeltaOps` gate (Spark 3.5+).
    SparkShimLoader.shim
      .createPaimonDeltaBatchWrite(table, rowSchema, rowIdSchema, operationType, readSnapshotId)
      .asInstanceOf[DeltaBatchWrite]
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    val buffer = mutable.ArrayBuffer[CustomMetric](PaimonCommitDurationMetric())
    operationType match {
      case Snapshot.Operation.DELETE =>
        buffer += PaimonDeletedRecordsMetric()
      case Snapshot.Operation.UPDATE =>
        buffer += PaimonAddedTableFilesMetric()
        buffer += PaimonNumWritersMetric()
        buffer += PaimonUpdatedRecordsMetric()
      case Snapshot.Operation.MERGE =>
        buffer += PaimonAddedTableFilesMetric()
        buffer += PaimonNumWritersMetric()
        buffer += PaimonDeletedRecordsMetric()
        buffer += PaimonUpdatedRecordsMetric()
        buffer += PaimonInsertedRecordsMetric()
      case _ =>
    }
    buffer.toArray
  }

  override def toString: String = s"PaimonDeltaWrite(table=${table.fullName()})"

  override def description(): String = toString
}
