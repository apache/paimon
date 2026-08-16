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
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.write.{DeltaBatchWrite, DeltaWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * Spark-3.x thin wrapper that mixes `DeltaBatchWrite` into [[PaimonDeltaWriteBase]]. See the base
 * class scaladoc for why the inheritance lives here rather than in `paimon-spark-common`.
 */
class PaimonDeltaBatchWrite(
    table: FileStoreTable,
    rowSchema: StructType,
    rowIdSchema: StructType,
    operationType: Snapshot.Operation,
    readSnapshotId: Option[Long])
  extends PaimonDeltaWriteBase(table, rowSchema, rowIdSchema, operationType, readSnapshotId)
  with DeltaBatchWrite
  with Serializable {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory =
    createPaimonDeltaWriterFactory(info)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = commitMessages(messages)

  override def abort(messages: Array[WriterCommitMessage]): Unit = abortMessages(messages)
}
