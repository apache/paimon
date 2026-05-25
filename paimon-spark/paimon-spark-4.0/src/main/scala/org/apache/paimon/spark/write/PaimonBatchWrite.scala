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

import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * Spark-4.0 shadow wrapper. Source-identical to the `paimon-spark4-common` version but compiled
 * against Spark 4.0.2; the maven shade order picks `paimon-spark-4.0/target/classes` ahead of the
 * shaded 4-common copy, so the class metadata loaded at runtime does not include the 4.1-only
 * `BatchWrite.commit(.., WriteSummary)` signature that triggers `ClassNotFoundException` via
 * `ObjectStreamClass.getPrivateMethod` during Spark task serialization.
 */
class PaimonBatchWrite(
    table: FileStoreTable,
    writeSchema: StructType,
    dataSchema: StructType,
    overwritePartitions: Option[Map[String, String]],
    copyOnWriteScan: Option[PaimonCopyOnWriteScan])
  extends PaimonBatchWriteBase(table, writeSchema, dataSchema, overwritePartitions, copyOnWriteScan)
  with BatchWrite
  with Serializable {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    createPaimonDataWriterFactory(info)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = commitMessages(messages)

  override def abort(messages: Array[WriterCommitMessage]): Unit = abortMessages(messages)
}

object PaimonBatchWrite {
  def apply(
      table: FileStoreTable,
      writeSchema: StructType,
      dataSchema: StructType,
      overwritePartitions: Option[Map[String, String]],
      copyOnWriteScan: Option[PaimonCopyOnWriteScan]): PaimonBatchWrite =
    new PaimonBatchWrite(table, writeSchema, dataSchema, overwritePartitions, copyOnWriteScan)
}
