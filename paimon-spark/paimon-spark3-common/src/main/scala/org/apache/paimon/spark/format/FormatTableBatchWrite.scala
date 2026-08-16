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

package org.apache.paimon.spark.format

import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * Spark-3.x thin wrapper that mixes `BatchWrite` into [[FormatTableBatchWriteBase]]. See the base
 * class scaladoc for why the inheritance lives here rather than in `paimon-spark-common`.
 */
class FormatTableBatchWrite(
    table: FormatTable,
    overwriteDynamic: Option[Boolean],
    overwritePartitions: Option[Map[String, String]],
    writeSchema: StructType)
  extends FormatTableBatchWriteBase(table, overwriteDynamic, overwritePartitions, writeSchema)
  with BatchWrite
  with Serializable {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    createFormatTableDataWriterFactory()

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = commitMessages(messages)

  override def abort(messages: Array[WriterCommitMessage]): Unit = abortMessages(messages)
}
