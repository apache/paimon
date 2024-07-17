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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.BatchWriteBuilder

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec

import java.util.{Collections, UUID}

import scala.collection.JavaConverters._

case class PaimonTruncateTableCommand(v2Table: SparkTable, partitionSpec: TablePartitionSpec)
  extends PaimonLeafRunnableCommand
  with WithFileStoreTable {

  override def table: FileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val commit = table.store.newCommit(UUID.randomUUID.toString)

    if (partitionSpec.isEmpty) {
      commit.truncateTable(BatchWriteBuilder.COMMIT_IDENTIFIER)
    } else {
      commit.dropPartitions(
        Collections.singletonList(partitionSpec.asJava),
        BatchWriteBuilder.COMMIT_IDENTIFIER
      )
    }

    Seq.empty[Row]
  }
}
