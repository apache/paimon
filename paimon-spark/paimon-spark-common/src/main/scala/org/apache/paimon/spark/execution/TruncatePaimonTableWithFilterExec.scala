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

package org.apache.paimon.spark.execution

import org.apache.paimon.CoreOptions
import org.apache.paimon.options.Options
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.{FileStoreTable, Table}
import org.apache.paimon.table.sink.BatchTableCommit

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

import java.util.{Collections => JCollections}

case class TruncatePaimonTableWithFilterExec(
    table: Table,
    partitionPredicate: Option[PartitionPredicate])
  extends PaimonLeafV2CommandExec {

  override def run(): Seq[InternalRow] = {
    partitionPredicate match {
      case Some(p) =>
        table match {
          case fileStoreTable: FileStoreTable =>
            if (fileStoreTable.newSnapshotReader().withPartitionFilter(p).partitions().isEmpty) {
              withCommit(table)(_.commit(JCollections.emptyList()))
            } else {
              // The commit matches the predicate against the partition values themselves. A spec
              // rebuilt from a value does not always parse back to it, such as a blank string,
              // whose name is the default partition name.
              val commit = fileStoreTable.newCommit(
                CoreOptions.createCommitUser(new Options(fileStoreTable.options())))
              try {
                commit.truncatePartitions(p)
              } finally {
                commit.close()
              }
            }
          case _ =>
            throw new UnsupportedOperationException("Unsupported truncate table")
        }
      case _ =>
        withCommit(table)(_.truncateTable())
    }
    Nil
  }

  private def withCommit(table: Table)(operation: BatchTableCommit => Unit): Unit = {
    val commit = table.newBatchWriteBuilder().newCommit()
    try {
      operation(commit)
    } finally {
      commit.close()
    }
  }

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"TruncatePaimonTableWithFilterExec: ${table.fullName()}" +
      partitionPredicate.map(p => s", PartitionPredicate: [$p]").getOrElse("")
  }
}
