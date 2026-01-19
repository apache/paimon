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

import org.apache.paimon.fs.StorageType
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.action.ArchivePartitionAction
import org.apache.paimon.spark.catalyst.plans.logical.{AlterTableArchiveCommand, ArchiveCold, ArchiveOperation, ArchiveStandard, RestoreArchive, Unarchive}
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

case class AlterTableArchiveExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitionSpec: Map[String, String],
    operation: ArchiveOperation)
  extends PaimonLeafV2CommandExec
  with Logging {

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case SparkTable(paimonTable: FileStoreTable) =>
        val sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext)
        val action = new ArchivePartitionAction(paimonTable, sparkContext)
        val partitionSpecs = List(partitionSpec)

        operation match {
          case ArchiveStandard =>
            val count = action.archive(partitionSpecs, StorageType.Archive)
            logInfo(s"Archived $count files for partition $partitionSpec")
          case ArchiveCold =>
            val count = action.archive(partitionSpecs, StorageType.ColdArchive)
            logInfo(s"Cold archived $count files for partition $partitionSpec")
          case RestoreArchive(durationOpt) =>
            val duration = durationOpt.getOrElse(java.time.Duration.ofDays(7))
            val count = action.restoreArchive(partitionSpecs, duration)
            logInfo(s"Restored $count files for partition $partitionSpec")
          case Unarchive =>
            // Determine current storage type - for now, assume Archive
            // In a production implementation, this could be tracked in metadata
            val count = action.unarchive(partitionSpecs, StorageType.Archive)
            logInfo(s"Unarchived $count files for partition $partitionSpec")
        }
      case t =>
        throw new UnsupportedOperationException(s"Unsupported table : $t")
    }
    Nil
  }

  override def output: Seq[Attribute] = Nil
}

