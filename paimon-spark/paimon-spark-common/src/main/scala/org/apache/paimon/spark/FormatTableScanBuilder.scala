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

package org.apache.paimon.spark

import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

case class FormatTableScanBuilder(table: FormatTable) extends PaimonBaseScanBuilder {

  private var columnPruningApplied = false

  override def pruneColumns(requiredSchema: StructType): Unit = {
    super.pruneColumns(requiredSchema)
    columnPruningApplied = true
  }

  override def build(): PaimonFormatTableScan = {
    // Spark calls pruneColumns before building a query scan, even when it needs every column.
    // A fresh scan built for relation statistics should not list every catalog-managed partition.
    if (
      !columnPruningApplied && table.partitionManager() != null &&
      pushedPartitionFilters.isEmpty && pushedDataFilters.isEmpty && pushedLimit.isEmpty
    ) {
      new PaimonFormatTableScan(
        table,
        requiredSchema,
        pushedPartitionFilters,
        pushedDataFilters,
        pushedLimit) {
        override def estimateStatistics: Statistics = new Statistics {
          override def sizeInBytes(): OptionalLong = OptionalLong.empty()
          override def numRows(): OptionalLong = OptionalLong.empty()
        }
      }
    } else {
      PaimonFormatTableScan(
        table,
        requiredSchema,
        pushedPartitionFilters,
        pushedDataFilters,
        pushedLimit)
    }
  }
}
