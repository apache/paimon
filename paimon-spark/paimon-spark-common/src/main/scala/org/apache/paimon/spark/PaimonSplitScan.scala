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

import org.apache.paimon.CoreOptions
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.table.{KnownSplitsTable, Table}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class PaimonSplitScanBuilder(table: KnownSplitsTable) extends PaimonBaseScanBuilder(table) {
  override def build(): Scan = {
    PaimonSplitScan(table, table.splits(), requiredSchema, pushed.map(_._2))
  }
}

/** For internal use only. */
case class PaimonSplitScan(
    table: Table,
    dataSplits: Array[DataSplit],
    requiredSchema: StructType,
    filters: Seq[Predicate])
  extends ColumnPruningAndPushDown
  with ScanHelper {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())

  override def toBatch: Batch = {
    PaimonBatch(
      getInputPartitions(dataSplits.asInstanceOf[Array[Split]]),
      readBuilder,
      metadataColumns)
  }

  override def description(): String = {
    val pushedFiltersStr = if (filters.nonEmpty) {
      ", PushedFilters: [" + filters.mkString(",") + "]"
    } else {
      ""
    }
    s"PaimonSplitScan: [${table.name}]" + pushedFiltersStr
  }
}

object PaimonSplitScan {
  def apply(table: Table, dataSplits: Array[DataSplit]): PaimonSplitScan = {
    val requiredSchema = SparkTypeUtils.fromPaimonRowType(table.rowType)
    new PaimonSplitScan(table, dataSplits, requiredSchema, Seq.empty)
  }
}
