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
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.source.Split

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** A ScanBuilder implementation for {@link FormatTable} that supports basic scan operations. */
case class PaimonFormatTableScanBuilder(
    table: FormatTable,
    requiredSchema: StructType,
    filters: Seq[Predicate])
  extends ScanBuilder {
  override def build() = PaimonFormatTableScan(table, requiredSchema, filters)
}

case class PaimonFormatTableScan(
    table: FormatTable,
    requiredSchema: StructType,
    filters: Seq[Predicate])
  extends ColumnPruningAndPushDown
  with ScanHelper {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())
  protected var inputSplits: Array[Split] = _

  def getOriginSplits: Array[Split] = {
    if (inputSplits == null) {
      inputSplits = readBuilder
        .newScan()
        .plan()
        .splits()
        .asScala
        .toArray
    }
    inputSplits
  }

  override def toBatch: Batch = {
    PaimonBatch(
      getInputPartitions(getOriginSplits),
      readBuilder,
      coreOptions.blobAsDescriptor(),
      metadataColumns)
  }

  override def supportedCustomMetrics: Array[CustomMetric] = {
    Array(
      PaimonNumSplitMetric(),
      PaimonSplitSizeMetric(),
      PaimonAvgSplitSizeMetric(),
      PaimonResultedTableFilesMetric()
    )
  }

  override def reportDriverMetrics(): Array[CustomTaskMetric] = {
    val filesCount = getOriginSplits.length
    Array(
      PaimonResultedTableFilesTaskMetric(filesCount)
    )
  }

  override def description(): String = {
    val pushedFiltersStr = if (filters.nonEmpty) {
      ", PushedFilters: [" + filters.mkString(",") + "]"
    } else {
      ""
    }
    s"PaimonFormatTableScan: [${table.name}]" + pushedFiltersStr
  }
}
