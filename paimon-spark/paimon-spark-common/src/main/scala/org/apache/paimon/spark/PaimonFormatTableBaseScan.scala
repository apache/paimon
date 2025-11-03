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
import org.apache.paimon.predicate.{Predicate, PredicateBuilder}
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.FormatTable.Format
import org.apache.paimon.table.format.FormatDataSplit
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.table.source.Split

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.orc.OrcFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.PaimonSparkSession
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

import scala.collection.JavaConverters._

/** Base Scan implementation for [[FormatTable]]. */
abstract class PaimonFormatTableBaseScan(
    table: FormatTable,
    requiredSchema: StructType,
    filters: Seq[Predicate],
    pushDownLimit: Option[Int])
  extends ColumnPruningAndPushDown
  with ScanHelper
  with SupportsReportStatistics {

  override val coreOptions: CoreOptions = CoreOptions.fromMap(table.options())
  protected var inputSplits: Array[Split] = _
  protected var inputPartitions: Seq[PaimonInputPartition] = _

  override lazy val readBuilder: ReadBuilder = {
    val builder = table.newReadBuilder().withReadType(readTableRowType)
    if (filters.nonEmpty) {
      val pushedPredicate = PredicateBuilder.and(filters: _*)
      builder.withFilter(pushedPredicate)
    }
    pushDownLimit.foreach(builder.withLimit)
    pushDownTopN.foreach(builder.withTopN)
    builder
  }

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

  final def lazyInputPartitions: Seq[PaimonInputPartition] = {
    if (inputPartitions == null) {
      inputPartitions = getInputPartitions(getOriginSplits)
    }
    inputPartitions
  }

  override def toBatch: Batch = {
    PaimonBatch(lazyInputPartitions, readBuilder, coreOptions.blobAsDescriptor(), metadataColumns)
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        var totalFileSize = 0L
        lazyInputPartitions
          .flatMap(_.splits)
          .foreach(
            split => {
              split match {
                case formatSplit: FormatDataSplit if totalFileSize != Long.MaxValue =>
                  val length = math.max(0L, formatSplit.length())
                  if (length > 0) {
                    try {
                      totalFileSize = Math.addExact(totalFileSize, length)
                    } catch {
                      case _: ArithmeticException =>
                        totalFileSize = Long.MaxValue
                    }
                  }
                case _ =>
              }
            })
        OptionalLong.of(totalFileSize)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
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
