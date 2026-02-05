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

package org.apache.paimon.spark.rowops

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{And, CompoundPredicate, Predicate, PredicateBuilder}
import org.apache.paimon.spark.commands.SparkDataFileMeta
import org.apache.paimon.spark.commands.SparkDataFileMeta.convertToSparkDataFileMeta
import org.apache.paimon.spark.scan.BaseScan
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.{DataSplit, ReadBuilder, Split}

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{Predicate => SparkPredicate}
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering
import org.apache.spark.sql.sources.{Filter, In}
import org.apache.spark.sql.types.StructType

import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Note: The [[pushedPartitionFilters]] and [[pushedDataFilters]] are intentionally set to empty
 * because file-level filtering is handled through Spark's runtime V2 filtering mechanism.
 *
 * When Spark's runtime filter is not applied (e.g., when NOT MATCHED BY SOURCE is present in
 * MergeInto), this scan will read all data from the table.
 */
case class PaimonCopyOnWriteScan(
    table: FileStoreTable,
    requiredSchema: StructType,
    pushedPartitionFilters: Seq[PartitionPredicate] = Seq.empty,
    pushedDataFilters: Seq[Predicate] = Seq.empty)
  extends BaseScan
  with SupportsRuntimeV2Filtering {

  // Track whether filter() has been called
  @volatile private var filterApplied: Boolean = false

  private val filteredFileNames: mutable.Set[String] = mutable.Set[String]()

  override def inputSplits: Array[Split] = {
    loadSplits()
    dataSplits.asInstanceOf[Array[Split]]
  }

  var dataSplits: Array[DataSplit] = Array()

  private def loadSplits(): Unit = {
    val snapshotReader = table.newSnapshotReader()
    if (table.coreOptions().manifestDeleteFileDropStats()) {
      snapshotReader.dropStats()
    }
    if (filterApplied) {
      snapshotReader.withDataFileNameFilter(fileName => filteredFileNames.contains(fileName))
    }
    dataSplits = snapshotReader.read().splits().asScala.collect { case s: DataSplit => s }.toArray
  }

  def scannedFiles: Seq[SparkDataFileMeta] = {
    dataSplits
      .flatMap(dataSplit => convertToSparkDataFileMeta(dataSplit, dataSplit.totalBuckets()))
      .toSeq
  }

  override def filterAttributes(): Array[NamedReference] = {
    Array(Expressions.column(FILE_PATH_COLUMN))
  }

  override def filter(predicates: Array[SparkPredicate]): Unit = {
    filterApplied = true
    val runtimeFilters: Array[Filter] = predicates.flatMap(PaimonUtils.filterV2ToV1)
    for (filter <- runtimeFilters) {
      filter match {
        case in: In if in.attribute.equalsIgnoreCase(FILE_PATH_COLUMN) =>
          for (value <- in.values) {
            val location = value.asInstanceOf[String]
            filteredFileNames.add(Paths.get(location).getFileName.toString)
          }
        case _ => logWarning("Unsupported runtime filter")
      }
    }
  }

}
