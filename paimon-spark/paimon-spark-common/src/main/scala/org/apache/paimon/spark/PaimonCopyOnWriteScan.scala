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

import org.apache.paimon.predicate.{Predicate, TopN}
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.{FileStoreTable, InnerTable}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{Batch, SupportsRuntimeFiltering}
import org.apache.spark.sql.sources.{Filter, In}
import org.apache.spark.sql.types.StructType

import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.collection.mutable

case class PaimonCopyOnWriteScan(
    table: InnerTable,
    requiredSchema: StructType,
    filters: Seq[Predicate],
    reservedFilters: Seq[Filter],
    override val pushDownLimit: Option[Int],
    override val pushDownTopN: Option[TopN],
    bucketedScanDisabled: Boolean = false)
  extends PaimonScanCommon(
    table,
    requiredSchema,
    filters,
    reservedFilters,
    pushDownLimit,
    pushDownTopN)
  with SupportsRuntimeFiltering {

  var filteredLocations: mutable.Set[String] = mutable.Set[String]()

  var filteredFileNames: mutable.Set[String] = mutable.Set[String]()

  var dataSplits: Array[DataSplit] = Array()

  def disableBucketedScan(): PaimonCopyOnWriteScan = {
    copy(bucketedScanDisabled = true)
  }

  override def filterAttributes(): Array[NamedReference] = {
    Array(Expressions.column(FILE_PATH_COLUMN))
  }

  override def filter(runtimefilters: Array[Filter]): Unit = {
    for (filter <- runtimefilters) {
      filter match {
        case in: In if in.attribute.equalsIgnoreCase(FILE_PATH_COLUMN) =>
          for (value <- in.values) {
            val location = value.asInstanceOf[String]
            filteredLocations.add(location)
            filteredFileNames.add(Paths.get(location).getFileName.toString)
          }
        case _ => logWarning("Unsupported runtime filter")
      }
    }

    table match {
      case fileStoreTable: FileStoreTable =>
        val snapshotReader = fileStoreTable.newSnapshotReader()
        if (fileStoreTable.coreOptions().manifestDeleteFileDropStats()) {
          snapshotReader.dropStats()
        }

        filters.foreach(snapshotReader.withFilter)

        snapshotReader.withDataFileNameFilter(fileName => filteredFileNames.contains(fileName))

        dataSplits =
          snapshotReader.read().splits().asScala.collect { case s: DataSplit => s }.toArray

      case _ => throw new RuntimeException("Only FileStoreTable support.")
    }

  }

  override def toBatch: Batch = {
    PaimonBatch(
      getInputPartitions(dataSplits.asInstanceOf[Array[Split]]),
      readBuilder,
      coreOptions.blobAsDescriptor(),
      metadataColumns)
  }
}
