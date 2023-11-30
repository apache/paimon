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

import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.table.{DataTable, Table}
import org.apache.paimon.table.source.{ReadBuilder, Split}

import org.apache.spark.sql.Utils.fieldReference
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics, SupportsRuntimeFiltering}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.sources.{EqualTo, Filter, In}
import org.apache.spark.sql.types.StructType

import java.util.OptionalLong

import scala.collection.JavaConverters._

case class PaimonScan(table: Table, readBuilder: ReadBuilder)
  extends Scan
  with SupportsReportStatistics
  with SupportsRuntimeFiltering {

  private var splits: Array[Split] = _

  override def description(): String = {
    s"paimon(${readBuilder.tableName()})"
  }

  override def readSchema(): StructType = {
    SparkTypeUtils.fromPaimonRowType(readBuilder.readType())
  }

  override def toBatch: Batch = {
    PaimonBatch(getSplits, readBuilder)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PaimonMicroBatchStream(table.asInstanceOf[DataTable], readBuilder, checkpointLocation)
  }

  override def estimateStatistics(): Statistics = {
    val rowCount = getSplits.map(_.rowCount).sum
    val scannedTotalSize = rowCount * readSchema().defaultSize

    new Statistics {
      override def sizeInBytes(): OptionalLong = OptionalLong.of(scannedTotalSize)

      override def numRows(): OptionalLong = OptionalLong.of(rowCount)
    }
  }

  override def filterAttributes(): Array[NamedReference] = {
    val requiredFields = readBuilder.readType().getFieldNames.asScala
    table
      .partitionKeys()
      .asScala
      .toArray
      .filter(requiredFields.contains)
      .map(fieldReference)
  }

  override def filter(filters: Array[Filter]): Unit = {
    val converter = new SparkFilterConverter(table.rowType())
    val partitionFilter = filters.flatMap {
      case in @ In(attr, _) if table.partitionKeys().contains(attr) =>
        Some(converter.convert(in))
      case _ => None
    }
    if (partitionFilter.nonEmpty) {
      readBuilder.withFilter(partitionFilter.head)
      splits = readBuilder.newScan().plan().splits().asScala.toArray
    }
  }

  private def getSplits: Array[Split] = {
    if (splits == null) {
      splits = readBuilder.newScan().plan().splits().asScala.toArray
    }
    splits
  }

}
