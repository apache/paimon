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

package org.apache.paimon.spark.write

import org.apache.paimon.CoreOptions.ChangelogProducer
import org.apache.paimon.options.Options
import org.apache.paimon.spark._
import org.apache.paimon.spark.commands.SchemaHelper
import org.apache.paimon.spark.rowops.PaimonCopyOnWriteScan
import org.apache.paimon.table.BucketMode.BUCKET_UNAWARE
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class PaimonV2Write(
    override val originTable: FileStoreTable,
    overwritePartitions: Option[Map[String, String]],
    copyOnWriteScan: Option[PaimonCopyOnWriteScan],
    dataSchema: StructType,
    options: Options
) extends Write
  with RequiresDistributionAndOrdering
  with SchemaHelper
  with Logging {

  private val writeSchema = mergeSchema(dataSchema, options)
  private val writeRequirement = PaimonWriteRequirement(table)

  override def requiredDistribution(): Distribution = {
    val distribution = writeRequirement.distribution
    logInfo(s"Requesting $distribution as write distribution for table ${table.name()}")
    distribution
  }

  override def requiredOrdering(): Array[SortOrder] = {
    val ordering = writeRequirement.ordering
    logInfo(s"Requesting ${ordering.mkString(",")} as write ordering for table ${table.name()}")
    ordering
  }

  override def toBatch: BatchWrite = {
    PaimonBatchWrite(table, writeSchema, dataSchema, overwritePartitions, copyOnWriteScan)
  }

  override def supportedCustomMetrics(): Array[CustomMetric] = {
    val buffer = mutable.ArrayBuffer[CustomMetric](
      // write metrics
      PaimonNumWritersMetric(),
      // commit metrics
      PaimonCommitDurationMetric(),
      PaimonAddedTableFilesMetric()
    )
    if (copyOnWriteScan.isEmpty) {
      // todo: support record metrics for row level ops
      buffer += PaimonInsertedRecordsMetric()
    }
    if (copyOnWriteScan.nonEmpty) {
      buffer += PaimonDeletedTableFilesMetric()
    }
    if (!coreOptions.changelogProducer().equals(ChangelogProducer.NONE)) {
      buffer += PaimonAppendedChangelogFilesMetric()
    }
    if (!table.partitionKeys().isEmpty) {
      buffer += PaimonPartitionsWrittenMetric()
    }
    if (!table.bucketMode().equals(BUCKET_UNAWARE)) {
      buffer += PaimonBucketsWrittenMetric()
    }
    buffer.toArray
  }

  override def toString: String = {
    val overwriteDynamic = table.coreOptions().dynamicPartitionOverwrite()
    val overwriteDynamicStr = if (overwriteDynamic) {
      ", overwriteDynamic=true"
    } else {
      ""
    }
    val overwritePartitionsStr = overwritePartitions match {
      case Some(partitions) if partitions.nonEmpty => s", overwritePartitions=$partitions"
      case Some(_) if !overwriteDynamic => ", overwriteTable=true"
      case _ => ""
    }
    s"PaimonWrite(table=${table.fullName()}$overwriteDynamicStr$overwritePartitionsStr)"
  }

  override def description(): String = toString
}
