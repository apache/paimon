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

package org.apache.paimon.spark.commands

import org.apache.paimon.CoreOptions.DYNAMIC_PARTITION_OVERWRITE
import org.apache.paimon.options.Options
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate
import org.apache.paimon.spark._
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.table.{FileStoreTable, PrimaryKeyFileStoreTable}
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.utils.{ChainTableUtils, RowDataToObjectArrayConverter}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

import scala.collection.JavaConverters._

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(
    override val originTable: FileStoreTable,
    saveMode: SaveMode,
    _data: DataFrame,
    options: Options,
    batchId: Long = -1)
  extends RunnableCommand
  with ExpressionHelper
  with SchemaHelper
  with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val data = mergeSchema(sparkSession, _data, options)

    val (dynamicPartitionOverwriteMode, overwritePartition) = parseSaveMode()
    // use the extra options to rebuild the table object
    updateTableWithOptions(
      Map(DYNAMIC_PARTITION_OVERWRITE.key -> dynamicPartitionOverwriteMode.toString))

    val writer = PaimonSparkWriter(table, batchId = batchId)
    if (overwritePartition != null) {
      writer.writeBuilder.withOverwrite(overwritePartition.asJava)
    }
    val commitMessages = writer.write(data)
    writer.commit(commitMessages)
    truncatePartitionsForChainTable(
      dynamicPartitionOverwriteMode,
      overwritePartition,
      commitMessages)
    Seq.empty
  }

  private def parseSaveMode(): (Boolean, Map[String, String]) = {
    var dynamicPartitionOverwriteMode = false
    val overwritePartition = saveMode match {
      case InsertInto => null
      case Overwrite(filter) =>
        if (filter.isEmpty) {
          Map.empty[String, String]
        } else if (isTruncate(filter.get)) {
          Map.empty[String, String]
        } else {
          convertPartitionFilterToMap(filter.get, table.schema.logicalPartitionType())
        }
      case DynamicOverWrite =>
        dynamicPartitionOverwriteMode = true
        Map.empty[String, String]
      case _ =>
        throw new UnsupportedOperationException(s" This mode is unsupported for now.")
    }
    (dynamicPartitionOverwriteMode, overwritePartition)
  }

  private def truncatePartitionsForChainTable(
      dynamicPartitionOverwriteMode: Boolean,
      overwritePartition: Map[String, String],
      commitMessages: Seq[CommitMessage]): Unit = {
    val overwrite =
      dynamicPartitionOverwriteMode || (overwritePartition != null && !overwritePartition.isEmpty)
    if (ChainTableUtils.isChainScanFallbackDeltaBranch(table.coreOptions()) && overwrite) {
      val partitionPredicate = if (!dynamicPartitionOverwriteMode) {
        val partitionPredicate = createPartitionPredicate(
          overwritePartition.asJava,
          table.schema().logicalPartitionType,
          table.coreOptions().partitionDefaultName())
        PartitionPredicate.fromPredicate(table.schema().logicalPartitionType, partitionPredicate)
      } else {
        null
      }
      val snapshotTbl = table
        .asInstanceOf[PrimaryKeyFileStoreTable]
        .switchToBranch(table.coreOptions.scanFallbackSnapshotBranch())
      val sparkTable = SparkTable(snapshotTbl)
      val partitionConverter = new RowDataToObjectArrayConverter(
        table.schema().logicalPartitionType)
      val writePartitions = commitMessages
        .map(commitMessage => commitMessage.partition())
        .distinct
        .filter(partition => dynamicPartitionOverwriteMode || partitionPredicate.test(partition))
        .map(
          partition => {
            val result = partitionConverter.convert(partition)
            for (i <- 0 until result.length) {
              result(i) = DataConverter
                .fromPaimon(result(i), partitionConverter.rowType.getFields.get(i).`type`)
            }
            InternalRow.fromSeq(result)
          })
        .toArray
      sparkTable.dropPartitions(writePartitions)
    }
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    this.asInstanceOf[WriteIntoPaimonTable]
}
