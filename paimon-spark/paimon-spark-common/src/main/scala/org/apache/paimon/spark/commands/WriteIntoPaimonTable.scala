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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.DYNAMIC_PARTITION_OVERWRITE
import org.apache.paimon.options.Options
import org.apache.paimon.partition.actions.PartitionMarkDoneAction
import org.apache.paimon.spark._
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.utils.{InternalRowPartitionComputer, PartitionPathUtils, TypeUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

import scala.collection.JavaConverters._

/** Used to write a [[DataFrame]] into a paimon table. */
case class WriteIntoPaimonTable(
    override val originTable: FileStoreTable,
    saveMode: SaveMode,
    data: DataFrame,
    options: Options)
  extends RunnableCommand
  with PaimonCommand
  with SchemaHelper
  with Logging {

  private lazy val mergeSchema = options.get(SparkConnectorOptions.MERGE_SCHEMA)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (mergeSchema) {
      val dataSchema = SparkSystemColumns.filterSparkSystemColumns(data.schema)
      val allowExplicitCast = options.get(SparkConnectorOptions.EXPLICIT_CAST)
      mergeAndCommitSchema(dataSchema, allowExplicitCast)
    }

    val (dynamicPartitionOverwriteMode, overwritePartition) = parseSaveMode()
    // use the extra options to rebuild the table object
    updateTableWithOptions(
      Map(DYNAMIC_PARTITION_OVERWRITE.key -> dynamicPartitionOverwriteMode.toString))

    val writer = PaimonSparkWriter(table)
    if (overwritePartition != null) {
      writer.writeBuilder.withOverwrite(overwritePartition.asJava)
    }
    val commitMessages = writer.write(data)
    writer.commit(commitMessages)

    markDone(commitMessages)
    Seq.empty
  }

  private def markDone(commitMessages: Seq[CommitMessage]): Unit = {
    val coreOptions = table.coreOptions()
    if (coreOptions.toConfiguration.get(CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT)) {
      val actions = PartitionMarkDoneAction.createActions(originTable, table.coreOptions())
      val partitionComputer = new InternalRowPartitionComputer(
        coreOptions.partitionDefaultName,
        TypeUtils.project(table.rowType(), table.partitionKeys()),
        table.partitionKeys().asScala.toArray
      )
      val partitions = commitMessages
        .map(c => c.partition())
        .map(p => PartitionPathUtils.generatePartitionPath(partitionComputer.generatePartValues(p)))
      for (elem <- partitions) {
        actions.forEach(a => a.markDone(elem))
      }
    }
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

  override def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    this.asInstanceOf[WriteIntoPaimonTable]

}
