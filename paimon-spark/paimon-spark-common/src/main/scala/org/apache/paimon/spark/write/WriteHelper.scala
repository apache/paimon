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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.TagCreationMode
import org.apache.paimon.partition.actions.PartitionMarkDoneAction
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.tag.TagBatchCreation
import org.apache.paimon.utils.{InternalRowPartitionComputer, PartitionPathUtils, PartitionStatisticsReporter, TypeUtils}

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

trait WriteHelper extends Logging {

  val table: FileStoreTable

  lazy val coreOptions: CoreOptions = table.coreOptions()

  def postCommit(messages: Seq[CommitMessage]): Unit = {
    if (messages.isEmpty) {
      return
    }

    reportToHms(messages)
    batchCreateTag()
    markDoneIfNeeded(messages)
  }

  private def reportToHms(messages: Seq[CommitMessage]): Unit = {
    val config = coreOptions.toConfiguration
    if (
      config.get(CoreOptions.PARTITION_IDLE_TIME_TO_REPORT_STATISTIC).toMillis <= 0 ||
      table.partitionKeys.isEmpty ||
      !coreOptions.partitionedTableInMetastore ||
      table.catalogEnvironment.partitionModification() == null
    ) {
      return
    }

    val partitionComputer = new InternalRowPartitionComputer(
      coreOptions.partitionDefaultName,
      table.schema.logicalPartitionType,
      table.partitionKeys.toArray(new Array[String](0)),
      coreOptions.legacyPartitionName()
    )
    val hmsReporter = new PartitionStatisticsReporter(
      table,
      table.catalogEnvironment.partitionModification()
    )

    val partitions = messages.map(_.partition()).distinct
    val currentTime = System.currentTimeMillis()
    try {
      partitions.foreach {
        partition =>
          val partitionPath = PartitionPathUtils.generatePartitionPath(
            partitionComputer.generatePartValues(partition))
          hmsReporter.report(partitionPath, currentTime)
      }
    } catch {
      case e: Throwable =>
        logWarning("Failed to report to hms", e)
    } finally {
      hmsReporter.close()
    }
  }

  private def batchCreateTag(): Unit = {
    if (coreOptions.tagCreationMode() == TagCreationMode.BATCH) {
      val tagCreation = new TagBatchCreation(table)
      tagCreation.createTag()
    }
  }

  private def markDoneIfNeeded(commitMessages: Seq[CommitMessage]): Unit = {
    if (coreOptions.toConfiguration.get(CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT)) {
      val actions =
        PartitionMarkDoneAction.createActions(getClass.getClassLoader, table, coreOptions)
      val partitionComputer = new InternalRowPartitionComputer(
        coreOptions.partitionDefaultName,
        TypeUtils.project(table.rowType(), table.partitionKeys()),
        table.partitionKeys().asScala.toArray,
        coreOptions.legacyPartitionName()
      )
      val partitions = commitMessages
        .map(c => c.partition())
        .distinct
        .map(p => PartitionPathUtils.generatePartitionPath(partitionComputer.generatePartValues(p)))
      for (partition <- partitions) {
        actions.forEach(a => a.markDone(partition))
      }
    }
  }
}
