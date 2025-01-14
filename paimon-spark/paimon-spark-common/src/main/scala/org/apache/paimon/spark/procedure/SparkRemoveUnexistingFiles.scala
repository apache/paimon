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

package org.apache.paimon.spark.procedure

import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataIncrement}
import org.apache.paimon.memory.MemorySegment
import org.apache.paimon.operation.ListUnexistingFiles
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl, CommitMessageSerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper

import java.util
import java.util.{Collections, UUID}

import scala.collection.JavaConverters._

case class SparkRemoveUnexistingFiles(
    table: FileStoreTable,
    dryRun: Boolean,
    parallelism: Int,
    @transient spark: SparkSession)
  extends SQLConfHelper
  with Logging {

  private def buildRDD() = {
    val binaryPartitions = table.newScan().listPartitions()
    val realParallelism = Math.min(binaryPartitions.size(), parallelism)

    val numPartitionFields = table.schema().partitionKeys().size()
    val pathAndMessage = spark.sparkContext
      .parallelize(
        binaryPartitions.asScala.map(partition => partition.toBytes).toSeq,
        realParallelism)
      .mapPartitions {
        iter =>
          {
            val reuse = new BinaryRow(numPartitionFields)
            val operation = new ListUnexistingFiles(table)
            val serializer = new CommitMessageSerializer()
            iter.flatMap(
              partitionBytes => {
                reuse.pointTo(MemorySegment.wrap(partitionBytes), 0, partitionBytes.length)
                operation.list(reuse).asScala.map {
                  case (bucket, metaMap) =>
                    val message = new CommitMessageImpl(
                      reuse,
                      bucket,
                      new DataIncrement(
                        Collections.emptyList(),
                        new util.ArrayList[DataFileMeta](metaMap.values()),
                        Collections.emptyList()),
                      CompactIncrement.emptyIncrement())
                    (metaMap.keySet().asScala.toSeq, serializer.serialize(message))
                }
              })
          }
      }
      .repartition(1)
      .cache()

    if (!dryRun) {
      pathAndMessage.foreachPartition {
        iter =>
          {
            val serializer = new CommitMessageSerializer()
            val messages = new util.ArrayList[CommitMessage]()
            iter.foreach {
              case (_, bytes) => messages.add(serializer.deserialize(serializer.getVersion, bytes))
            }
            val commit = table.newCommit(UUID.randomUUID().toString)
            commit.commit(Long.MaxValue, messages)
          }
      }
    }

    pathAndMessage.mapPartitions(
      iter => {
        iter.flatMap { case (paths, _) => paths }
      })
  }
}

object SparkRemoveUnexistingFiles extends SQLConfHelper {

  def execute(
      catalog: Catalog,
      databaseName: String,
      tableName: String,
      dryRun: Boolean,
      parallelismOpt: Integer): Array[String] = {
    val spark = SparkSession.active
    val parallelism = if (parallelismOpt == null) {
      Math.max(spark.sparkContext.defaultParallelism, conf.numShufflePartitions)
    } else {
      parallelismOpt.intValue()
    }

    val identifier = new Identifier(databaseName, tableName)
    val table = catalog.getTable(identifier)
    assert(
      table.isInstanceOf[FileStoreTable],
      s"Only FileStoreTable supports remove-unexsiting-files action. The table type is '${table.getClass.getName}'.")
    val fileStoreTable = table.asInstanceOf[FileStoreTable]
    SparkRemoveUnexistingFiles(fileStoreTable, dryRun, parallelism, spark).buildRDD().collect()
  }
}
