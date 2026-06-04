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

package org.apache.paimon.spark.read

import org.apache.paimon.table.{DataTable, Table}
import org.apache.paimon.tag.BatchReadTagCreator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.ConcurrentHashMap

/**
 * A Spark [[QueryExecutionListener]] that cleans up batch-read protection tags when a query
 * completes (success or failure). TTL expiration serves as a safety net if this cleanup fails.
 */
class BatchReadTagCleanupListener extends QueryExecutionListener with Logging {

  private val pendingCleanups = new ConcurrentHashMap[String, DataTable]()

  def registerCleanup(tagName: String, table: Table): Unit = {
    table match {
      case dt: DataTable => pendingCleanups.put(tagName, dt)
      case _ =>
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    cleanupAll()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    cleanupAll()
  }

  private def cleanupAll(): Unit = {
    val iter = pendingCleanups.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val tagName = entry.getKey
      val dataTable = entry.getValue
      iter.remove()
      try {
        val creator = new BatchReadTagCreator(
          dataTable.tagManager(),
          dataTable.snapshotManager(),
          dataTable.coreOptions().scanPlanAutoTagTimeRetained())
        creator.deleteReadTag(tagName)
      } catch {
        case e: Exception =>
          logWarning(
            s"Failed to delete batch read protection tag '$tagName'. " +
              "It will be cleaned up by TTL expiration.",
            e)
      }
    }
  }
}

object BatchReadTagCleanupListener {

  @volatile private var instance: BatchReadTagCleanupListener = _

  def getOrCreate(spark: SparkSession): BatchReadTagCleanupListener = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new BatchReadTagCleanupListener()
          spark.listenerManager.register(instance)
        }
      }
    }
    instance
  }
}
