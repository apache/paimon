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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

/**
 * Cleans up batch-read protection tags when the SQL execution that planned the scan ends (success
 * or failure), or, for scans planned outside a running execution, when the next execution ends. TTL
 * expiration serves as a safety net if this cleanup fails.
 */
class BatchReadTagCleanupListener extends SparkListener with Logging {

  private val pendingCleanups =
    new ConcurrentHashMap[java.lang.Long, ConcurrentHashMap[String, DataTable]]()

  // scans planned outside a running SQL execution, e.g. through Dataset.rdd on Spark 3 or
  // Dataset.cache, with the time they were planned
  private val unownedCleanups = new ConcurrentHashMap[String, (DataTable, Long)]()

  def registerCleanup(executionId: Option[Long], tagName: String, table: Table): Unit = {
    table match {
      case dt: DataTable =>
        executionId.filter(SQLExecution.getQueryExecution(_) != null) match {
          case Some(id) =>
            // adds the tag inside compute, so it never lands in a map that onOtherEvent removed
            pendingCleanups.compute(
              Long.box(id),
              (_: java.lang.Long, tags: ConcurrentHashMap[String, DataTable]) => {
                val result = if (tags == null) new ConcurrentHashMap[String, DataTable]() else tags
                result.put(tagName, dt)
                result
              }
            )
          case None => unownedCleanups.put(tagName, (dt, System.currentTimeMillis()))
        }
      case _ =>
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case end: SparkListenerSQLExecutionEnd =>
        Option(pendingCleanups.remove(end.executionId)).foreach(cleanup)
        // executions that have ended without their end event reaching this listener
        pendingCleanups.keySet().asScala.foreach {
          id =>
            if (SQLExecution.getQueryExecution(id) == null) {
              Option(pendingCleanups.remove(id)).foreach(cleanup)
            }
        }
        // the end event of an execution that ended before a scan was planned can still arrive
        // after it, and must not clean that scan's tag
        val iter = unownedCleanups.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val (dataTable, plannedAt) = entry.getValue
          if (plannedAt < end.time) {
            iter.remove()
            deleteTag(entry.getKey, dataTable)
          }
        }
      case _ =>
    }
  }

  private def cleanup(tags: ConcurrentHashMap[String, DataTable]): Unit = {
    val iter = tags.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      iter.remove()
      deleteTag(entry.getKey, entry.getValue)
    }
  }

  private def deleteTag(tagName: String, dataTable: DataTable): Unit = {
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

object BatchReadTagCleanupListener {

  @volatile private var instance: BatchReadTagCleanupListener = _
  @volatile private var registeredContext: SparkContext = _

  def getOrCreate(spark: SparkSession): BatchReadTagCleanupListener = {
    val sc = spark.sparkContext
    if (instance == null || (registeredContext ne sc)) {
      synchronized {
        if (instance == null) {
          instance = new BatchReadTagCleanupListener()
        }
        if (registeredContext ne sc) {
          sc.addSparkListener(instance)
          registeredContext = sc
        }
      }
    }
    instance
  }
}
