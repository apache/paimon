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

import org.apache.paimon.fs.{FileIO, Path}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

/** Owns external split metadata without retaining the scans which produced it. */
private[spark] object SplitMetadataCleanupManager extends Logging {

  private val listeners = new ConcurrentHashMap[String, CleanupListener]()
  private val endedApplications = ConcurrentHashMap.newKeySet[String]()

  def register(
      spark: SparkSession,
      fileIO: FileIO,
      directory: Path,
      broadcastFileIO: Broadcast[FileIO]): SplitMetadataResourceHandle = {
    val applicationId = spark.sparkContext.applicationId
    val resource = new SplitMetadataResourceHandle(fileIO, directory, broadcastFileIO)
    val cleanupImmediately = synchronized {
      if (spark.sparkContext.isStopped || endedApplications.contains(applicationId)) {
        true
      } else {
        var listener = listeners.get(applicationId)
        if (listener == null) {
          val newListener = new CleanupListener(applicationId)
          try spark.sparkContext.addSparkListener(newListener)
          catch {
            case error: Exception => throw error
          }
          if (spark.sparkContext.isStopped || endedApplications.contains(applicationId)) {
            listener = null
          } else {
            listeners.put(applicationId, newListener)
            listener = newListener
          }
        }
        listener == null || !listener.register(resource)
      }
    }
    if (cleanupImmediately) {
      resource.close()
    }
    resource
  }

  private def applicationEnded(applicationId: String, listener: CleanupListener): Unit =
    synchronized {
      endedApplications.add(applicationId)
      listeners.remove(applicationId, listener)
    }

  final private class CleanupListener(applicationId: String) extends SparkListener {

    private val resources = ConcurrentHashMap.newKeySet[SplitMetadataResourceHandle]()
    private var closed = false

    def register(resource: SplitMetadataResourceHandle): Boolean = synchronized {
      if (!closed) {
        resources.add(resource)
      }
      !closed
    }

    override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
      applicationEnded(applicationId, this)
      val pending = synchronized {
        closed = true
        val copy = resources.asScala.toList
        resources.clear()
        copy
      }
      pending.foreach(_.close())
    }
  }
}

/** Idempotent ownership handle for one scan's external split metadata. */
final private[spark] class SplitMetadataResourceHandle(
    fileIO: FileIO,
    directory: Path,
    broadcastFileIO: Broadcast[FileIO])
  extends AutoCloseable
  with Logging {

  private val closed = new AtomicBoolean(false)

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try {
        SharedSplitMetadataExternalizer.cleanup(fileIO, directory)
      } catch {
        case error: Exception =>
          logWarning(s"Failed to clean shared split metadata directory $directory", error)
      } finally {
        try broadcastFileIO.destroy()
        catch {
          case error: Exception =>
            logWarning(s"Failed to destroy FileIO broadcast for $directory", error)
        }
      }
    }
  }
}
