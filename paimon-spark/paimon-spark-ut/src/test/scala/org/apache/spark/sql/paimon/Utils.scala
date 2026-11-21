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

package org.apache.spark.sql.paimon

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, SparkEnv}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{Utils => SparkUtils}

import java.io.File

/** A wrapper that some Objects or Classes is limited to access beyond [[org.apache.spark]] package. */
object Utils {

  def createTempDir: File = SparkUtils.createTempDir(System.getProperty("java.io.tmpdir"), "spark")

  def waitUntilEventEmpty(spark: SparkSession): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty()
  }

  def materializeShuffle(spark: SparkSession, dependency: ShuffleDependency[_, _, _]): Unit = {
    spark.sparkContext.submitMapStage(dependency).get()
  }

  def availableShuffleOutputs(shuffleId: Int): Int = {
    SparkEnv.get.mapOutputTracker
      .asInstanceOf[MapOutputTrackerMaster]
      .getNumAvailableOutputs(shuffleId)
  }

  def invalidateShuffleOutputs(shuffleId: Int, partitions: Seq[Int]): Unit = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val locations = tracker.shuffleStatuses(shuffleId).withMapStatuses {
      statuses => partitions.map(index => (index, statuses(index).location))
    }
    locations.foreach {
      case (index, location) => tracker.unregisterMapOutput(shuffleId, index, location)
    }
  }

}
