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

import org.apache.paimon.annotation.Experimental
import org.apache.paimon.spark.sources.PaimonMicroBatchStream
import org.apache.paimon.table.source.WrittenColumns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.{IdentityHashMap, Map => JMap, Optional, UUID}

import scala.collection.mutable
import scala.util.control.NonFatal

/** Driver-side access to metadata planned for a Paimon streaming micro-batch. */
@Experimental
final class PaimonSparkMicroBatchMetadata private ()

object PaimonSparkMicroBatchMetadata {

  private val StreamingQueryIdKey = "sql.streaming.queryId"

  /**
   * Returns written columns for a raw foreachBatch Dataset with exactly one Paimon streaming
   * source. This method only inspects driver-side RDD planning metadata and does not run a Spark
   * job. The result is empty when metadata collection was not enabled, the Dataset is not backed by
   * a Paimon source, the lineage is incomplete, or multiple Paimon sources make the result
   * ambiguous.
   */
  def writtenColumns(batch: Dataset[_]): Optional[WrittenColumns] = {
    try {
      extractWrittenColumns(batch)
    } catch {
      case NonFatal(_) => Optional.empty()
      case _: LinkageError => Optional.empty()
    }
  }

  private def extractWrittenColumns(batch: Dataset[_]): Optional[WrittenColumns] = {
    if (!hasExactlyOnePaimonSource(batch)) {
      return Optional.empty()
    }

    val visited = new IdentityHashMap[RDD[_], java.lang.Boolean]()
    val metadata = mutable.ArrayBuffer.empty[PaimonMicroBatchMetadata]
    var incompletePaimonSource = false

    def visit(rdd: RDD[_]): Unit = {
      if (!visited.containsKey(rdd)) {
        visited.put(rdd, java.lang.Boolean.TRUE)
        rdd match {
          case dataSourceRDD: DataSourceRDD =>
            dataSourceRDD.partitions.foreach {
              partition =>
                SparkShimLoader.shim.dataSourceInputPartitions(partition).foreach {
                  case input: PaimonMicroBatchInputPartition => metadata += input.metadata
                  case _: PaimonInputPartition => incompletePaimonSource = true
                  case _ =>
                }
            }
          case _ =>
        }
        rdd.dependencies.foreach(dependency => visit(dependency.rdd))
      }
    }

    visit(batch.queryExecution.toRdd)

    val distinct = metadata.distinct
    if (incompletePaimonSource || distinct.size != 1) {
      Optional.empty()
    } else {
      val only = distinct.head
      if (metadata.size != only.splitCount) {
        Optional.empty()
      } else {
        Optional.of(only.writtenColumns)
      }
    }
  }

  private def hasExactlyOnePaimonSource(batch: Dataset[_]): Boolean = {
    val queryId = batch.sparkSession.sparkContext.getLocalProperty(StreamingQueryIdKey)
    if (queryId == null) {
      return false
    }

    val sharedState =
      batch.sparkSession.getClass.getMethod("sharedState").invoke(batch.sparkSession)
    val activeQueries =
      sharedState.getClass
        .getMethod("activeStreamingQueries")
        .invoke(sharedState)
        .asInstanceOf[JMap[UUID, AnyRef]]
    val execution = activeQueries.get(UUID.fromString(queryId))
    if (execution == null) {
      return false
    }

    // Spark replaces sources without new offsets with LocalRelation before foreachBatch. Their
    // RDD lineage therefore contains no InputPartition to inspect. The active StreamExecution is
    // the only per-query structure which still retains every source. Keep this Spark-internal
    // access isolated here and fail closed if a Spark version changes it.
    val sources =
      execution.getClass.getMethod("sources").invoke(execution).asInstanceOf[Seq[AnyRef]]
    val distinctSources = new IdentityHashMap[AnyRef, java.lang.Boolean]()
    sources.foreach(source => distinctSources.put(source, java.lang.Boolean.TRUE))

    if (distinctSources.size() != 1) {
      false
    } else {
      distinctSources.keySet().iterator().next().isInstanceOf[PaimonMicroBatchStream]
    }
  }
}
