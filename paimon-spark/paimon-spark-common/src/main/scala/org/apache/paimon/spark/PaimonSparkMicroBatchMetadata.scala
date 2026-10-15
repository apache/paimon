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

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD

import java.lang.reflect.Method
import java.util.{IdentityHashMap, List => JList, Map => JMap, Optional, UUID}

import scala.util.control.NonFatal

/** Driver-side access to metadata planned for a Paimon streaming micro-batch. */
@Experimental
final class PaimonSparkMicroBatchMetadata private ()

object PaimonSparkMicroBatchMetadata {

  private val StreamingQueryIdKey = "sql.streaming.queryId"

  /**
   * Returns written columns for a raw foreachBatch Dataset with exactly one Paimon streaming
   * source. This method only inspects driver-side RDD planning metadata and does not run a Spark
   * job. The result is empty when the Dataset is not backed by a Paimon source, the lineage is
   * incomplete, or multiple Paimon sources make the result ambiguous.
   */
  def writtenColumnIds(batch: Dataset[_]): Optional[JList[Integer]] = {
    try {
      extractWrittenColumnIds(batch)
    } catch {
      case NonFatal(_) => Optional.empty()
      case _: LinkageError => Optional.empty()
    }
  }

  private def extractWrittenColumnIds(batch: Dataset[_]): Optional[JList[Integer]] = {
    if (!hasExactlyOnePaimonSource(batch)) {
      return Optional.empty()
    }

    val visited = new IdentityHashMap[RDD[_], java.lang.Boolean]()
    var only: PaimonMicroBatchMetadata = null

    def inspectOccurrence(dataSourceRDD: DataSourceRDD): Boolean = {
      var occurrenceOnly: PaimonMicroBatchMetadata = null
      var inputCount = 0
      var valid = true
      val inputs = dataSourceInputPartitions(dataSourceRDD)

      while (valid && inputs.hasNext) {
        inputs.next() match {
          case input: PaimonMicroBatchInputPartition =>
            val current = input.metadata
            if (current eq null) {
              valid = false
            } else if (occurrenceOnly eq null) {
              occurrenceOnly = current
              inputCount += 1
            } else if ((occurrenceOnly eq current) || occurrenceOnly == current) {
              inputCount += 1
            } else {
              valid = false
            }
          case _: PaimonInputPartition => valid = false
          case _ =>
        }
      }

      if (!valid || ((occurrenceOnly ne null) && inputCount != occurrenceOnly.splitCount)) {
        false
      } else if (occurrenceOnly eq null) {
        true
      } else if (only eq null) {
        only = occurrenceOnly
        true
      } else {
        (only eq occurrenceOnly) || only == occurrenceOnly
      }
    }

    def visit(rdd: RDD[_]): Boolean = {
      if (visited.containsKey(rdd)) {
        true
      } else {
        visited.put(rdd, java.lang.Boolean.TRUE)
        val valid =
          rdd match {
            case dataSourceRDD: DataSourceRDD => inspectOccurrence(dataSourceRDD)
            case _ => true
          }
        if (!valid) {
          false
        } else {
          val dependencies = rdd.dependencies.iterator
          var complete = true
          while (complete && dependencies.hasNext) {
            complete = visit(dependencies.next().rdd)
          }
          complete
        }
      }
    }

    if (!visit(batch.queryExecution.toRdd) || (only eq null)) {
      Optional.empty()
    } else {
      only.writtenColumnIds
    }
  }

  /**
   * The input partitions of every `DataSourceRDDPartition` of `rdd`, lazily. All partitions of one
   * RDD are the same class, so the accessor is resolved once for the whole RDD.
   */
  private def dataSourceInputPartitions(rdd: DataSourceRDD): Iterator[InputPartition] = {
    val partitions = rdd.partitions
    if (partitions.isEmpty) {
      Iterator.empty
    } else {
      val accessor = inputPartitionAccessor(partitions.head)
      partitions.iterator.flatMap {
        partition =>
          requireNonNullPartition(partition)
          normalizeInputPartitions(accessor.invoke(partition))
      }
    }
  }

  private def inputPartitionAccessor(partition: Partition): Method = {
    requireNonNullPartition(partition)
    methodOf(partition, "inputPartitions")
      .orElse(methodOf(partition, "inputPartition"))
      .getOrElse(throw new IllegalArgumentException(
        s"No input partition accessor on ${partition.getClass.getName}."))
  }

  private def requireNonNullPartition(partition: Partition): Unit = {
    if (partition == null) {
      throw new IllegalArgumentException("Data source RDD partition must not be null.")
    }
  }

  private def methodOf(partition: Partition, name: String): Option[Method] =
    try {
      Some(partition.getClass.getMethod(name))
    } catch {
      case _: NoSuchMethodException => None
    }

  /**
   * `DataSourceRDDPartition` has held its input partition(s) in three shapes across the supported
   * Spark versions: a bare `InputPartition` (3.2), a `Seq[InputPartition]` from 3.3 to 4.1, where
   * the RDD itself grouped storage-partitioned-join partitions, and an `Option[InputPartition]`
   * since 4.2 (SPARK-55535), which moved that grouping out into `GroupPartitionsExec`. A `None`
   * there is a padded empty partition and contributes no input, the same as the empty `Seq` the
   * middle shape used for it.
   */
  private def normalizeInputPartitions(value: Any): Seq[InputPartition] =
    value match {
      case null => throw new IllegalArgumentException("Input partitions must not be null.")
      case input: InputPartition => Seq(input)
      case values: scala.collection.Seq[_] =>
        values.iterator.flatMap(normalizeInputPartitions).toVector
      case option: Option[_] =>
        option.iterator.flatMap(normalizeInputPartitions).toVector
      case other =>
        throw new IllegalArgumentException(
          s"Unexpected input partition type ${other.getClass.getName}.")
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
    sources.headOption.exists(
      first => first.isInstanceOf[PaimonMicroBatchStream] && sources.forall(_ eq first))
  }
}
