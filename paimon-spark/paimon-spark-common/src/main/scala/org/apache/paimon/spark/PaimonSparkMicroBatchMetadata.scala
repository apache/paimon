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
import org.apache.paimon.spark.sources.{PaimonMicroBatchStream, PaimonSourceOffset}

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD

import java.util.{IdentityHashMap, List => JList, Map => JMap, Optional, UUID}

import scala.util.control.NonFatal

/** Driver-side access to metadata planned for a Paimon streaming micro-batch. */
@Experimental
final class PaimonSparkMicroBatchMetadata private ()

/** The Paimon source offsets bounding one Spark streaming micro-batch. */
@Experimental
final class PaimonSparkMicroBatchOffsetRange private[spark] (
    val startOffset: String,
    val endOffset: String)

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
      extractMetadata(batch).flatten
        .map(_.writtenColumnIds)
        .getOrElse(Optional.empty())
    } catch {
      case NonFatal(_) => Optional.empty()
      case _: LinkageError => Optional.empty()
    }
  }

  /**
   * Returns the exact source offset range of a raw foreachBatch Dataset. The batchId must be the
   * value passed to the same callback. Empty full snapshots are supported when the source offset
   * includes snapshot completion metadata (consumer-id is set). An empty result means the source,
   * batch, or its offsets cannot be identified unambiguously.
   */
  def offsetRange(batch: Dataset[_], batchId: Long): Optional[PaimonSparkMicroBatchOffsetRange] = {
    try {
      extractOffsetRange(batch, batchId)
    } catch {
      case NonFatal(_) => Optional.empty()
      case _: LinkageError => Optional.empty()
    }
  }

  private def extractOffsetRange(
      batch: Dataset[_],
      batchId: Long): Optional[PaimonSparkMicroBatchOffsetRange] = {
    val source = activePaimonSource(batch)
    if (source.isEmpty || activeBatchId(source.get._1) != batchId) {
      return Optional.empty()
    }

    extractMetadata(batch) match {
      case Some(Some(metadata)) =>
        Optional.of(new PaimonSparkMicroBatchOffsetRange(metadata.startOffset, metadata.endOffset))
      case Some(None) =>
        val (execution, stream) = source.get
        val end = executionOffset(execution, "availableOffsets", stream)
          .map(offset => PaimonSourceOffset(offset.json()))
        if (!end.exists(_.emptySnapshotCompleted)) {
          return Optional.empty()
        }
        val start = executionOffset(execution, startOffsetsMethod(execution), stream)
          .map(offset => PaimonSourceOffset(offset.json()))
          .orElse {
            if (batchId == 0 && end.get.snapshotId > 0) {
              Some(PaimonSourceOffset(end.get.snapshotId - 1, -1, scanSnapshot = true))
            } else {
              None
            }
          }
        start
          .filter(offset => offset.compareTo(end.get) < 0)
          .map(
            offset =>
              Optional.of(new PaimonSparkMicroBatchOffsetRange(offset.json(), end.get.json())))
          .getOrElse(Optional.empty())
      case None => Optional.empty()
    }
  }

  private def activeBatchId(execution: AnyRef): Long = {
    val value =
      try {
        execution.getClass.getMethod("currentBatchId").invoke(execution)
      } catch {
        case _: NoSuchMethodException =>
          // Spark 4 moved the batch ID into StreamExecutionContext.
          val context = execution.getClass.getMethod("getLatestExecutionContext").invoke(execution)
          context.getClass.getMethod("batchId").invoke(context)
      }
    value.asInstanceOf[java.lang.Number].longValue()
  }

  private def startOffsetsMethod(execution: AnyRef): String = {
    try {
      // Spark 4 keeps each batch's start offsets in its execution context.
      execution.getClass.getMethod("getStartOffsetsOfLatestBatch")
      "getStartOffsetsOfLatestBatch"
    } catch {
      case _: NoSuchMethodException => "committedOffsets"
    }
  }

  private def executionOffset(
      execution: AnyRef,
      method: String,
      source: PaimonMicroBatchStream): Option[Offset] = {
    execution.getClass
      .getMethod(method)
      .invoke(execution)
      .asInstanceOf[scala.collection.Map[AnyRef, Offset]]
      .get(source)
  }

  // Outer None means invalid or ambiguous lineage; Some(None) means no input partitions.
  private def extractMetadata(batch: Dataset[_]): Option[Option[PaimonMicroBatchMetadata]] = {
    if (!hasExactlyOnePaimonSource(batch)) {
      return None
    }

    val visited = new IdentityHashMap[RDD[_], java.lang.Boolean]()
    var only: PaimonMicroBatchMetadata = null

    def inspectOccurrence(dataSourceRDD: DataSourceRDD): Boolean = {
      var occurrenceOnly: PaimonMicroBatchMetadata = null
      var inputCount = 0
      var valid = true
      val partitions = dataSourceRDD.partitions
      var partitionIndex = 0

      while (valid && partitionIndex < partitions.length) {
        val inputs = dataSourceInputPartitions(partitions(partitionIndex)).iterator
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
        partitionIndex += 1
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

    if (!visit(batch.queryExecution.toRdd)) None else Some(Option(only))
  }

  private def dataSourceInputPartitions(partition: Partition): Seq[InputPartition] = {
    if (partition == null) {
      throw new IllegalArgumentException("Data source RDD partition must not be null.")
    }

    val pluralMethod =
      try {
        Some(partition.getClass.getMethod("inputPartitions"))
      } catch {
        case _: NoSuchMethodException => None
      }

    pluralMethod match {
      case Some(method) => requireInputPartitions(method.invoke(partition))
      case None =>
        Seq(requireInputPartition(partition.getClass.getMethod("inputPartition").invoke(partition)))
    }
  }

  private def requireInputPartitions(value: Any): Seq[InputPartition] =
    value match {
      case null => throw new IllegalArgumentException("Input partitions must not be null.")
      case values: scala.collection.Seq[_] =>
        values.iterator.map(requireInputPartition).toVector
      case other =>
        throw new IllegalArgumentException(
          s"Unexpected input partitions type ${other.getClass.getName}.")
    }

  private def requireInputPartition(value: Any): InputPartition =
    value match {
      case input: InputPartition => input
      case null => throw new IllegalArgumentException("Input partition must not be null.")
      case other =>
        throw new IllegalArgumentException(
          s"Unexpected input partition type ${other.getClass.getName}.")
    }

  private def hasExactlyOnePaimonSource(batch: Dataset[_]): Boolean =
    activePaimonSource(batch).isDefined

  private def activePaimonSource(batch: Dataset[_]): Option[(AnyRef, PaimonMicroBatchStream)] = {
    val queryId = batch.sparkSession.sparkContext.getLocalProperty(StreamingQueryIdKey)
    if (queryId == null) {
      return None
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
      return None
    }

    // Spark replaces sources without new offsets with LocalRelation before foreachBatch. Their
    // RDD lineage therefore contains no InputPartition to inspect. The active StreamExecution is
    // the only per-query structure which still retains every source. Keep this Spark-internal
    // access isolated here and fail closed if a Spark version changes it.
    val sources =
      execution.getClass.getMethod("sources").invoke(execution).asInstanceOf[Seq[AnyRef]]
    sources.headOption.collect {
      case first: PaimonMicroBatchStream if sources.forall(_ eq first) => (execution, first)
    }
  }
}
