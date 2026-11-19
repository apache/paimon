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

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.append.dataevolution.{DataEvolutionCompactTask, DataEvolutionRangeShuffle, DataEvolutionRangeSortTask}
import org.apache.paimon.data.{BinaryRow, InternalRow}
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.globalindex.IndexedSplit
import org.apache.paimon.io.{DataInputDeserializer, DataOutputSerializer}
import org.apache.paimon.memory.MemorySegment
import org.apache.paimon.mergetree.compact.ConcatRecordReader
import org.apache.paimon.reader.ReaderSupplier
import org.apache.paimon.spark.utils.SparkProcedureUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessageSerializer
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{KeyComparatorSupplier, ProjectedRow}

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.util

import scala.collection.JavaConverters._

object SparkDataEvolutionRangeSort {

  def execute(
      table: FileStoreTable,
      snapshot: Snapshot,
      fields: util.List[String],
      tasks: util.List[DataEvolutionCompactTask],
      spark: SparkSession): Unit = {
    val parallelism = spark.sessionState.conf.numShufflePartitions
    if (parallelism == 1) {
      DataEvolutionRewriteExecutor.execute(
        table,
        snapshot,
        _ => new DataEvolutionRewriteExecutor.PlanResult(tasks, true),
        JavaSparkContext.fromSparkContext(spark.sparkContext),
        spark,
        DataEvolutionRewriteExecutor.RANGE_SORT_COMMIT
      )
      return
    }
    val planned = tasks.asScala.map(_.asInstanceOf[DataEvolutionRangeSortTask])
    val splits = planned
      .flatMap(_.readSplits(table).asScala)
      .map {
        split =>
          val out = new DataOutputSerializer(256)
          split.serialize(out)
          out.getCopyOfBuffer
      }
      .toVector
    val exchange = new DataEvolutionRangeShuffle(table, fields)
    val keyType = exchange.keyType()
    implicit val ordering: Ordering[Array[Byte]] = new KeyOrdering(keyType)
    val sc = spark.sparkContext
    val commitUser = CoreOptions.createCommitUser(table.coreOptions().toConfiguration)
    val keyed = sc
      .parallelize(splits, SparkProcedureUtils.readParallelism(splits.asJava, spark))
      .mapPartitions {
        inputs =>
          val readers = inputs
            .map(
              split =>
                new ReaderSupplier[InternalRow] {
                  override def get() =
                    exchange.read(IndexedSplit.deserialize(new DataInputDeserializer(split)))
                })
            .toList
            .asJava
          val rows = ConcatRecordReader.create(readers).toCloseableIterator
          TaskContext.get().addTaskCompletionListener[Unit](_ => rows.close())
          val project = ProjectedRow.from(keyType, exchange.exchangeType())
          val serializer = new InternalRowSerializer(keyType)
          rows.asScala.map {
            row =>
              (
                serializer.toBinaryRow(project.replaceRow(row)).copy().toBytes,
                row.getBinary(exchange.payloadIndex()))
          }
      }
      .persist(StorageLevel.DISK_ONLY)
    try {
      // RangePartitioner samples keys, then the Spark shuffle sorts each reducer's run.
      val outputs = keyed
        .sortByKey(ascending = true, numPartitions = parallelism)
        .mapPartitions {
          rows =>
            val writer = exchange.createWriter(commitUser)
            try {
              val key = new BinaryRow(keyType.getFieldCount)
              rows.foreach {
                case (bytes, payload) =>
                  key.pointTo(MemorySegment.wrap(bytes), 0, bytes.length)
                  writer.write(key.getBinary(0), payload)
              }
              CommitMessageSerializer.serializeAll(writer.finish()).asScala.toVector.iterator
            } finally writer.close()
        }
        .collect()
        .toList
        .asJava
      val inputs = planned.map(_.inputFiles()).toList.asJava
      DataEvolutionRewriteExecutor.commitSorted(
        table,
        snapshot,
        DataEvolutionRangeShuffle.replacements(
          inputs,
          CommitMessageSerializer.deserializeAll(outputs)),
        commitUser,
        spark)
    } finally keyed.unpersist(false)
  }

  private class KeyOrdering(keyType: RowType) extends Ordering[Array[Byte]] with Serializable {
    @transient private lazy val comparator = new KeyComparatorSupplier(keyType).get()
    @transient private lazy val leftRow = new BinaryRow(keyType.getFieldCount)
    @transient private lazy val rightRow = new BinaryRow(keyType.getFieldCount)

    override def compare(left: Array[Byte], right: Array[Byte]): Int = {
      leftRow.pointTo(MemorySegment.wrap(left), 0, left.length)
      rightRow.pointTo(MemorySegment.wrap(right), 0, right.length)
      comparator.compare(leftRow, rightRow)
    }
  }
}
