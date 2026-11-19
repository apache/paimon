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
import org.apache.paimon.data.{BinaryString, Decimal, InternalRow, Timestamp}
import org.apache.paimon.globalindex.IndexedSplit
import org.apache.paimon.io.{DataInputDeserializer, DataOutputSerializer}
import org.apache.paimon.memory.MemorySegment
import org.apache.paimon.mergetree.compact.ConcatRecordReader
import org.apache.paimon.reader.ReaderSupplier
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.sort.OrderSorter
import org.apache.paimon.spark.utils.SparkProcedureUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessageSerializer
import org.apache.paimon.types.DataTypeRoot._
import org.apache.paimon.utils.SortUtil

import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
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
    val exchangeType = exchange.exchangeType()
    val schema = StructType(exchangeType.getFields.asScala.map {
      field =>
        val dataType = field.`type`()
        val sparkType = dataType.getTypeRoot match {
          case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE | FLOAT | DOUBLE |
              CHAR | VARCHAR =>
            BinaryType
          case DATE => IntegerType
          case _ => SparkTypeUtils.fromPaimonType(dataType)
        }
        StructField(field.name(), sparkType, dataType.isNullable)
    }.toSeq)
    val sc = spark.sparkContext
    val commitUser = CoreOptions.createCommitUser(table.coreOptions().toConfiguration)
    val rows = sc
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
          val input = ConcatRecordReader.create(readers).toCloseableIterator
          TaskContext.get().addTaskCompletionListener[Unit](_ => input.close())
          val getters = Array.tabulate(exchangeType.getFieldCount) {
            i => InternalRow.createFieldGetter(exchangeType.getTypeAt(i), i)
          }
          input.asScala.map(
            row =>
              Row.fromSeq(getters.iterator.map(g => sparkValue(g.getFieldOrNull(row))).toVector))
      }
    val input = spark.createDataFrame(rows, schema).persist(StorageLevel.DISK_ONLY)
    try {
      val outputs = OrderSorter
        .sortByRange(input, exchange.sortColumns(), parallelism)
        .queryExecution
        .toRdd
        .mapPartitions {
          rows =>
            val writer = exchange.createWriter(commitUser)
            try {
              rows.foreach(
                row => writer.write(row.getBinary(0), row.getBinary(exchange.payloadIndex())))
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
    } finally input.unpersist(false)
  }

  // Preserve Paimon's timestamp precision, signed-zero ordering and binary string comparison.
  private def sparkValue(value: AnyRef): AnyRef = value match {
    case t: Timestamp =>
      val bytes = new Array[Byte](12)
      SortUtil.putTimestampNormalizedKey(t, MemorySegment.wrap(bytes), 0, bytes.length)
      bytes
    case f: java.lang.Float =>
      val bytes = new Array[Byte](4)
      SortUtil.putFloatNormalizedKey(f, MemorySegment.wrap(bytes), 0, bytes.length)
      bytes
    case d: java.lang.Double =>
      val bytes = new Array[Byte](8)
      SortUtil.putDoubleNormalizedKey(d, MemorySegment.wrap(bytes), 0, bytes.length)
      bytes
    case s: BinaryString => s.toBytes
    case d: Decimal => d.toBigDecimal
    case other => other
  }
}
