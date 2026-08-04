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
import org.apache.paimon.table.source.WrittenColumns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.{IdentityHashMap, Optional}

import scala.collection.mutable
import scala.util.control.NonFatal

/** Driver-side access to metadata planned for a Paimon streaming micro-batch. */
@Experimental
final class PaimonSparkMicroBatchMetadata private ()

object PaimonSparkMicroBatchMetadata {

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
}
