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

import org.apache.paimon.data.GenericRow
import org.apache.paimon.spark.data.SparkInternalRow
import org.apache.paimon.table.source.Split
import org.apache.paimon.types.{DataField, DataTypes, RowType}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, SupportsReportPartitioning}

import scala.collection.JavaConverters._

trait PaimonInputPartition extends InputPartition {
  def splits: Seq[Split]

  def rowCount(): Long = {
    splits.map(_.rowCount()).sum
  }

  // Used to avoid checking [[PaimonBucketedInputPartition]] to workaround for multi Spark version
  def bucketed = false
}

case class SimplePaimonInputPartition(splits: Seq[Split]) extends PaimonInputPartition
object PaimonInputPartition {
  def apply(split: Split): PaimonInputPartition = {
    SimplePaimonInputPartition(Seq(split))
  }

  def apply(splits: Seq[Split]): PaimonInputPartition = {
    SimplePaimonInputPartition(splits)
  }
}

/** The partition key to report to Spark. */
case class OutputPartitionKey(partitionValue: GenericRow, bucket: Int, partitionType: RowType) {

  // append bucket column after partition column
  private lazy val fullType = {
    val fields = partitionType.getFields.asScala
    RowType.of(
      fields :+ new DataField(
        RowType.currentHighestFieldId(partitionType.getFields) + 1,
        "_bucket",
        DataTypes.INT()): _*)
  }

  private lazy val fullRow = {
    val columns =
      ((0 until partitionValue.getFieldCount).map(p => partitionValue.getField(p)) :+ bucket)
        .map(_.asInstanceOf[Object])
    GenericRow.of(columns: _*)
  }

  lazy val keyRow: SparkInternalRow = {
    SparkInternalRow.create(fullType).replace(fullRow)
  }
}

/** Bucketed input partition should work with [[SupportsReportPartitioning]] together. */
case class PaimonBucketedInputPartition(splits: Seq[Split], key: OutputPartitionKey)
  extends PaimonInputPartition
  with HasPartitionKey {
  override def partitionKey(): InternalRow = key.keyRow
  override def bucketed: Boolean = true
}
