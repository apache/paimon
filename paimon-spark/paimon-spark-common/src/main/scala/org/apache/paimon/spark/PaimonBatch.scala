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

import org.apache.paimon.table.source.{ReadBuilder, Split}

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

import java.util.Objects

/** A Spark [[Batch]] for paimon. */
case class PaimonBatch(splits: Array[Split], readBuilder: ReadBuilder) extends Batch {

  override def planInputPartitions(): Array[InputPartition] =
    splits.map(new SparkInputPartition(_).asInstanceOf[InputPartition])

  override def createReaderFactory(): PartitionReaderFactory = new PaimonPartitionReaderFactory(
    readBuilder)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: PaimonBatch =>
        this.splits.sameElements(other.splits) &&
        readBuilder.equals(other.readBuilder)

      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(splits.toSeq, readBuilder)
  }
}
