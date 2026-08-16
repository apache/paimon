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

package org.apache.paimon.spark.read

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.spark.PaimonBaseScanBuilder
import org.apache.paimon.table.`object`.ObjectTable
import org.apache.paimon.table.source.Split

import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class ObjectTableScanBuilder(val table: ObjectTable) extends PaimonBaseScanBuilder {

  override def build(): ObjectTableScan =
    ObjectTableScan(table, requiredSchema)
}

/** Scan implementation for [[ObjectTable]] */
case class ObjectTableScan(table: ObjectTable, requiredSchema: StructType) extends BaseScan {

  override val pushedPartitionFilters: Seq[PartitionPredicate] = Nil
  override val pushedDataFilters: Seq[Predicate] = Nil
  override val pushedLimit: Option[Int] = None

  protected def getInputSplits: Array[Split] = {
    readBuilder
      .newScan()
      .plan()
      .splits()
      .asScala
      .toArray
  }
}
