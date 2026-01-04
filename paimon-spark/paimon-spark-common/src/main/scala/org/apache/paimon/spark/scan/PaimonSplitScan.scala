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

package org.apache.paimon.spark.scan

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.spark.{PaimonBaseScanBuilder, PaimonBatch}
import org.apache.paimon.table.{InnerTable, KnownSplitsTable}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class PaimonSplitScanBuilder(val table: KnownSplitsTable) extends PaimonBaseScanBuilder {

  override def build(): Scan = {
    PaimonSplitScan(
      table,
      table.splits(),
      requiredSchema,
      pushedPartitionFilters,
      pushedDataFilters)
  }
}

/** For internal use only. */
case class PaimonSplitScan(
    table: InnerTable,
    dataSplits: Array[DataSplit],
    requiredSchema: StructType,
    pushedPartitionFilters: Seq[PartitionPredicate],
    pushedDataFilters: Seq[Predicate])
  extends BaseScan {

  override def inputSplits: Array[Split] = dataSplits.asInstanceOf[Array[Split]]
}
