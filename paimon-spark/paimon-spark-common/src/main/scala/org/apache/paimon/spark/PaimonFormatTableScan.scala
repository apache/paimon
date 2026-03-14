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

import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.spark.read.{BaseScan, PaimonSupportsRuntimeFiltering}
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.source.Split

import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/** Scan implementation for [[FormatTable]] */
case class PaimonFormatTableScan(
    table: FormatTable,
    requiredSchema: StructType,
    pushedPartitionFilters: Seq[PartitionPredicate],
    pushedDataFilters: Seq[Predicate],
    override val pushedLimit: Option[Int])
  extends BaseScan
  with PaimonSupportsRuntimeFiltering {

  protected def getInputSplits: Array[Split] = {
    readBuilder
      .newScan()
      .plan()
      .splits()
      .asScala
      .toArray
  }
}
