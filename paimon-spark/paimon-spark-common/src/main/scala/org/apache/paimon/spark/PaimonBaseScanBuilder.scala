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

import org.apache.paimon.predicate.{Predicate, TopN}
import org.apache.paimon.table.InnerTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

abstract class PaimonBaseScanBuilder(table: InnerTable)
  extends ScanBuilder
  with SupportsPushDownRequiredColumns
  with Logging {

  protected var requiredSchema: StructType = SparkTypeUtils.fromPaimonRowType(table.rowType())

  protected var pushedPaimonPredicates: Array[Predicate] = Array.empty

  protected var reservedFilters: Array[Filter] = Array.empty

  protected var hasPostScanPredicates = false

  protected var pushDownLimit: Option[Int] = None

  protected var pushDownTopN: Option[TopN] = None

  override def build(): Scan = {
    PaimonScan(
      table,
      requiredSchema,
      pushedPaimonPredicates,
      reservedFilters,
      pushDownLimit,
      pushDownTopN)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
