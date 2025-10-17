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

import org.apache.paimon.table.FormatTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.read.{SupportsPushDownRequiredColumns, SupportsRuntimeFiltering}
import org.apache.spark.sql.types.StructType

import java.util.{List => JList}

/** ScanBuilder for {@link FormatTable}. */
case class PaimonFormatTableScanBuilder(table: FormatTable)
  extends PaimonBasePushDown
  with SupportsPushDownRequiredColumns {

  override protected var partitionKeys: JList[String] = table.partitionKeys()
  override protected var rowType: RowType = table.rowType()
  protected var requiredSchema: StructType = SparkTypeUtils.fromPaimonRowType(rowType)

  override def build() = PaimonFormatTableScan(table, requiredSchema, pushedPaimonPredicates, None)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
