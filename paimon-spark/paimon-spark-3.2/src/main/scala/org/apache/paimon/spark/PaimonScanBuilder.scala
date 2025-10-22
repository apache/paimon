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

import org.apache.paimon.table.InnerTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.sources.Filter

import java.util.{List => JList}

class PaimonScanBuilder(table: InnerTable)
  extends PaimonBaseScanBuilder(table)
  with PaimonBasePushDown {

  override protected var partitionKeys: JList[String] = table.partitionKeys()
  override protected var rowType: RowType = table.rowType()

  override def build(): Scan = {
    PaimonScan(table, requiredSchema, pushedPaimonPredicates, reservedFilters, None, pushDownTopN)
  }
}
