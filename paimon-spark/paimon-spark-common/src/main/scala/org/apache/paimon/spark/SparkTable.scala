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

import org.apache.paimon.spark.rowops.PaimonSparkCopyOnWriteOperation
import org.apache.paimon.table.{FileStoreTable, Table}

import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.{RowLevelOperationBuilder, RowLevelOperationInfo}

/** A spark [[org.apache.spark.sql.connector.catalog.Table]] for paimon. */
case class SparkTable(override val table: Table)
  extends PaimonSparkTableBase(table)
  with SupportsRowLevelOperations {

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    table match {
      case t: FileStoreTable if useV2Write =>
        () => new PaimonSparkCopyOnWriteOperation(t, info)
      case _ =>
        throw new UnsupportedOperationException(
          s"Write operation is only supported for FileStoreTable with V2 write enabled. " +
            s"Actual table type: ${table.getClass.getSimpleName}, useV2Write: $useV2Write")
    }
  }
}

case class SparkIcebergTable(table: Table) extends BaseTable

case class SparkLanceTable(table: Table) extends BaseTable

case class SparkObjectTable(table: Table) extends BaseTable
