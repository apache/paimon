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

import org.apache.paimon.table.format.FormatDataSplit
import org.apache.paimon.types.RowType

import org.apache.spark.sql.connector.read.Statistics

import java.util.OptionalLong

import scala.collection.JavaConverters._

case class FormatTableStatistics[T <: PaimonFormatTableBaseScan](scan: T) extends Statistics {

  private lazy val fileTotalSize: Long =
    scan.getOriginSplits.map(_.asInstanceOf[FormatDataSplit]).map(_.length()).sum

  override def sizeInBytes(): OptionalLong = {
    val size = fileTotalSize /
      estimateRowSize(scan.tableRowType) *
      estimateRowSize(scan.readTableRowType)
    OptionalLong.of(size)
  }

  private def estimateRowSize(rowType: RowType): Long = {
    rowType.getFields.asScala.map(_.`type`().defaultSize().toLong).sum
  }

  override def numRows(): OptionalLong = OptionalLong.empty()
}
