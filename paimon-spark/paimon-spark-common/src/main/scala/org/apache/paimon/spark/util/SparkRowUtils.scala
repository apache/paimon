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

package org.apache.paimon.spark.util

import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.spark.SparkRow
import org.apache.paimon.types.{RowKind, RowType}
import org.apache.paimon.utils.UriReaderFactory

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object SparkRowUtils {

  def toPaimonRow(
      writeType: RowType,
      rowkindColIdx: Int,
      catalogContext: CatalogContext): Row => SparkRow = {
    toPaimonRow(writeType, rowkindColIdx, new UriReaderFactory(catalogContext))
  }

  def toPaimonRow(
      writeType: RowType,
      rowkindColIdx: Int,
      uriReaderFactory: UriReaderFactory): Row => SparkRow = {
    if (rowkindColIdx != -1) {
      row =>
        SparkRow.fromUriReaderFactory(
          writeType,
          row,
          RowKind.fromByteValue(row.getByte(rowkindColIdx)),
          uriReaderFactory)
    } else {
      row => SparkRow.fromUriReaderFactory(writeType, row, RowKind.INSERT, uriReaderFactory)
    }
  }

  def getFieldIndex(schema: StructType, colName: String): Int = {
    try {
      schema.fieldIndex(colName)
    } catch {
      case _: IllegalArgumentException =>
        -1
    }
  }
}
