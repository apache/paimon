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

package org.apache.paimon.spark.data

import org.apache.paimon.types.{DataTypeRoot, RowType}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import java.util.OptionalInt

abstract class SparkInternalRow extends InternalRow {
  def replace(row: org.apache.paimon.data.InternalRow): SparkInternalRow
}

object SparkInternalRow {

  def create(rowType: RowType): SparkInternalRow = {
    create(rowType, blobAsDescriptor = false)
  }

  def create(rowType: RowType, blobAsDescriptor: Boolean): SparkInternalRow = {
    val fieldIndex = blobFieldIndex(rowType)
    if (fieldIndex.isPresent) {
      SparkShimLoader.shim.createSparkInternalRowWithBlob(
        rowType,
        fieldIndex.getAsInt,
        blobAsDescriptor)
    } else {
      SparkShimLoader.shim.createSparkInternalRow(rowType)
    }
  }

  private def blobFieldIndex(rowType: RowType): OptionalInt = {
    var i: Int = 0
    while (i < rowType.getFieldCount) {
      if (rowType.getTypeAt(i).getTypeRoot.equals(DataTypeRoot.BLOB)) {
        return OptionalInt.of(i)
      }
      i += 1
    }
    OptionalInt.empty()
  }

}
