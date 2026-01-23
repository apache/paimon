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

import scala.collection.mutable

abstract class SparkInternalRow extends InternalRow {
  def replace(row: org.apache.paimon.data.InternalRow): SparkInternalRow
}

object SparkInternalRow {

  def create(rowType: RowType): SparkInternalRow = {
    create(rowType, blobAsDescriptor = false)
  }

  def create(rowType: RowType, blobAsDescriptor: Boolean): SparkInternalRow = {
    val blobs = blobFields(rowType)
    if (blobs.nonEmpty) {
      SparkShimLoader.shim.createSparkInternalRowWithBlob(rowType, blobs, blobAsDescriptor)
    } else {
      SparkShimLoader.shim.createSparkInternalRow(rowType)
    }
  }

  private def blobFields(rowType: RowType): Set[Int] = {
    var i: Int = 0
    val blobFields = new mutable.HashSet[Int]()
    while (i < rowType.getFieldCount) {
      if (rowType.getTypeAt(i).getTypeRoot.equals(DataTypeRoot.BLOB)) {
        blobFields.add(i)
      }
      i += 1
    }
    blobFields.toSet
  }

}
