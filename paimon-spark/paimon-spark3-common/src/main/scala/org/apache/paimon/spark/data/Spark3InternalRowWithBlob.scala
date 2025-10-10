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

import org.apache.paimon.types.RowType
import org.apache.paimon.utils.InternalRowUtils.copyInternalRow

import org.apache.spark.sql.catalyst.InternalRow

class Spark3InternalRowWithBlob(rowType: RowType, blobFieldIndex: Int, blobAsDescriptor: Boolean)
  extends Spark3InternalRow(rowType) {

  override def getBinary(ordinal: Int): Array[Byte] = {
    if (ordinal == blobFieldIndex) {
      if (blobAsDescriptor) {
        row.getBlob(ordinal).toDescriptor.serialize()
      } else {
        row.getBlob(ordinal).toData
      }
    } else {
      super.getBinary(ordinal)
    }
  }

  override def copy: InternalRow =
    SparkInternalRow.create(rowType, blobAsDescriptor).replace(copyInternalRow(row, rowType))
}
