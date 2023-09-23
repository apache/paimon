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
package org.apache.paimon.spark.cdc

import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/** RowKind col, see more in [[org.apache.paimon.types.RowType]] */
case object RowKindCol {
  val name: String = "_row_kind"
  val structType: DataType = StringType
  val nullAble: Boolean = false
  val order: Int = 0
}

object CDCCol {

  /** Add CDC cols in row for changelog reading. */
  def addCDCCols(schema: StructType): StructType = {
    schema.add(StructField(RowKindCol.name, RowKindCol.structType, RowKindCol.nullAble))
  }

}
