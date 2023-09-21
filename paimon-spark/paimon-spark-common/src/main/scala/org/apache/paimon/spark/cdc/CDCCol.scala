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

import org.apache.spark.sql.types.{DataType, StringType, StructType}

sealed trait CDCCol {
  def name: String

  def order: Int

  def structType: DataType

  def nullAble: Boolean = false
}

case object RowKind extends CDCCol {
  override def name: String = "_row_kind"

  override def order: Int = 0

  override def structType: DataType = StringType
}

object CDCCol {
  val count: Int = 1

  def fromOrder(order: Int): CDCCol = {
    order match {
      case o if o == RowKind.order => RowKind
      case _ => throw new UnsupportedOperationException(s"Unsupported order $order for CDCSchema.");
    }
  }

  def addCDCCols(schema: StructType): StructType = {
    var newSchema = schema
    for (order <- 0 until count) {
      val col = fromOrder(order)
      newSchema = newSchema.add(col.name, col.structType, col.nullAble)
    }
    newSchema
  }
}
