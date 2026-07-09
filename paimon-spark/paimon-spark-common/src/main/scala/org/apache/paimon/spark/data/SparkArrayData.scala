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

import org.apache.paimon.data.{Blob, BlobView, InternalArray}
import org.apache.paimon.spark.DataConverter
import org.apache.paimon.types.{ArrayType => PaimonArrayType, BigIntType, BlobType, DataType => PaimonDataType, DataTypeChecks, RowType}
import org.apache.paimon.utils.InternalRowUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

abstract class SparkArrayData extends org.apache.spark.sql.catalyst.util.ArrayData {

  def replace(array: InternalArray): SparkArrayData

  def withBlobAsDescriptor(blobAsDescriptor: Boolean): SparkArrayData
}

abstract class AbstractSparkArrayData extends SparkArrayData {

  val elementType: PaimonDataType

  var paimonArray: InternalArray = _

  protected var blobAsDescriptor: Boolean = false

  override def replace(array: InternalArray): SparkArrayData = {
    this.paimonArray = array
    this
  }

  override def withBlobAsDescriptor(blobAsDescriptor: Boolean): SparkArrayData = {
    this.blobAsDescriptor = blobAsDescriptor
    this
  }

  override def numElements(): Int = paimonArray.size()

  override def copy(): ArrayData = {
    SparkArrayData
      .create(elementType, blobAsDescriptor)
      .replace(InternalRowUtils.copyArray(paimonArray, elementType))
  }

  override def array: Array[Any] = {
    Array.range(0, numElements()).map {
      i =>
        if (isNullAt(i)) {
          null
        } else {
          elementType match {
            case _: BlobType => fromBlob(paimonArray.getBlob(i))
            case _ =>
              DataConverter
                .fromPaimon(InternalRowUtils.get(paimonArray, i, elementType), elementType)
          }
        }
    }
  }

  override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def isNullAt(ordinal: Int): Boolean = paimonArray.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean = paimonArray.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = paimonArray.getByte(ordinal)

  override def getShort(ordinal: Int): Short = paimonArray.getShort(ordinal)

  override def getInt(ordinal: Int): Int = paimonArray.getInt(ordinal)

  override def getLong(ordinal: Int): Long = elementType match {
    case _: BigIntType => paimonArray.getLong(ordinal)
    case _ =>
      DataConverter.fromPaimon(
        paimonArray.getTimestamp(ordinal, DataTypeChecks.getPrecision(elementType)))
  }

  override def getFloat(ordinal: Int): Float = paimonArray.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = paimonArray.getDouble(ordinal)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    DataConverter.fromPaimon(paimonArray.getDecimal(ordinal, precision, scale))

  override def getUTF8String(ordinal: Int): UTF8String =
    DataConverter.fromPaimon(paimonArray.getString(ordinal))

  override def getBinary(ordinal: Int): Array[Byte] = elementType match {
    case _: BlobType =>
      if (paimonArray.isNullAt(ordinal)) {
        null
      } else {
        fromBlob(paimonArray.getBlob(ordinal))
      }
    case _ => paimonArray.getBinary(ordinal)
  }

  private def fromBlob(blob: Blob): Array[Byte] = {
    if (blob == null) {
      null
    } else {
      blob match {
        case blobView: BlobView if !blobView.isResolved =>
          Blob.serializeBlob(blobView)
        case _ if blobAsDescriptor =>
          blob.toDescriptor.serialize()
        case _ =>
          blob.toData
      }
    }
  }

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException()

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = DataConverter
    .fromPaimon(paimonArray.getRow(ordinal, numFields), elementType.asInstanceOf[RowType])

  override def getArray(ordinal: Int): ArrayData = DataConverter.fromPaimon(
    paimonArray.getArray(ordinal),
    elementType.asInstanceOf[PaimonArrayType],
    blobAsDescriptor)

  override def getMap(ordinal: Int): MapData =
    DataConverter.fromPaimon(paimonArray.getMap(ordinal), elementType)

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    SpecializedGettersReader.read(this, ordinal, dataType, true, true)

}

object SparkArrayData {
  def create(elementType: PaimonDataType): SparkArrayData = {
    create(elementType, blobAsDescriptor = false)
  }

  def create(elementType: PaimonDataType, blobAsDescriptor: Boolean): SparkArrayData = {
    SparkShimLoader.shim.createSparkArrayData(elementType).withBlobAsDescriptor(blobAsDescriptor)
  }
}
