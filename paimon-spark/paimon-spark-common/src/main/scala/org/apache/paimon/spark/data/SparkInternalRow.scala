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

import org.apache.paimon.data.{Decimal => PaimonDecimal, InternalRow => PaimonInternalRow, _}
import org.apache.paimon.spark.DataConverter
import org.apache.paimon.spark.util.shim.TypeUtils
import org.apache.paimon.types.{ArrayType => PaimonArrayType, BigIntType, DataType => PaimonDataType, DataTypeChecks, IntType, MapType => PaimonMapType, MultisetType, RowType}
import org.apache.paimon.types.DataTypeRoot._
import org.apache.paimon.utils.InternalRowUtils.copyInternalRow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.util.Objects

abstract class SparkInternalRow extends InternalRow {
  def replace(row: org.apache.paimon.data.InternalRow): SparkInternalRow
}

//abstract class AbstractSparkInternalRow extends SparkInternalRow {
//
//  protected val rowType: RowType
//  protected var row: PaimonInternalRow = _
//
//  override def replace(row: PaimonInternalRow): SparkInternalRow = {
//    this.row = row
//    this
//  }
//
//  override def numFields: Int = row.getFieldCount
//
//  override def setNullAt(ordinal: Int): Unit = throw new UnsupportedOperationException()
//
//  override def update(ordinal: Int, value: Any): Unit = throw new UnsupportedOperationException()
//
//  override def copy(): InternalRow = {
//    SparkInternalRow.create(rowType).replace(copyInternalRow(row, rowType))
//  }
//
//  override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)
//
//  override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)
//
//  override def getByte(ordinal: Int): Byte = row.getByte(ordinal)
//
//  override def getShort(ordinal: Int): Short = row.getShort(ordinal)
//
//  override def getInt(ordinal: Int): Int = row.getInt(ordinal)
//
//  override def getLong(ordinal: Int): Long = rowType.getTypeAt(ordinal) match {
//    case _: BigIntType =>
//      row.getLong(ordinal)
//    case _ =>
//      DataConverter.fromPaimon(
//        row.getTimestamp(ordinal, DataTypeChecks.getPrecision(rowType.getTypeAt(ordinal))))
//  }
//
//  override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)
//
//  override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)
//
//  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
//    DataConverter.fromPaimon(row.getDecimal(ordinal, precision, scale))
//
//  override def getUTF8String(ordinal: Int): UTF8String =
//    DataConverter.fromPaimon(row.getString(ordinal))
//
//  override def getBinary(ordinal: Int): Array[Byte] = row.getBinary(ordinal)
//
//  override def getInterval(ordinal: Int): CalendarInterval =
//    throw new UnsupportedOperationException()
//
//  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
//    DataConverter.fromPaimon(
//      row.getRow(ordinal, numFields),
//      rowType.getTypeAt(ordinal).asInstanceOf[RowType])
//  }
//
//  override def getArray(ordinal: Int): ArrayData = {
//    DataConverter.fromPaimon(
//      row.getArray(ordinal),
//      rowType.getTypeAt(ordinal).asInstanceOf[PaimonArrayType])
//  }
//
//  override def getMap(ordinal: Int): MapData = {
//    DataConverter.fromPaimon(row.getMap(ordinal), rowType.getTypeAt(ordinal))
//  }
//
//  override def get(ordinal: Int, dataType: DataType): AnyRef = {
//    if (isNullAt(ordinal) || dataType.isInstanceOf[NullType]) {
//      null
//    } else {
//      dataType match {
//        case _: BooleanType => getBinary(ordinal)
//        case _: ByteType => getByte(ordinal)
//        case _: ShortType => getShort(ordinal)
//        case _: IntegerType => getInt(ordinal)
//        case _: LongType => getLong(ordinal)
//        case _: FloatType => getFloat(ordinal)
//        case _: DoubleType => getDouble(ordinal)
//        case _: StringType | _: CharType | _: VarcharType => getUTF8String(ordinal)
//        case dt: DecimalType => getDecimal(ordinal, dt.precision, dt.scale)
//        case _: DateType => getInt(ordinal)
//        case _: TimestampType => getLong(ordinal)
//        case _: CalendarIntervalType => getInterval(ordinal)
//        case _: BinaryType => getBinary(ordinal)
//        case st: StructType => getStruct(ordinal, st.size)
//        case _: ArrayType => getArray(ordinal)
//        case _: MapType => getMap(ordinal)
//        case _: UserDefinedType[_] =>
//          get(ordinal, dataType.asInstanceOf[UserDefinedType[_]].sqlType)
//      }
//    }
//  }
//
//  private def getAs[T](ordinal: Int, dataType: DataType): T = {
//    if (isNullAt(ordinal) || dataType.isInstanceOf[NullType]) {
//      null
//    } else {
//      dataType match {
//        case _: BooleanType => row.getBinary(ordinal).
//        case _: ByteType => getByte(ordinal)
//        case _: ShortType => getShort(ordinal)
//        case _: IntegerType => getInt(ordinal)
//        case _: LongType => getLong(ordinal)
//        case _: FloatType => getFloat(ordinal)
//        case _: DoubleType => getDouble(ordinal)
//        case _: StringType | _: CharType | _: VarcharType => getUTF8String(ordinal)
//        case dt: DecimalType => getDecimal(ordinal, dt.precision, dt.scale)
//        case _: DateType => getInt(ordinal)
//        case _: TimestampType => getLong(ordinal)
//        case _: CalendarIntervalType => getInterval(ordinal)
//        case _: BinaryType => getBinary(ordinal)
//        case st: StructType => getStruct(ordinal, st.size)
//        case _: ArrayType => getArray(ordinal)
//        case _: MapType => getMap(ordinal)
//        case _: UserDefinedType[_] =>
//          get(ordinal, dataType.asInstanceOf[UserDefinedType[_]].sqlType)
//      }
//    }
//  }
//
//  override def equals(obj: Any): Boolean = {
//    if (this == obj) {
//      return true
//    }
//    if (obj == null || getClass != obj.getClass) {
//      return false
//    }
//    val that = obj.asInstanceOf[AbstractSparkInternalRow]
//    Objects.equals(rowType, that.rowType) && Objects.equals(row, that.row)
//  }
//
//  override def hashCode(): Int = Objects.hash(rowType, row)
//}

object SparkInternalRow {

  def create(rowType: RowType): SparkInternalRow = {
    SparkShimLoader.getSparkShim.createSparkInternalRow(rowType)
  }

//  def fromPaimon(o: Any, dataType: PaimonDataType): Any = {
//    if (o == null) {
//      return null
//    }
//    dataType.getTypeRoot match {
//      case TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
//        fromPaimon(o.asInstanceOf[Timestamp])
//      case CHAR | VARCHAR =>
//        fromPaimon(o.asInstanceOf[BinaryString])
//      case DECIMAL =>
//        fromPaimon(o.asInstanceOf[PaimonDecimal])
//      case ARRAY =>
//        fromPaimonArray(o.asInstanceOf[InternalArray], dataType.asInstanceOf[PaimonArrayType])
//      case MAP | MULTISET =>
//        fromPaimonMap(o.asInstanceOf[InternalMap], dataType)
//      case ROW =>
//        fromPaimonRow(o.asInstanceOf[PaimonInternalRow], dataType.asInstanceOf[RowType])
//    }
//  }
//
//  def fromPaimon(string: BinaryString): UTF8String = {
//    UTF8String.fromBytes(string.toBytes)
//  }
//
//  def fromPaimon(decimal: PaimonDecimal): Decimal = {
//    Decimal.apply(decimal.toBigDecimal)
//  }
//
//  def fromPaimonRow(row: PaimonInternalRow, rowType: RowType): InternalRow = {
//    create(rowType).replace(row)
//  }
//
//  def fromPaimon(timestamp: Timestamp): Long = {
//    if (TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType()) {
//      DateTimeUtils.fromJavaTimestamp(timestamp.toSQLTimestamp)
//    } else {
//      timestamp.toMicros
//    }
//  }
//
//  def fromPaimonArray(array: InternalArray, arrayType: PaimonArrayType): ArrayData = {
//    fromPaimonArrayElementType(array, arrayType.getElementType)
//  }
//
//  def fromPaimonMap(map: InternalMap, mapType: PaimonDataType): MapData = {
//    val (keyType, valueType) = mapType match {
//      case mt: PaimonMapType => (mt.getKeyType, mt.getValueType)
//      case mst: MultisetType => (mst.getElementType, new IntType())
//      case _ => throw new UnsupportedOperationException("Unsupported type: " + mapType)
//    }
//    new ArrayBasedMapData(
//      fromPaimonArrayElementType(map.keyArray(), keyType),
//      fromPaimonArrayElementType(map.valueArray(), valueType))
//  }
//
//  private def fromPaimonArrayElementType(
//      array: InternalArray,
//      elementType: PaimonDataType): ArrayData = {
//    SparkArrayData.create(elementType).replace(array);
//  }

}
