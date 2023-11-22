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

package org.apache.migrate.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.math.BigDecimal;

/** Generate different converter to write data. */
public class DataTypeWriter implements DataTypeVisitor<DataConverter> {

    @Override
    public DataConverter visit(CharType charType) {
        return (writer, pos, value) -> writer.writeString(pos, BinaryString.fromString(value));
    }

    @Override
    public DataConverter visit(VarCharType varCharType) {
        return (writer, pos, value) -> writer.writeString(pos, BinaryString.fromString(value));
    }

    @Override
    public DataConverter visit(BooleanType booleanType) {
        return (writer, pos, value) -> writer.writeBoolean(pos, Boolean.parseBoolean(value));
    }

    @Override
    public DataConverter visit(BinaryType binaryType) {
        return (writer, pos, value) -> writer.writeBinary(pos, value.getBytes());
    }

    @Override
    public DataConverter visit(VarBinaryType varBinaryType) {
        return (writer, pos, value) -> writer.writeBinary(pos, value.getBytes());
    }

    @Override
    public DataConverter visit(DecimalType decimalType) {
        return (writer, pos, value) ->
                writer.writeDecimal(
                        pos,
                        Decimal.fromBigDecimal(
                                new BigDecimal(value),
                                decimalType.getPrecision(),
                                decimalType.getScale()),
                        decimalType.getPrecision());
    }

    @Override
    public DataConverter visit(TinyIntType tinyIntType) {
        return (writer, pos, value) -> writer.writeByte(pos, Byte.parseByte(value));
    }

    @Override
    public DataConverter visit(SmallIntType smallIntType) {
        return (writer, pos, value) -> writer.writeShort(pos, Short.parseShort(value));
    }

    @Override
    public DataConverter visit(IntType intType) {
        return (writer, pos, value) -> writer.writeInt(pos, Integer.parseInt(value));
    }

    @Override
    public DataConverter visit(BigIntType bigIntType) {
        return (writer, pos, value) -> writer.writeLong(pos, Long.parseLong(value));
    }

    @Override
    public DataConverter visit(FloatType floatType) {
        return (writer, pos, value) -> writer.writeFloat(pos, Float.parseFloat(value));
    }

    @Override
    public DataConverter visit(DoubleType doubleType) {
        return (writer, pos, value) -> writer.writeDouble(pos, Double.parseDouble(value));
    }

    @Override
    public DataConverter visit(DateType dateType) {
        return (writer, pos, value) -> writer.writeInt(pos, Integer.parseInt(value));
    }

    @Override
    public DataConverter visit(TimeType timeType) {
        return (writer, pos, value) -> writer.writeInt(pos, Integer.parseInt(value));
    }

    @Override
    public DataConverter visit(TimestampType timestampType) {
        return (writer, pos, value) ->
                writer.writeTimestamp(
                        pos,
                        Timestamp.fromEpochMillis(Long.parseLong(value)),
                        timestampType.getPrecision());
    }

    @Override
    public DataConverter visit(LocalZonedTimestampType localZonedTimestampType) {
        return (writer, pos, value) ->
                writer.writeTimestamp(
                        pos,
                        Timestamp.fromEpochMillis(Long.parseLong(value)),
                        localZonedTimestampType.getPrecision());
    }

    @Override
    public DataConverter visit(ArrayType arrayType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataConverter visit(MultisetType multisetType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataConverter visit(MapType mapType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataConverter visit(RowType rowType) {
        throw new UnsupportedOperationException();
    }
}
