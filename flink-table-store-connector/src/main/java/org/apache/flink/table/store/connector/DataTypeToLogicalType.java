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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.BinaryType;
import org.apache.flink.table.store.types.BooleanType;
import org.apache.flink.table.store.types.CharType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypeVisitor;
import org.apache.flink.table.store.types.DateType;
import org.apache.flink.table.store.types.DecimalType;
import org.apache.flink.table.store.types.DoubleType;
import org.apache.flink.table.store.types.FloatType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.LocalZonedTimestampType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.MultisetType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.SmallIntType;
import org.apache.flink.table.store.types.TimeType;
import org.apache.flink.table.store.types.TimestampType;
import org.apache.flink.table.store.types.TinyIntType;
import org.apache.flink.table.store.types.VarBinaryType;
import org.apache.flink.table.store.types.VarCharType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

/** Convert {@link DataType} to {@link LogicalType}. */
public class DataTypeToLogicalType implements DataTypeVisitor<LogicalType> {

    public static final DataTypeToLogicalType INSTANCE = new DataTypeToLogicalType();

    @Override
    public LogicalType visit(CharType charType) {
        return new org.apache.flink.table.types.logical.CharType(
                charType.isNullable(), charType.getLength());
    }

    @Override
    public LogicalType visit(VarCharType varCharType) {
        return new org.apache.flink.table.types.logical.VarCharType(
                varCharType.isNullable(), varCharType.getLength());
    }

    @Override
    public LogicalType visit(BooleanType booleanType) {
        return new org.apache.flink.table.types.logical.BooleanType(booleanType.isNullable());
    }

    @Override
    public LogicalType visit(BinaryType binaryType) {
        return new org.apache.flink.table.types.logical.BinaryType(
                binaryType.isNullable(), binaryType.getLength());
    }

    @Override
    public LogicalType visit(VarBinaryType varBinaryType) {
        return new org.apache.flink.table.types.logical.VarBinaryType(
                varBinaryType.isNullable(), varBinaryType.getLength());
    }

    @Override
    public LogicalType visit(DecimalType decimalType) {
        return new org.apache.flink.table.types.logical.DecimalType(
                decimalType.isNullable(), decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public LogicalType visit(TinyIntType tinyIntType) {
        return new org.apache.flink.table.types.logical.TinyIntType(tinyIntType.isNullable());
    }

    @Override
    public LogicalType visit(SmallIntType smallIntType) {
        return new org.apache.flink.table.types.logical.SmallIntType(smallIntType.isNullable());
    }

    @Override
    public LogicalType visit(IntType intType) {
        return new org.apache.flink.table.types.logical.IntType(intType.isNullable());
    }

    @Override
    public LogicalType visit(BigIntType bigIntType) {
        return new org.apache.flink.table.types.logical.BigIntType(bigIntType.isNullable());
    }

    @Override
    public LogicalType visit(FloatType floatType) {
        return new org.apache.flink.table.types.logical.FloatType(floatType.isNullable());
    }

    @Override
    public LogicalType visit(DoubleType doubleType) {
        return new org.apache.flink.table.types.logical.DoubleType(doubleType.isNullable());
    }

    @Override
    public LogicalType visit(DateType dateType) {
        return new org.apache.flink.table.types.logical.DateType(dateType.isNullable());
    }

    @Override
    public LogicalType visit(TimeType timeType) {
        return new org.apache.flink.table.types.logical.TimeType(
                timeType.isNullable(), timeType.getPrecision());
    }

    @Override
    public LogicalType visit(TimestampType timestampType) {
        return new org.apache.flink.table.types.logical.TimestampType(
                timestampType.isNullable(), timestampType.getPrecision());
    }

    @Override
    public LogicalType visit(LocalZonedTimestampType localZonedTimestampType) {
        return new org.apache.flink.table.types.logical.LocalZonedTimestampType(
                localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
    }

    @Override
    public LogicalType visit(ArrayType arrayType) {
        return new org.apache.flink.table.types.logical.ArrayType(
                arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public LogicalType visit(MultisetType multisetType) {
        return new org.apache.flink.table.types.logical.MultisetType(
                multisetType.isNullable(), multisetType.getElementType().accept(this));
    }

    @Override
    public LogicalType visit(MapType mapType) {
        return new org.apache.flink.table.types.logical.MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public LogicalType visit(RowType rowType) {
        List<org.apache.flink.table.types.logical.RowType.RowField> dataFields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            LogicalType fieldType = field.type().accept(this);
            dataFields.add(
                    new org.apache.flink.table.types.logical.RowType.RowField(
                            field.name(), fieldType, field.description()));
        }

        return new org.apache.flink.table.types.logical.RowType(rowType.isNullable(), dataFields);
    }
}
