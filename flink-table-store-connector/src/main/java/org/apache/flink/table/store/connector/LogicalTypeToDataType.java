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

import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Convert {@link LogicalType} to {@link DataType}. */
public class LogicalTypeToDataType extends LogicalTypeDefaultVisitor<DataType> {

    private final AtomicInteger currentHighestFieldId;

    public LogicalTypeToDataType(AtomicInteger currentHighestFieldId) {
        this.currentHighestFieldId = currentHighestFieldId;
    }

    @Override
    public DataType visit(CharType charType) {
        return new org.apache.flink.table.store.types.CharType(
                charType.isNullable(), charType.getLength());
    }

    @Override
    public DataType visit(VarCharType varCharType) {
        return new org.apache.flink.table.store.types.VarCharType(
                varCharType.isNullable(), varCharType.getLength());
    }

    @Override
    public DataType visit(BooleanType booleanType) {
        return new org.apache.flink.table.store.types.BooleanType(booleanType.isNullable());
    }

    @Override
    public DataType visit(BinaryType binaryType) {
        return new org.apache.flink.table.store.types.BinaryType(
                binaryType.isNullable(), binaryType.getLength());
    }

    @Override
    public DataType visit(VarBinaryType varBinaryType) {
        return new org.apache.flink.table.store.types.VarBinaryType(
                varBinaryType.isNullable(), varBinaryType.getLength());
    }

    @Override
    public DataType visit(DecimalType decimalType) {
        return new org.apache.flink.table.store.types.DecimalType(
                decimalType.isNullable(), decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public DataType visit(TinyIntType tinyIntType) {
        return new org.apache.flink.table.store.types.TinyIntType(tinyIntType.isNullable());
    }

    @Override
    public DataType visit(SmallIntType smallIntType) {
        return new org.apache.flink.table.store.types.SmallIntType(smallIntType.isNullable());
    }

    @Override
    public DataType visit(IntType intType) {
        return new org.apache.flink.table.store.types.IntType(intType.isNullable());
    }

    @Override
    public DataType visit(BigIntType bigIntType) {
        return new org.apache.flink.table.store.types.BigIntType(bigIntType.isNullable());
    }

    @Override
    public DataType visit(FloatType floatType) {
        return new org.apache.flink.table.store.types.FloatType(floatType.isNullable());
    }

    @Override
    public DataType visit(DoubleType doubleType) {
        return new org.apache.flink.table.store.types.DoubleType(doubleType.isNullable());
    }

    @Override
    public DataType visit(DateType dateType) {
        return new org.apache.flink.table.store.types.DateType(dateType.isNullable());
    }

    @Override
    public DataType visit(TimeType timeType) {
        return new org.apache.flink.table.store.types.TimeType(
                timeType.isNullable(), timeType.getPrecision());
    }

    @Override
    public DataType visit(TimestampType timestampType) {
        return new org.apache.flink.table.store.types.TimestampType(
                timestampType.isNullable(), timestampType.getPrecision());
    }

    @Override
    public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
        return new org.apache.flink.table.store.types.LocalZonedTimestampType(
                localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new org.apache.flink.table.store.types.ArrayType(
                arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MultisetType multisetType) {
        return new org.apache.flink.table.store.types.MultisetType(
                multisetType.isNullable(), multisetType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MapType mapType) {
        return new org.apache.flink.table.store.types.MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public DataType visit(RowType rowType) {
        List<DataField> dataFields = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            int id = currentHighestFieldId.incrementAndGet();
            DataType fieldType = field.getType().accept(this);
            dataFields.add(
                    new DataField(
                            id, field.getName(), fieldType, field.getDescription().orElse(null)));
        }

        return new org.apache.flink.table.store.types.RowType(rowType.isNullable(), dataFields);
    }

    @Override
    protected DataType defaultMethod(LogicalType logicalType) {
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
}
