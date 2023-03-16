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

package org.apache.flink.table.store.presto;

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.Decimal;
import org.apache.flink.table.store.data.InternalArray;
import org.apache.flink.table.store.data.InternalMap;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.Timestamp;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypeChecks;
import org.apache.flink.table.store.utils.RowDataUtils;

import org.apache.flink.shaded.guava30.com.google.common.base.Verify;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeShortScaledValue;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;

/** Presto {@link ConnectorPageSource}. */
public class PrestoPageSource implements ConnectorPageSource {

    private final RecordReader<InternalRow> reader;
    private final PageBuilder pageBuilder;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    private boolean isFinished = false;

    public PrestoPageSource(RecordReader<InternalRow> reader, List<ColumnHandle> projectedColumns) {
        this.reader = reader;
        this.columnTypes = new ArrayList<>();
        this.logicalTypes = new ArrayList<>();
        for (ColumnHandle handle : projectedColumns) {
            PrestoColumnHandle prestoColumnHandle = (PrestoColumnHandle) handle;
            columnTypes.add(prestoColumnHandle.getPrestoType());
            logicalTypes.add(prestoColumnHandle.logicalType());
        }

        this.pageBuilder = new PageBuilder(columnTypes);
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getCompletedPositions() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public Page getNextPage() {
        try {
            return nextPage();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getSystemMemoryUsage() {
        return 0;
    }

    private Page nextPage() throws IOException {
        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
        if (batch == null) {
            isFinished = true;
            return null;
        }
        InternalRow row;
        while ((row = batch.next()) != null) {
            pageBuilder.declarePosition();
            for (int i = 0; i < columnTypes.size(); i++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(i);
                appendTo(
                        columnTypes.get(i),
                        logicalTypes.get(i),
                        RowDataUtils.get(row, i, logicalTypes.get(i)),
                        output);
            }
        }
        batch.releaseBatch();
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    private void appendTo(Type type, DataType logicalType, Object value, BlockBuilder output) {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, (Boolean) value);
        } else if (javaType == long.class) {
            if (type.equals(BIGINT)) {
                type.writeLong(output, ((Number) value).longValue());
            } else if (type.equals(INTEGER)) {
                type.writeLong(output, ((Number) value).intValue());
            } else if (type instanceof DecimalType) {
                Verify.verify(isShortDecimal(type), "The type should be short decimal");
                DecimalType decimalType = (DecimalType) type;
                BigDecimal decimal = ((Decimal) value).toBigDecimal();
                type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
            } else if (type.equals(DATE)) {
                type.writeLong(output, (int) value);
            } else if (type.equals(TIMESTAMP)) {
                type.writeLong(output, ((Timestamp) value).getMillisecond() * 1_000);
            } else if (type.equals(TIME)) {
                type.writeLong(output, (int) value * 1_000);
            } else {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        } else if (javaType == double.class) {
            type.writeDouble(output, ((Number) value).doubleValue());
        } else if (type instanceof DecimalType) {
            writeObject(output, type, value);
        } else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        } else if (javaType == Block.class) {
            writeBlock(output, type, logicalType, value);
        } else {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value) {
        if (type instanceof VarcharType || type instanceof CharType) {
            type.writeSlice(output, wrappedBuffer(((BinaryString) value).toBytes()));
        } else if (type instanceof VarbinaryType) {
            type.writeSlice(output, wrappedBuffer((byte[]) value));
        } else {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value) {
        if (type instanceof DecimalType) {
            Verify.verify(isLongDecimal(type), "The type should be long decimal");
            DecimalType decimalType = (DecimalType) type;
            BigDecimal decimal = ((Decimal) value).toBigDecimal();
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        } else {
            throw new PrestoException(
                    GENERIC_INTERNAL_ERROR,
                    "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, DataType logicalType, Object value) {
        if (type instanceof ArrayType) {
            BlockBuilder builder = output.beginBlockEntry();

            InternalArray arrayData = (InternalArray) value;
            DataType elementType = DataTypeChecks.getNestedTypes(logicalType).get(0);
            for (int i = 0; i < arrayData.size(); i++) {
                appendTo(
                        type.getTypeParameters().get(0),
                        elementType,
                        RowDataUtils.get(arrayData, i, elementType),
                        builder);
            }

            output.closeEntry();
            return;
        }
        if (type instanceof RowType) {
            InternalRow rowData = (InternalRow) value;
            BlockBuilder builder = output.beginBlockEntry();
            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                Type fieldType = type.getTypeParameters().get(index);
                DataType fieldLogicalType =
                        ((org.apache.flink.table.store.types.RowType) logicalType).getTypeAt(index);
                appendTo(
                        fieldType,
                        fieldLogicalType,
                        RowDataUtils.get(rowData, index, fieldLogicalType),
                        builder);
            }
            output.closeEntry();
            return;
        }
        if (type instanceof MapType) {
            InternalMap mapData = (InternalMap) value;
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            DataType keyType =
                    ((org.apache.flink.table.store.types.MapType) logicalType).getKeyType();
            DataType valueType =
                    ((org.apache.flink.table.store.types.MapType) logicalType).getValueType();
            BlockBuilder builder = output.beginBlockEntry();
            for (int i = 0; i < keyArray.size(); i++) {
                appendTo(
                        type.getTypeParameters().get(0),
                        keyType,
                        RowDataUtils.get(keyArray, i, keyType),
                        builder);
                appendTo(
                        type.getTypeParameters().get(1),
                        valueType,
                        RowDataUtils.get(valueArray, i, valueType),
                        builder);
            }
            output.closeEntry();
            return;
        }
        throw new PrestoException(
                GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }
}
