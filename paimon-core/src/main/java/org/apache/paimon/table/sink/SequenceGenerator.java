/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

/** Generate sequence number. */
public class SequenceGenerator {
    private final int index;

    private final Generator generator;
    private final DataType fieldType;

    public SequenceGenerator(String field, RowType rowType) {
        index = rowType.getFieldNames().indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format(
                            "Can not find sequence field %s in table schema: %s", field, rowType));
        }
        fieldType = rowType.getTypeAt(index);
        generator = fieldType.accept(new SequenceGeneratorVisitor());
    }

    public SequenceGenerator(int index, DataType dataType) {
        this.index = index;
        this.fieldType = dataType;
        if (index == -1) {
            throw new RuntimeException(String.format("Index : %s is invalid", index));
        }
        generator = fieldType.accept(new SequenceGeneratorVisitor());
    }

    public int index() {
        return index;
    }

    public DataType fieldType() {
        return fieldType;
    }

    @Nullable
    public Long generateNullable(InternalRow row) {
        return generator.generateNullable(row, index);
    }

    public long generate(InternalRow row) {
        return generator.generate(row, index);
    }

    public long generateWithPadding(InternalRow row, CoreOptions.SequenceAutoPadding autoPadding) {
        long sequence = generate(row);
        switch (autoPadding) {
            case ROW_KIND_FLAG:
                return addRowKindFlag(sequence, row.getRowKind());
            case SECOND_TO_MICRO:
                return secondToMicro(sequence);
            case MILLIS_TO_MICRO:
                return millisToMicro(sequence);
            default:
                throw new UnsupportedOperationException(
                        "Unknown sequence padding mode " + autoPadding.name());
        }
    }

    private long addRowKindFlag(long sequence, RowKind rowKind) {
        return (sequence << 1) | (rowKind.isAdd() ? 1 : 0);
    }

    private long millisToMicro(long sequence) {
        // Generated value is millis
        return sequence * 1_000 + getCurrentMicroOfMillis();
    }

    private long secondToMicro(long sequence) {
        // timestamp returns millis
        long second = fieldType.is(DataTypeFamily.TIMESTAMP) ? sequence / 1000 : sequence;
        return second * 1_000_000 + getCurrentMicroOfSeconds();
    }

    private static long getCurrentMicroOfMillis() {
        long currentNanoTime = System.nanoTime();
        long mills = TimeUnit.MILLISECONDS.convert(currentNanoTime, TimeUnit.NANOSECONDS);
        return (currentNanoTime - mills * 1_000_000) / 1000;
    }

    private static long getCurrentMicroOfSeconds() {
        long currentNanoTime = System.nanoTime();
        long seconds = TimeUnit.SECONDS.convert(currentNanoTime, TimeUnit.NANOSECONDS);
        return (currentNanoTime - seconds * 1_000_000_000) / 1000;
    }

    private interface Generator {
        long generate(InternalRow row, int i);

        @Nullable
        default Long generateNullable(InternalRow row, int i) {
            if (row.isNullAt(i)) {
                return null;
            }
            return generate(row, i);
        }
    }

    private static class SequenceGeneratorVisitor extends DataTypeDefaultVisitor<Generator> {

        @Override
        public Generator visit(CharType charType) {
            return stringGenerator();
        }

        @Override
        public Generator visit(VarCharType varCharType) {
            return stringGenerator();
        }

        private Generator stringGenerator() {
            return (row, i) -> Long.parseLong(row.getString(i).toString());
        }

        @Override
        public Generator visit(DecimalType decimalType) {
            return (row, i) ->
                    InternalRowUtils.castToIntegral(
                            row.getDecimal(i, decimalType.getPrecision(), decimalType.getScale()));
        }

        @Override
        public Generator visit(TinyIntType tinyIntType) {
            return InternalRow::getByte;
        }

        @Override
        public Generator visit(SmallIntType smallIntType) {
            return InternalRow::getShort;
        }

        @Override
        public Generator visit(IntType intType) {
            return InternalRow::getInt;
        }

        @Override
        public Generator visit(BigIntType bigIntType) {
            return InternalRow::getLong;
        }

        @Override
        public Generator visit(FloatType floatType) {
            return (row, i) -> (long) row.getFloat(i);
        }

        @Override
        public Generator visit(DoubleType doubleType) {
            return (row, i) -> (long) row.getDouble(i);
        }

        @Override
        public Generator visit(DateType dateType) {
            return InternalRow::getInt;
        }

        @Override
        public Generator visit(TimestampType timestampType) {
            return (row, i) -> row.getTimestamp(i, timestampType.getPrecision()).getMillisecond();
        }

        @Override
        public Generator visit(LocalZonedTimestampType localZonedTimestampType) {
            return (row, i) ->
                    row.getTimestamp(i, localZonedTimestampType.getPrecision()).getMillisecond();
        }

        @Override
        protected Generator defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }
}
