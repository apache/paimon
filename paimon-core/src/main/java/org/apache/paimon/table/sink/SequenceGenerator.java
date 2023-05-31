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
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
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
    private final PaddingGenerator generatorWithPadding;

    public SequenceGenerator(String field, RowType rowType) {
        index = rowType.getFieldNames().indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format(
                            "Can not find sequence field %s in table schema: %s", field, rowType));
        }
        generator = rowType.getTypeAt(index).accept(new SequenceGeneratorVisitor());
        generatorWithPadding =
                rowType.getTypeAt(index).accept(new PaddingSequenceGeneratorVisitor(generator));
    }

    public int index() {
        return index;
    }

    @Nullable
    public Long generateNullable(InternalRow row) {
        return generator.generateNullable(row, index);
    }

    public long generate(InternalRow row) {
        return generator.generate(row, index);
    }

    public long generateWithPadding(InternalRow row, CoreOptions.SequenceAutoPadding autoPadding) {
        switch (autoPadding) {
            case SECOND_TO_MICRO:
                return generatorWithPadding.generateWithPaddingOnSeconds(row, index);
            case MILLIS_TO_MICRO:
                return generatorWithPadding.generateWithPaddingOnMillis(row, index);
            default:
                throw new UnsupportedOperationException(
                        "Unknown sequence padding mode " + autoPadding.name());
        }
    }

    private static long getCurrentMicroOfMillis() {
        long currentNanoTime = System.nanoTime();
        long mills = TimeUnit.MILLISECONDS.convert(currentNanoTime, TimeUnit.NANOSECONDS);
        long microOfMillis = (currentNanoTime - mills * 1_000_000) / 1000;
        return microOfMillis;
    }

    private static long getCurrentMicroOfSeconds() {
        long currentNanoTime = System.nanoTime();
        long seconds = TimeUnit.SECONDS.convert(currentNanoTime, TimeUnit.NANOSECONDS);
        long microOfSecs = (currentNanoTime - seconds * 1_000_000_000) / 1000;
        return microOfSecs;
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

    private interface PaddingGenerator {
        long generateWithPaddingOnSeconds(InternalRow row, int i);

        long generateWithPaddingOnMillis(InternalRow row, int i);
    }

    private static class DefaultPaddingGenerator implements PaddingGenerator {
        private Generator generator;

        public DefaultPaddingGenerator(Generator generator) {
            this.generator = generator;
        }

        @Override
        public long generateWithPaddingOnSeconds(InternalRow row, int i) {
            long second = generator.generate(row, i);
            long secondWithMicro = second * 1_000_000 + getCurrentMicroOfSeconds();
            return secondWithMicro;
        }

        @Override
        public long generateWithPaddingOnMillis(InternalRow row, int i) {
            long millis = generator.generate(row, i);
            long millisWithMicro = millis * 1_000 + getCurrentMicroOfMillis();
            return millisWithMicro;
        }
    }

    private static class TimestampPaddingGenerator implements PaddingGenerator {
        private Generator generator;

        public TimestampPaddingGenerator(Generator generator) {
            this.generator = generator;
        }

        @Override
        public long generateWithPaddingOnSeconds(InternalRow row, int i) {
            return generator.generate(row, i) / 1000 * 1_000_000 + getCurrentMicroOfSeconds();
        }

        @Override
        public long generateWithPaddingOnMillis(InternalRow row, int i) {
            return generator.generate(row, i) * 1_000 + getCurrentMicroOfMillis();
        }
    }

    private static class NoopPaddingGenerator implements PaddingGenerator {
        private Generator generator;

        public NoopPaddingGenerator(Generator generator) {
            this.generator = generator;
        }

        @Override
        public long generateWithPaddingOnSeconds(InternalRow row, int i) {
            return generator.generate(row, i);
        }

        @Override
        public long generateWithPaddingOnMillis(InternalRow row, int i) {
            return generator.generate(row, i);
        }
    }

    private static class PaddingSequenceGeneratorVisitor
            extends DataTypeDefaultVisitor<PaddingGenerator> {

        private Generator generator;

        public PaddingSequenceGeneratorVisitor(Generator generator) {
            this.generator = generator;
        }

        @Override
        public PaddingGenerator visit(TimestampType timestampType) {
            return new TimestampPaddingGenerator(generator);
        }

        @Override
        public PaddingGenerator visit(LocalZonedTimestampType localZonedTimestampType) {
            return new TimestampPaddingGenerator(generator);
        }

        @Override
        public PaddingGenerator visit(IntType intType) {
            return new DefaultPaddingGenerator(generator);
        }

        @Override
        public PaddingGenerator visit(BigIntType bigIntType) {
            return new DefaultPaddingGenerator(generator);
        }

        @Override
        protected PaddingGenerator defaultMethod(DataType dataType) {
            return new NoopPaddingGenerator(generator);
        }
    }
}
