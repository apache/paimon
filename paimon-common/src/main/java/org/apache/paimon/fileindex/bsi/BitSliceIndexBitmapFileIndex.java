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

package org.apache.paimon.fileindex.bsi;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexFilterPushDownAnalyzer;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.BitSliceIndexRoaringBitmap;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** implementation of BSI file index. */
public class BitSliceIndexBitmapFileIndex implements FileIndexer {

    public static final int VERSION_1 = 1;

    private final DataType dataType;

    public BitSliceIndexBitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            DataInput input = new DataInputStream(inputStream);
            byte version = input.readByte();
            if (version > VERSION_1) {
                throw new RuntimeException(
                        String.format(
                                "read bsi index file fail, "
                                        + "your plugin version is lower than %d",
                                version));
            }

            int rowNumber = input.readInt();

            boolean hasPositive = input.readBoolean();
            BitSliceIndexRoaringBitmap positive =
                    hasPositive
                            ? BitSliceIndexRoaringBitmap.map(input)
                            : BitSliceIndexRoaringBitmap.EMPTY;

            boolean hasNegative = input.readBoolean();
            BitSliceIndexRoaringBitmap negative =
                    hasNegative
                            ? BitSliceIndexRoaringBitmap.map(input)
                            : BitSliceIndexRoaringBitmap.EMPTY;

            return new Reader(dataType, rowNumber, positive, negative);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileIndexFilterPushDownAnalyzer createFilterPushDownAnalyzer() {
        return new FilterPushDownAnalyzer();
    }

    private static class Writer extends FileIndexWriter {

        private final Function<Object, Long> valueMapper;
        private final StatsCollectList collector;

        public Writer(DataType dataType) {
            this.valueMapper = getValueMapper(dataType);
            this.collector = new StatsCollectList();
        }

        @Override
        public void write(Object key) {
            collector.add(valueMapper.apply(key));
        }

        @Override
        public byte[] serializedBytes() {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutput out = new DataOutputStream(bos);

                BitSliceIndexRoaringBitmap.Appender positive =
                        new BitSliceIndexRoaringBitmap.Appender(
                                collector.positiveMin, collector.positiveMax);
                BitSliceIndexRoaringBitmap.Appender negative =
                        new BitSliceIndexRoaringBitmap.Appender(
                                collector.negativeMin, collector.negativeMax);

                for (int i = 0; i < collector.values.size(); i++) {
                    Long value = collector.values.get(i);
                    if (value != null) {
                        if (value < 0) {
                            negative.append(i, Math.abs(value));
                        } else {
                            positive.append(i, value);
                        }
                    }
                }

                out.writeByte(VERSION_1);
                out.writeInt(collector.values.size());

                boolean hasPositive = positive.isNotEmpty();
                out.writeBoolean(hasPositive);
                if (hasPositive) {
                    positive.serialize(out);
                }

                boolean hasNegative = negative.isNotEmpty();
                out.writeBoolean(hasNegative);
                if (hasNegative) {
                    negative.serialize(out);
                }
                return bos.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static class StatsCollectList {
            private long positiveMin;
            private long positiveMax;
            private long negativeMin;
            private long negativeMax;
            // todo: Find a way to reduce the risk of out-of-memory.
            private final List<Long> values = new ArrayList<>();

            public void add(Long value) {
                values.add(value);
                if (value != null) {
                    collect(value);
                }
            }

            private void collect(long value) {
                if (value < 0) {
                    negativeMin = Math.min(negativeMin, Math.abs(value));
                    negativeMax = Math.max(negativeMax, Math.abs(value));
                } else {
                    positiveMin = Math.min(positiveMin, value);
                    positiveMax = Math.max(positiveMax, value);
                }
            }
        }
    }

    private static class Reader extends FileIndexReader {

        private final int rowNumber;
        private final BitSliceIndexRoaringBitmap positive;
        private final BitSliceIndexRoaringBitmap negative;
        private final Function<Object, Long> valueMapper;

        public Reader(
                DataType dataType,
                int rowNumber,
                BitSliceIndexRoaringBitmap positive,
                BitSliceIndexRoaringBitmap negative) {
            this.rowNumber = rowNumber;
            this.positive = positive;
            this.negative = negative;
            this.valueMapper = getValueMapper(dataType);
        }

        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> {
                        RoaringBitmap32 bitmap =
                                RoaringBitmap32.or(positive.isNotNull(), negative.isNotNull());
                        bitmap.flip(0, rowNumber);
                        return bitmap;
                    });
        }

        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> RoaringBitmap32.or(positive.isNotNull(), negative.isNotNull()));
        }

        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return visitIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return visitNotIn(fieldRef, Collections.singletonList(literal));
        }

        @Override
        public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () ->
                            literals.stream()
                                    .map(valueMapper)
                                    .map(
                                            value -> {
                                                if (value < 0) {
                                                    return negative.eq(Math.abs(value));
                                                } else {
                                                    return positive.eq(value);
                                                }
                                            })
                                    .reduce(
                                            new RoaringBitmap32(),
                                            (x1, x2) -> RoaringBitmap32.or(x1, x2)));
        }

        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> {
                        RoaringBitmap32 ebm =
                                RoaringBitmap32.or(positive.isNotNull(), negative.isNotNull());
                        RoaringBitmap32 eq =
                                literals.stream()
                                        .map(valueMapper)
                                        .map(
                                                value -> {
                                                    if (value < 0) {
                                                        return negative.eq(Math.abs(value));
                                                    } else {
                                                        return positive.eq(value);
                                                    }
                                                })
                                        .reduce(
                                                new RoaringBitmap32(),
                                                (x1, x2) -> RoaringBitmap32.or(x1, x2));
                        return RoaringBitmap32.andNot(ebm, eq);
                    });
        }

        @Override
        public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return negative.gt(Math.abs(value));
                        } else {
                            return RoaringBitmap32.or(positive.lt(value), negative.isNotNull());
                        }
                    });
        }

        @Override
        public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return negative.gte(Math.abs(value));
                        } else {
                            return RoaringBitmap32.or(positive.lte(value), negative.isNotNull());
                        }
                    });
        }

        @Override
        public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return RoaringBitmap32.or(
                                    positive.isNotNull(), negative.lt(Math.abs(value)));
                        } else {
                            return positive.gt(value);
                        }
                    });
        }

        @Override
        public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    Collections.singleton(fieldRef),
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return RoaringBitmap32.or(
                                    positive.isNotNull(), negative.lte(Math.abs(value)));
                        } else {
                            return positive.gte(value);
                        }
                    });
        }
    }

    private static class FilterPushDownAnalyzer extends FileIndexFilterPushDownAnalyzer {

        @Override
        public Boolean visitIsNull(FieldRef fieldRef) {
            return true;
        }

        @Override
        public Boolean visitIsNotNull(FieldRef fieldRef) {
            return true;
        }

        @Override
        public Boolean visitEqual(FieldRef fieldRef, Object literal) {
            return true;
        }

        @Override
        public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
            return true;
        }

        @Override
        public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
            return true;
        }

        @Override
        public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return true;
        }

        @Override
        public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
            return true;
        }

        @Override
        public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return true;
        }

        @Override
        public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
            return true;
        }

        @Override
        public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return true;
        }
    }

    public static Function<Object, Long> getValueMapper(DataType dataType) {
        return dataType.accept(
                new DataTypeDefaultVisitor<Function<Object, Long>>() {
                    @Override
                    public Function<Object, Long> visit(DecimalType decimalType) {
                        return o -> o == null ? null : ((Decimal) o).toUnscaledLong();
                    }

                    @Override
                    public Function<Object, Long> visit(TinyIntType tinyIntType) {
                        return o -> o == null ? null : ((Byte) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(SmallIntType smallIntType) {
                        return o -> o == null ? null : ((Short) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(IntType intType) {
                        return o -> o == null ? null : ((Integer) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(BigIntType bigIntType) {
                        return o -> o == null ? null : (Long) o;
                    }

                    @Override
                    public Function<Object, Long> visit(DateType dateType) {
                        return o -> o == null ? null : ((Integer) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(TimeType timeType) {
                        return o -> o == null ? null : ((Integer) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(TimestampType timestampType) {
                        return getTimeStampMapper(timestampType.getPrecision());
                    }

                    @Override
                    public Function<Object, Long> visit(
                            LocalZonedTimestampType localZonedTimestampType) {
                        return getTimeStampMapper(localZonedTimestampType.getPrecision());
                    }

                    @Override
                    protected Function<Object, Long> defaultMethod(DataType dataType) {
                        throw new UnsupportedOperationException(
                                dataType.asSQLString()
                                        + " type is not support to build bsi index yet.");
                    }

                    private Function<Object, Long> getTimeStampMapper(int precision) {
                        return o -> {
                            if (o == null) {
                                return null;
                            } else if (precision <= 3) {
                                return ((Timestamp) o).getMillisecond();
                            } else {
                                return ((Timestamp) o).toMicros();
                            }
                        };
                    }
                });
    }
}
