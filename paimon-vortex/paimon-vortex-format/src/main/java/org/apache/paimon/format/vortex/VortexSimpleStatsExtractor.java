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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Pair;

import dev.vortex.api.VortexColumnStatistics;
import dev.vortex.api.VortexWriteSummary;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.OptionalLong;

/**
 * Extracts statistics of Vortex files.
 *
 * <p>A file that this process wrote has its statistics in the native write summary, so nothing is
 * read again. Any other file is scanned once to collect them.
 */
public class VortexSimpleStatsExtractor implements SimpleStatsExtractor {

    private final RowType rowType;
    private final SimpleColStatsCollector.Factory[] statsCollectors;

    public VortexSimpleStatsExtractor(
            RowType rowType, SimpleColStatsCollector.Factory[] statsCollectors) {
        this.rowType = rowType;
        this.statsCollectors = statsCollectors;
    }

    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException {
        return extractWithFileInfo(fileIO, path, length).getLeft();
    }

    @Override
    public SimpleColStats[] extract(
            FileIO fileIO, Path path, long length, @Nullable Object writerMetadata)
            throws IOException {
        if (writerMetadata instanceof VortexWriteSummary) {
            return fromSummary((VortexWriteSummary) writerMetadata);
        }
        return extract(fileIO, path, length);
    }

    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
            FileIO fileIO, Path path, long length) throws IOException {
        SimpleStatsCollector collector = new SimpleStatsCollector(rowType, statsCollectors);
        long rowCount = 0;

        try (VortexRecordsReader reader =
                new VortexRecordsReader(
                        VortexInput.of(fileIO, path, length), path, rowType, rowType, null, null)) {
            FileRecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (!collector.isDisabled()) {
                            collector.collect(row);
                        }
                        rowCount++;
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }

        return Pair.of(collector.extract(), new FileInfo(rowCount));
    }

    private SimpleColStats[] fromSummary(VortexWriteSummary summary) {
        SimpleColStatsCollector[] collectors = SimpleColStatsCollector.create(statsCollectors);
        SimpleColStats[] result = new SimpleColStats[rowType.getFieldCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = collectors[i].convert(SimpleColStats.NONE);
        }

        List<VortexColumnStatistics> columns = summary.columnStatistics();
        for (VortexColumnStatistics column : columns) {
            int index = column.columnIndex();
            if (index < 0 || index >= result.length) {
                continue;
            }

            DataType type = rowType.getTypeAt(index);
            OptionalLong nullCount = column.nullValueCount();
            Object min = null;
            Object max = null;
            if (hasUsableBounds(column)) {
                min = toPaimon(column.lowerBound().orElse(null), type);
                max = toPaimon(column.upperBound().orElse(null), type);
                if (min == null || max == null) {
                    min = null;
                    max = null;
                }
            }

            SimpleColStats stats =
                    new SimpleColStats(
                            min, max, nullCount.isPresent() ? nullCount.getAsLong() : null);
            result[index] = collectors[index].convert(stats);
        }
        return result;
    }

    /**
     * Vortex leaves NaN out of the bounds of a floating point column, while Paimon orders NaN above
     * every number. So such a column has no usable bounds when it holds a NaN.
     */
    private static boolean hasUsableBounds(VortexColumnStatistics column) {
        OptionalLong nanCount = column.nanValueCount();
        return !nanCount.isPresent() || nanCount.getAsLong() == 0;
    }

    /**
     * Converts a bound from the Java type of its Arrow storage scalar to the Paimon internal type.
     * Bounds of other types are not used.
     */
    @Nullable
    private static Object toPaimon(@Nullable Object value, DataType type) {
        if (value == null) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return value instanceof Boolean ? value : null;
            case TINYINT:
                return value instanceof Integer ? (byte) (int) (Integer) value : null;
            case SMALLINT:
                return value instanceof Integer ? (short) (int) (Integer) value : null;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return value instanceof Integer ? value : null;
            case BIGINT:
                return value instanceof Long ? value : null;
            case FLOAT:
                return value instanceof Float ? value : null;
            case DOUBLE:
                return value instanceof Double ? value : null;
            case DECIMAL:
                if (!(value instanceof BigDecimal)) {
                    return null;
                }
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        (BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
            case CHAR:
            case VARCHAR:
                return value instanceof String ? BinaryString.fromString((String) value) : null;
            case BINARY:
            case VARBINARY:
                return value instanceof byte[] ? value : null;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value instanceof Long
                        ? toTimestamp((Long) value, ((TimestampType) type).getPrecision())
                        : null;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return value instanceof Long
                        ? toTimestamp((Long) value, ((LocalZonedTimestampType) type).getPrecision())
                        : null;
            default:
                return null;
        }
    }

    /** The Arrow time unit of a timestamp follows its precision, see ArrowFieldTypeConversion. */
    private static Timestamp toTimestamp(long value, int precision) {
        if (precision == 0) {
            return Timestamp.fromEpochMillis(Math.multiplyExact(value, 1000L));
        } else if (precision <= 3) {
            return Timestamp.fromEpochMillis(value);
        } else if (precision <= 6) {
            return Timestamp.fromMicros(value);
        } else {
            return Timestamp.fromEpochMillis(
                    Math.floorDiv(value, 1_000_000L), (int) Math.floorMod(value, 1_000_000L));
        }
    }
}
