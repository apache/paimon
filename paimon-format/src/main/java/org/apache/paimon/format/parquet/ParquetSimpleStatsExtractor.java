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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.paimon.format.parquet.ParquetUtil.assertStatsClass;

/** {@link SimpleStatsExtractor} for parquet files. */
public class ParquetSimpleStatsExtractor implements SimpleStatsExtractor {

    private final RowType rowType;
    private final SimpleColStatsCollector.Factory[] statsCollectors;

    public ParquetSimpleStatsExtractor(
            RowType rowType, SimpleColStatsCollector.Factory[] statsCollectors) {
        this.rowType = rowType;
        this.statsCollectors = statsCollectors;
        Preconditions.checkArgument(
                rowType.getFieldCount() == statsCollectors.length,
                "The stats collector is not aligned to write schema.");
    }

    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path) throws IOException {
        return extractWithFileInfo(fileIO, path).getLeft();
    }

    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO fileIO, Path path)
            throws IOException {
        Pair<Map<String, Statistics<?>>, FileInfo> statsPair =
                ParquetUtil.extractColumnStats(fileIO, path);
        SimpleColStatsCollector[] collectors = SimpleColStatsCollector.create(statsCollectors);
        return Pair.of(
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                i -> {
                                    DataField field = rowType.getFields().get(i);
                                    return toFieldStats(
                                            field,
                                            statsPair.getLeft().get(field.name()),
                                            collectors[i]);
                                })
                        .toArray(SimpleColStats[]::new),
                statsPair.getRight());
    }

    private SimpleColStats toFieldStats(
            DataField field, Statistics<?> stats, SimpleColStatsCollector collector) {
        if (stats == null) {
            return new SimpleColStats(null, null, null);
        }
        long nullCount = stats.getNumNulls();
        if (!stats.hasNonNullValue()) {
            return collector.convert(new SimpleColStats(null, null, nullCount));
        }

        SimpleColStats fieldStats;
        switch (field.type().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                assertStatsClass(field, stats, BinaryStatistics.class);
                BinaryStatistics stringStats = (BinaryStatistics) stats;
                fieldStats =
                        new SimpleColStats(
                                BinaryString.fromString(stringStats.minAsString()),
                                BinaryString.fromString(stringStats.maxAsString()),
                                nullCount);
                break;
            case BOOLEAN:
                assertStatsClass(field, stats, BooleanStatistics.class);
                BooleanStatistics boolStats = (BooleanStatistics) stats;
                fieldStats = new SimpleColStats(boolStats.getMin(), boolStats.getMax(), nullCount);
                break;
            case DECIMAL:
                PrimitiveType primitive = stats.type();
                DecimalType decimalType = (DecimalType) (field.type());
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                fieldStats =
                        convertStatsToDecimalFieldStats(
                                primitive, field, stats, precision, scale, nullCount);
                break;
            case TINYINT:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics byteStats = (IntStatistics) stats;
                fieldStats =
                        new SimpleColStats(
                                (byte) byteStats.getMin(), (byte) byteStats.getMax(), nullCount);
                break;
            case SMALLINT:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics shortStats = (IntStatistics) stats;
                fieldStats =
                        new SimpleColStats(
                                (short) shortStats.getMin(),
                                (short) shortStats.getMax(),
                                nullCount);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics intStats = (IntStatistics) stats;
                fieldStats =
                        new SimpleColStats(
                                Long.valueOf(intStats.getMin()).intValue(),
                                Long.valueOf(intStats.getMax()).intValue(),
                                nullCount);
                break;
            case BIGINT:
                assertStatsClass(field, stats, LongStatistics.class);
                LongStatistics longStats = (LongStatistics) stats;
                fieldStats = new SimpleColStats(longStats.getMin(), longStats.getMax(), nullCount);
                break;
            case FLOAT:
                assertStatsClass(field, stats, FloatStatistics.class);
                FloatStatistics floatStats = (FloatStatistics) stats;
                fieldStats =
                        new SimpleColStats(floatStats.getMin(), floatStats.getMax(), nullCount);
                break;
            case DOUBLE:
                assertStatsClass(field, stats, DoubleStatistics.class);
                DoubleStatistics doubleStats = (DoubleStatistics) stats;
                fieldStats =
                        new SimpleColStats(doubleStats.getMin(), doubleStats.getMax(), nullCount);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                fieldStats = toTimestampStats(stats, ((TimestampType) field.type()).getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fieldStats =
                        toTimestampStats(
                                stats, ((LocalZonedTimestampType) field.type()).getPrecision());
                break;
            default:
                fieldStats = new SimpleColStats(null, null, nullCount);
        }
        return collector.convert(fieldStats);
    }

    private SimpleColStats toTimestampStats(Statistics<?> stats, int precision) {
        if (precision <= 3) {
            LongStatistics longStats = (LongStatistics) stats;
            return new SimpleColStats(
                    Timestamp.fromEpochMillis(longStats.getMin()),
                    Timestamp.fromEpochMillis(longStats.getMax()),
                    stats.getNumNulls());
        } else if (precision <= 6) {
            LongStatistics longStats = (LongStatistics) stats;
            return new SimpleColStats(
                    Timestamp.fromMicros(longStats.getMin()),
                    Timestamp.fromMicros(longStats.getMax()),
                    stats.getNumNulls());
        } else {
            return new SimpleColStats(null, null, stats.getNumNulls());
        }
    }

    /**
     * Parquet cannot provide statistics for decimal fields directly, but we can extract them from
     * primitive statistics.
     */
    private SimpleColStats convertStatsToDecimalFieldStats(
            PrimitiveType primitive,
            DataField field,
            Statistics<?> stats,
            int precision,
            int scale,
            long nullCount) {
        switch (primitive.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                assertStatsClass(field, stats, BinaryStatistics.class);
                BinaryStatistics decimalStats = (BinaryStatistics) stats;
                return new SimpleColStats(
                        Decimal.fromBigDecimal(
                                new BigDecimal(new BigInteger(decimalStats.getMinBytes()), scale),
                                precision,
                                scale),
                        Decimal.fromBigDecimal(
                                new BigDecimal(new BigInteger(decimalStats.getMaxBytes()), scale),
                                precision,
                                scale),
                        nullCount);
            case INT64:
                assertStatsClass(field, stats, LongStatistics.class);
                LongStatistics longStats = (LongStatistics) stats;
                return new SimpleColStats(
                        Decimal.fromUnscaledLong(longStats.getMin(), precision, scale),
                        Decimal.fromUnscaledLong(longStats.getMax(), precision, scale),
                        nullCount);
            case INT32:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics intStats = (IntStatistics) stats;
                return new SimpleColStats(
                        Decimal.fromUnscaledLong(intStats.getMin(), precision, scale),
                        Decimal.fromUnscaledLong(intStats.getMax(), precision, scale),
                        nullCount);
            default:
                return new SimpleColStats(null, null, nullCount);
        }
    }
}
