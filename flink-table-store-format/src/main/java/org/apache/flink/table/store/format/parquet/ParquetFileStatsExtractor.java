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

package org.apache.flink.table.store.format.parquet;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.store.utils.DateTimeUtils;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.table.store.format.parquet.ParquetUtil.assertStatsClass;

/** {@link FileStatsExtractor} for parquet files. */
public class ParquetFileStatsExtractor implements FileStatsExtractor {

    private final RowType rowType;
    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    public ParquetFileStatsExtractor(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public FieldStats[] extract(Path path) throws IOException {
        Map<String, Statistics> stats = ParquetUtil.extractColumnStats(path);

        return IntStream.range(0, rowType.getFieldCount())
                .mapToObj(
                        i -> {
                            RowType.RowField field = rowType.getFields().get(i);
                            return toFieldStats(field, stats.get(field.getName()));
                        })
                .toArray(FieldStats[]::new);
    }

    private FieldStats toFieldStats(RowType.RowField field, Statistics stats) {
        LogicalTypeRoot flinkType = field.getType().getTypeRoot();
        if (stats == null
                || flinkType == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || flinkType == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            throw new UnsupportedOperationException(
                    "type "
                            + field.getType().getTypeRoot()
                            + " not supported for extracting statistics in parquet format");
        }
        long nullCount = stats.getNumNulls();
        if (!stats.hasNonNullValue()) {
            return new FieldStats(null, null, nullCount);
        }

        switch (flinkType) {
            case CHAR:
            case VARCHAR:
                assertStatsClass(field, stats, BinaryStatistics.class);
                BinaryStatistics binaryStats = (BinaryStatistics) stats;
                return new FieldStats(
                        StringData.fromString(binaryStats.minAsString()),
                        StringData.fromString(binaryStats.maxAsString()),
                        nullCount);
            case BOOLEAN:
                assertStatsClass(field, stats, BooleanStatistics.class);
                BooleanStatistics boolStats = (BooleanStatistics) stats;
                return new FieldStats(boolStats.getMin(), boolStats.getMax(), nullCount);
            case DECIMAL:
                PrimitiveType primitive = stats.type();
                DecimalType decimalType = (DecimalType) (field.getType());
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (primitive.getOriginalType() != null
                        && primitive.getLogicalTypeAnnotation()
                                instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    return convertStatsToDecimalFieldStats(
                            primitive, field, stats, precision, scale, nullCount);
                } else {
                    return new FieldStats(null, null, nullCount);
                }
            case TINYINT:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics byteStats = (IntStatistics) stats;
                return new FieldStats(
                        (byte) byteStats.getMin(), (byte) byteStats.getMax(), nullCount);
            case SMALLINT:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics shortStats = (IntStatistics) stats;
                return new FieldStats(
                        (short) shortStats.getMin(), (short) shortStats.getMax(), nullCount);
            case INTEGER:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics intStats = (IntStatistics) stats;
                return new FieldStats(
                        Long.valueOf(intStats.getMin()).intValue(),
                        Long.valueOf(intStats.getMax()).intValue(),
                        nullCount);
            case BIGINT:
                assertStatsClass(field, stats, LongStatistics.class);
                LongStatistics longStats = (LongStatistics) stats;
                return new FieldStats(longStats.getMin(), longStats.getMax(), nullCount);
            case FLOAT:
                assertStatsClass(field, stats, FloatStatistics.class);
                FloatStatistics floatStats = (FloatStatistics) stats;
                return new FieldStats(floatStats.getMin(), floatStats.getMax(), nullCount);
            case DOUBLE:
                assertStatsClass(field, stats, DoubleStatistics.class);
                DoubleStatistics doubleStats = (DoubleStatistics) stats;
                return new FieldStats(doubleStats.getMin(), doubleStats.getMax(), nullCount);
            case DATE:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics dateStats = (IntStatistics) stats;
                return new FieldStats(
                        DateTimeUtils.toInternal(EPOCH_DAY.plusDays(dateStats.getMin())),
                        DateTimeUtils.toInternal(EPOCH_DAY.plusDays(dateStats.getMax())),
                        nullCount);
            default:
                return new FieldStats(null, null, nullCount);
        }
    }

    /**
     * Parquet cannot provide statistics for decimal fields directly, but we can extract them from
     * primitive statistics.
     */
    private FieldStats convertStatsToDecimalFieldStats(
            PrimitiveType primitive,
            RowType.RowField field,
            Statistics stats,
            int precision,
            int scale,
            long nullCount) {
        switch (primitive.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                assertStatsClass(field, stats, BinaryStatistics.class);
                BinaryStatistics decimalStats = (BinaryStatistics) stats;
                return new FieldStats(
                        DecimalData.fromBigDecimal(
                                new BigDecimal(new BigInteger(decimalStats.getMinBytes()), scale),
                                precision,
                                scale),
                        DecimalData.fromBigDecimal(
                                new BigDecimal(new BigInteger(decimalStats.getMaxBytes()), scale),
                                precision,
                                scale),
                        nullCount);
            case INT64:
                assertStatsClass(field, stats, LongStatistics.class);
                LongStatistics longStats = (LongStatistics) stats;
                return new FieldStats(
                        DecimalData.fromUnscaledLong(longStats.getMin(), precision, scale),
                        DecimalData.fromUnscaledLong(longStats.getMax(), precision, scale),
                        nullCount);
            case INT32:
                assertStatsClass(field, stats, IntStatistics.class);
                IntStatistics intStats = (IntStatistics) stats;
                return new FieldStats(
                        DecimalData.fromUnscaledLong(intStats.getMin(), precision, scale),
                        DecimalData.fromUnscaledLong(intStats.getMax(), precision, scale),
                        nullCount);
            default:
                return new FieldStats(null, null, nullCount);
        }
    }
}
