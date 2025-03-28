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

package org.apache.paimon.format.orc.filter;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.orc.OrcReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.util.List;
import java.util.stream.IntStream;

/** {@link SimpleStatsExtractor} for orc files. */
public class OrcSimpleStatsExtractor implements SimpleStatsExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(OrcSimpleStatsExtractor.class);

    private final RowType rowType;
    private final SimpleColStatsCollector.Factory[] statsCollectors;
    private final boolean legacyTimestampLtzType;

    public OrcSimpleStatsExtractor(
            RowType rowType,
            SimpleColStatsCollector.Factory[] statsCollectors,
            boolean legacyTimestampLtzType) {
        this.rowType = rowType;
        this.statsCollectors = statsCollectors;
        this.legacyTimestampLtzType = legacyTimestampLtzType;
        Preconditions.checkArgument(
                rowType.getFieldCount() == statsCollectors.length,
                "The stats collector is not aligned to write schema.");
    }

    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException {
        return extractWithFileInfo(fileIO, path, length).getLeft();
    }

    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
            FileIO fileIO, Path path, long length) throws IOException {
        try (Reader reader =
                OrcReaderFactory.createReader(new Configuration(false), fileIO, path, null)) {
            long rowCount = reader.getNumberOfRows();
            ColumnStatistics[] columnStatistics = reader.getStatistics();
            TypeDescription schema = reader.getSchema();

            List<String> columnNames = schema.getFieldNames();
            List<TypeDescription> columnTypes = schema.getChildren();

            SimpleColStatsCollector[] collectors = SimpleColStatsCollector.create(statsCollectors);

            return Pair.of(
                    IntStream.range(0, rowType.getFieldCount())
                            .mapToObj(
                                    i -> {
                                        DataField field = rowType.getFields().get(i);
                                        int fieldIdx = columnNames.indexOf(field.name());
                                        if (fieldIdx == -1) {
                                            return collectors[i].convert(
                                                    new SimpleColStats(null, null, null));
                                        } else {
                                            int colId = columnTypes.get(fieldIdx).getId();
                                            return toFieldStats(
                                                    field,
                                                    columnStatistics[colId],
                                                    rowCount,
                                                    collectors[i]);
                                        }
                                    })
                            .toArray(SimpleColStats[]::new),
                    new FileInfo(rowCount));
        }
    }

    private SimpleColStats toFieldStats(
            DataField field,
            ColumnStatistics stats,
            long rowCount,
            SimpleColStatsCollector collector) {
        long nullCount = rowCount - stats.getNumberOfValues();
        if (nullCount == rowCount) {
            // all nulls
            return collector.convert(new SimpleColStats(null, null, nullCount));
        }
        Preconditions.checkState(
                (nullCount > 0) == stats.hasNull(),
                "Bug in OrcFileStatsExtractor: nullCount is "
                        + nullCount
                        + " while stats.hasNull() is "
                        + stats.hasNull()
                        + "!");

        SimpleColStats fieldStats;
        try {
            fieldStats = toFieldStats(field, stats, nullCount);
        } catch (Exception e) {
            LOG.warn("Failed to extract field stats for field {}", field, e);
            fieldStats = new SimpleColStats(null, null, nullCount);
        }

        return collector.convert(fieldStats);
    }

    private SimpleColStats toFieldStats(DataField field, ColumnStatistics stats, long nullCount) {
        switch (field.type().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                assertStatsClass(field, stats, StringColumnStatistics.class);
                StringColumnStatistics stringStats = (StringColumnStatistics) stats;
                return new SimpleColStats(
                        BinaryString.fromString(stringStats.getMinimum()),
                        BinaryString.fromString(stringStats.getMaximum()),
                        nullCount);
            case BOOLEAN:
                assertStatsClass(field, stats, BooleanColumnStatistics.class);
                BooleanColumnStatistics boolStats = (BooleanColumnStatistics) stats;
                return new SimpleColStats(
                        boolStats.getFalseCount() == 0, boolStats.getTrueCount() != 0, nullCount);
            case DECIMAL:
                assertStatsClass(field, stats, DecimalColumnStatistics.class);
                DecimalColumnStatistics decimalStats = (DecimalColumnStatistics) stats;
                DecimalType decimalType = (DecimalType) (field.type());
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return new SimpleColStats(
                        Decimal.fromBigDecimal(
                                decimalStats.getMinimum().bigDecimalValue(), precision, scale),
                        Decimal.fromBigDecimal(
                                decimalStats.getMaximum().bigDecimalValue(), precision, scale),
                        nullCount);
            case TINYINT:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics byteStats = (IntegerColumnStatistics) stats;
                return new SimpleColStats(
                        (byte) byteStats.getMinimum(), (byte) byteStats.getMaximum(), nullCount);
            case SMALLINT:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics shortStats = (IntegerColumnStatistics) stats;
                return new SimpleColStats(
                        (short) shortStats.getMinimum(),
                        (short) shortStats.getMaximum(),
                        nullCount);
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics intStats = (IntegerColumnStatistics) stats;
                return new SimpleColStats(
                        Long.valueOf(intStats.getMinimum()).intValue(),
                        Long.valueOf(intStats.getMaximum()).intValue(),
                        nullCount);
            case BIGINT:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics longStats = (IntegerColumnStatistics) stats;
                return new SimpleColStats(
                        longStats.getMinimum(), longStats.getMaximum(), nullCount);
            case FLOAT:
                assertStatsClass(field, stats, DoubleColumnStatistics.class);
                DoubleColumnStatistics floatStats = (DoubleColumnStatistics) stats;
                return new SimpleColStats(
                        (float) floatStats.getMinimum(),
                        (float) floatStats.getMaximum(),
                        nullCount);
            case DOUBLE:
                assertStatsClass(field, stats, DoubleColumnStatistics.class);
                DoubleColumnStatistics doubleStats = (DoubleColumnStatistics) stats;
                return new SimpleColStats(
                        doubleStats.getMinimum(), doubleStats.getMaximum(), nullCount);
            case DATE:
                assertStatsClass(field, stats, DateColumnStatistics.class);
                DateColumnStatistics dateStats = (DateColumnStatistics) stats;
                return new SimpleColStats(
                        DateTimeUtils.toInternal(new Date(dateStats.getMinimum().getTime())),
                        DateTimeUtils.toInternal(new Date(dateStats.getMaximum().getTime())),
                        nullCount);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                assertStatsClass(field, stats, TimestampColumnStatistics.class);
                TimestampColumnStatistics timestampStats = (TimestampColumnStatistics) stats;
                return new SimpleColStats(
                        Timestamp.fromSQLTimestamp(timestampStats.getMinimum()),
                        Timestamp.fromSQLTimestamp(timestampStats.getMaximum()),
                        nullCount);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                assertStatsClass(field, stats, TimestampColumnStatistics.class);
                TimestampColumnStatistics timestampLtzStats = (TimestampColumnStatistics) stats;
                return legacyTimestampLtzType
                        ? new SimpleColStats(
                                Timestamp.fromSQLTimestamp(timestampLtzStats.getMinimum()),
                                Timestamp.fromSQLTimestamp(timestampLtzStats.getMaximum()),
                                nullCount)
                        : new SimpleColStats(
                                Timestamp.fromInstant(timestampLtzStats.getMinimum().toInstant()),
                                Timestamp.fromInstant(timestampLtzStats.getMaximum().toInstant()),
                                nullCount);
            default:
                return new SimpleColStats(null, null, nullCount);
        }
    }

    private void assertStatsClass(
            DataField field,
            ColumnStatistics stats,
            Class<? extends ColumnStatistics> expectedClass) {
        if (!expectedClass.isInstance(stats)) {
            throw new IllegalArgumentException(
                    "Expecting "
                            + expectedClass.getName()
                            + " for field "
                            + field.asSQLString()
                            + " but found "
                            + stats.getClass().getName());
        }
    }
}
