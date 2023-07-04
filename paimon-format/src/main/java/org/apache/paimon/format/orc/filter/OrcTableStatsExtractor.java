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
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.format.orc.OrcReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
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

import java.io.IOException;
import java.sql.Date;
import java.util.List;
import java.util.stream.IntStream;

/** {@link TableStatsExtractor} for orc files. */
public class OrcTableStatsExtractor implements TableStatsExtractor {

    private final RowType rowType;
    private final FieldStatsCollector[] stats;

    public OrcTableStatsExtractor(RowType rowType, FieldStatsCollector[] stats) {
        this.rowType = rowType;
        this.stats = stats;
        Preconditions.checkArgument(
                rowType.getFieldCount() == stats.length,
                "The stats collector is not aligned to write schema.");
    }

    @Override
    public FieldStats[] extract(FileIO fileIO, Path path) throws IOException {
        try (Reader reader = OrcReaderFactory.createReader(new Configuration(), fileIO, path)) {
            long rowCount = reader.getNumberOfRows();
            ColumnStatistics[] columnStatistics = reader.getStatistics();
            TypeDescription schema = reader.getSchema();

            List<String> columnNames = schema.getFieldNames();
            List<TypeDescription> columnTypes = schema.getChildren();

            return IntStream.range(0, rowType.getFieldCount())
                    .mapToObj(
                            i -> {
                                DataField field = rowType.getFields().get(i);
                                int fieldIdx = columnNames.indexOf(field.name());
                                int colId = columnTypes.get(fieldIdx).getId();
                                return toFieldStats(field, columnStatistics[colId], rowCount, i);
                            })
                    .toArray(FieldStats[]::new);
        }
    }

    private FieldStats toFieldStats(
            DataField field, ColumnStatistics stats, long rowCount, int idx) {
        long nullCount = rowCount - stats.getNumberOfValues();
        if (nullCount == rowCount) {
            // all nulls
            return this.stats[idx].convert(new FieldStats(null, null, nullCount));
        }
        Preconditions.checkState(
                (nullCount > 0) == stats.hasNull(),
                "Bug in OrcFileStatsExtractor: nullCount is "
                        + nullCount
                        + " while stats.hasNull() is "
                        + stats.hasNull()
                        + "!");

        FieldStats fieldStats;
        switch (field.type().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                assertStatsClass(field, stats, StringColumnStatistics.class);
                StringColumnStatistics stringStats = (StringColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                BinaryString.fromString(stringStats.getMinimum()),
                                BinaryString.fromString(stringStats.getMaximum()),
                                nullCount);
                break;
            case BOOLEAN:
                assertStatsClass(field, stats, BooleanColumnStatistics.class);
                BooleanColumnStatistics boolStats = (BooleanColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                boolStats.getFalseCount() == 0,
                                boolStats.getTrueCount() != 0,
                                nullCount);
                break;
            case DECIMAL:
                assertStatsClass(field, stats, DecimalColumnStatistics.class);
                DecimalColumnStatistics decimalStats = (DecimalColumnStatistics) stats;
                DecimalType decimalType = (DecimalType) (field.type());
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                fieldStats =
                        new FieldStats(
                                Decimal.fromBigDecimal(
                                        decimalStats.getMinimum().bigDecimalValue(),
                                        precision,
                                        scale),
                                Decimal.fromBigDecimal(
                                        decimalStats.getMaximum().bigDecimalValue(),
                                        precision,
                                        scale),
                                nullCount);
                break;
            case TINYINT:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics byteStats = (IntegerColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                (byte) byteStats.getMinimum(),
                                (byte) byteStats.getMaximum(),
                                nullCount);
                break;
            case SMALLINT:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics shortStats = (IntegerColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                (short) shortStats.getMinimum(),
                                (short) shortStats.getMaximum(),
                                nullCount);
                break;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics intStats = (IntegerColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                Long.valueOf(intStats.getMinimum()).intValue(),
                                Long.valueOf(intStats.getMaximum()).intValue(),
                                nullCount);
                break;
            case BIGINT:
                assertStatsClass(field, stats, IntegerColumnStatistics.class);
                IntegerColumnStatistics longStats = (IntegerColumnStatistics) stats;
                fieldStats =
                        new FieldStats(longStats.getMinimum(), longStats.getMaximum(), nullCount);
                break;
            case FLOAT:
                assertStatsClass(field, stats, DoubleColumnStatistics.class);
                DoubleColumnStatistics floatStats = (DoubleColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                (float) floatStats.getMinimum(),
                                (float) floatStats.getMaximum(),
                                nullCount);
                break;
            case DOUBLE:
                assertStatsClass(field, stats, DoubleColumnStatistics.class);
                DoubleColumnStatistics doubleStats = (DoubleColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                doubleStats.getMinimum(), doubleStats.getMaximum(), nullCount);
                break;
            case DATE:
                assertStatsClass(field, stats, DateColumnStatistics.class);
                DateColumnStatistics dateStats = (DateColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                DateTimeUtils.toInternal(
                                        new Date(dateStats.getMinimum().getTime())),
                                DateTimeUtils.toInternal(
                                        new Date(dateStats.getMaximum().getTime())),
                                nullCount);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                assertStatsClass(field, stats, TimestampColumnStatistics.class);
                TimestampColumnStatistics timestampStats = (TimestampColumnStatistics) stats;
                fieldStats =
                        new FieldStats(
                                Timestamp.fromSQLTimestamp(timestampStats.getMinimum()),
                                Timestamp.fromSQLTimestamp(timestampStats.getMaximum()),
                                nullCount);
                break;
            default:
                fieldStats = new FieldStats(null, null, nullCount);
        }
        return this.stats[idx].convert(fieldStats);
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
