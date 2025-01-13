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

package org.apache.paimon.flink.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility methods for analysis table. */
public class TableStatsUtil {

    /** create Paimon statistics. */
    @Nullable
    public static Statistics createTableStats(
            FileStoreTable table, CatalogTableStatistics catalogTableStatistics) {
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        if (snapshot == null) {
            return null;
        }
        return new Statistics(
                snapshot.id(),
                snapshot.schemaId(),
                catalogTableStatistics.getRowCount(),
                catalogTableStatistics.getTotalSize());
    }

    /** Create Paimon statistics from given Flink columnStatistics. */
    @Nullable
    public static Statistics createTableColumnStats(
            FileStoreTable table, CatalogColumnStatistics columnStatistics) {
        if (!table.statistics().isPresent()) {
            return null;
        }
        Statistics statistics = table.statistics().get();
        List<DataField> fields = table.schema().fields();
        Map<String, ColStats<?>> tableColumnStatsMap = new HashMap<>(fields.size());
        for (DataField field : fields) {
            CatalogColumnStatisticsDataBase catalogColumnStatisticsDataBase =
                    columnStatistics.getColumnStatisticsData().get(field.name());
            if (catalogColumnStatisticsDataBase == null) {
                continue;
            }
            tableColumnStatsMap.put(
                    field.name(), getPaimonColStats(field, catalogColumnStatisticsDataBase));
        }
        statistics.colStats().putAll(tableColumnStatsMap);
        return statistics;
    }

    /** Convert Flink ColumnStats to Paimon ColStats according to Paimon column type. */
    private static ColStats<?> getPaimonColStats(
            DataField field, CatalogColumnStatisticsDataBase colStat) {
        DataTypeRoot typeRoot = field.type().getTypeRoot();
        if (colStat instanceof CatalogColumnStatisticsDataString) {
            CatalogColumnStatisticsDataString stringColStat =
                    (CatalogColumnStatisticsDataString) colStat;
            if (typeRoot.equals(DataTypeRoot.CHAR) || typeRoot.equals(DataTypeRoot.VARCHAR)) {
                return ColStats.newColStats(
                        field.id(),
                        null != stringColStat.getNdv() ? stringColStat.getNdv() : null,
                        null,
                        null,
                        null != stringColStat.getNullCount() ? stringColStat.getNullCount() : null,
                        null != stringColStat.getAvgLength()
                                ? stringColStat.getAvgLength().longValue()
                                : null,
                        null != stringColStat.getMaxLength() ? stringColStat.getMaxLength() : null);
            }
        } else if (colStat instanceof CatalogColumnStatisticsDataBoolean) {
            CatalogColumnStatisticsDataBoolean booleanColStat =
                    (CatalogColumnStatisticsDataBoolean) colStat;
            if (typeRoot.equals(DataTypeRoot.BOOLEAN)) {
                return ColStats.newColStats(
                        field.id(),
                        (booleanColStat.getFalseCount() > 0 ? 1L : 0)
                                + (booleanColStat.getTrueCount() > 0 ? 1L : 0),
                        null,
                        null,
                        booleanColStat.getNullCount(),
                        null,
                        null);
            }
        } else if (colStat instanceof CatalogColumnStatisticsDataLong) {
            CatalogColumnStatisticsDataLong longColStat = (CatalogColumnStatisticsDataLong) colStat;
            if (typeRoot.equals(DataTypeRoot.INTEGER)) {
                return ColStats.newColStats(
                        field.id(),
                        null != longColStat.getNdv() ? longColStat.getNdv() : null,
                        null != longColStat.getMin() ? longColStat.getMin().intValue() : null,
                        null != longColStat.getMax() ? longColStat.getMax().intValue() : null,
                        null != longColStat.getNullCount() ? longColStat.getNullCount() : null,
                        null,
                        null);
            } else if (typeRoot.equals(DataTypeRoot.TINYINT)) {
                return ColStats.newColStats(
                        field.id(),
                        null != longColStat.getNdv() ? longColStat.getNdv() : null,
                        null != longColStat.getMin() ? longColStat.getMin().byteValue() : null,
                        null != longColStat.getMax() ? longColStat.getMax().byteValue() : null,
                        null != longColStat.getNullCount() ? longColStat.getNullCount() : null,
                        null,
                        null);

            } else if (typeRoot.equals(DataTypeRoot.SMALLINT)) {
                return ColStats.newColStats(
                        field.id(),
                        null != longColStat.getNdv() ? longColStat.getNdv() : null,
                        null != longColStat.getMin() ? longColStat.getMin().shortValue() : null,
                        null != longColStat.getMax() ? longColStat.getMax().shortValue() : null,
                        null != longColStat.getNullCount() ? longColStat.getNullCount() : null,
                        null,
                        null);
            } else if (typeRoot.equals(DataTypeRoot.BIGINT)) {
                return ColStats.newColStats(
                        field.id(),
                        null != longColStat.getNdv() ? longColStat.getNdv() : null,
                        null != longColStat.getMin() ? longColStat.getMin() : null,
                        null != longColStat.getMax() ? longColStat.getMax() : null,
                        null != longColStat.getNullCount() ? longColStat.getNullCount() : null,
                        null,
                        null);
            } else if (typeRoot.equals(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                return ColStats.newColStats(
                        field.id(),
                        null != longColStat.getNdv() ? longColStat.getNdv() : null,
                        null != longColStat.getMin()
                                ? org.apache.paimon.data.Timestamp.fromSQLTimestamp(
                                        new Timestamp(longColStat.getMin()))
                                : null,
                        null != longColStat.getMax()
                                ? org.apache.paimon.data.Timestamp.fromSQLTimestamp(
                                        new Timestamp(longColStat.getMax()))
                                : null,
                        null != longColStat.getNullCount() ? longColStat.getNullCount() : null,
                        null,
                        null);
            }
        } else if (colStat instanceof CatalogColumnStatisticsDataDouble) {
            CatalogColumnStatisticsDataDouble doubleColumnStatsData =
                    (CatalogColumnStatisticsDataDouble) colStat;
            if (typeRoot.equals(DataTypeRoot.FLOAT)) {
                return ColStats.newColStats(
                        field.id(),
                        null != doubleColumnStatsData.getNdv()
                                ? doubleColumnStatsData.getNdv()
                                : null,
                        null != doubleColumnStatsData.getMin()
                                ? doubleColumnStatsData.getMin().floatValue()
                                : null,
                        null != doubleColumnStatsData.getMax()
                                ? doubleColumnStatsData.getMax().floatValue()
                                : null,
                        null != doubleColumnStatsData.getNullCount()
                                ? doubleColumnStatsData.getNullCount()
                                : null,
                        null,
                        null);
            } else if (typeRoot.equals(DataTypeRoot.DOUBLE)) {
                return ColStats.newColStats(
                        field.id(),
                        null != doubleColumnStatsData.getNdv()
                                ? doubleColumnStatsData.getNdv()
                                : null,
                        null != doubleColumnStatsData.getMin()
                                ? doubleColumnStatsData.getMin()
                                : null,
                        null != doubleColumnStatsData.getMax()
                                ? doubleColumnStatsData.getMax()
                                : null,
                        null != doubleColumnStatsData.getNullCount()
                                ? doubleColumnStatsData.getNullCount()
                                : null,
                        null,
                        null);
            } else if (typeRoot.equals(DataTypeRoot.DECIMAL)) {
                BigDecimal max = BigDecimal.valueOf(doubleColumnStatsData.getMax());
                BigDecimal min = BigDecimal.valueOf(doubleColumnStatsData.getMin());
                return ColStats.newColStats(
                        field.id(),
                        null != doubleColumnStatsData.getNdv()
                                ? doubleColumnStatsData.getNdv()
                                : null,
                        null != doubleColumnStatsData.getMin()
                                ? Decimal.fromBigDecimal(min, min.precision(), min.scale())
                                : null,
                        null != doubleColumnStatsData.getMax()
                                ? Decimal.fromBigDecimal(max, max.precision(), max.scale())
                                : null,
                        null != doubleColumnStatsData.getNullCount()
                                ? doubleColumnStatsData.getNullCount()
                                : null,
                        null,
                        null);
            }
        } else if (colStat instanceof CatalogColumnStatisticsDataDate) {
            CatalogColumnStatisticsDataDate dateColumnStatsData =
                    (CatalogColumnStatisticsDataDate) colStat;
            if (typeRoot.equals(DataTypeRoot.DATE)) {
                return ColStats.newColStats(
                        field.id(),
                        null != dateColumnStatsData.getNdv() ? dateColumnStatsData.getNdv() : null,
                        null != dateColumnStatsData.getMin()
                                ? new Long(dateColumnStatsData.getMin().getDaysSinceEpoch())
                                        .intValue()
                                : null,
                        null != dateColumnStatsData.getMax()
                                ? new Long(dateColumnStatsData.getMax().getDaysSinceEpoch())
                                        .intValue()
                                : null,
                        null != dateColumnStatsData.getNullCount()
                                ? dateColumnStatsData.getNullCount()
                                : null,
                        null,
                        null);
            }
        } else if (colStat instanceof CatalogColumnStatisticsDataBinary) {
            CatalogColumnStatisticsDataBinary binaryColumnStatsData =
                    (CatalogColumnStatisticsDataBinary) colStat;
            if (typeRoot.equals(DataTypeRoot.VARBINARY) || typeRoot.equals(DataTypeRoot.BINARY)) {
                return ColStats.newColStats(
                        field.id(),
                        null,
                        null,
                        null,
                        null != binaryColumnStatsData.getNullCount()
                                ? binaryColumnStatsData.getNullCount()
                                : null,
                        null != binaryColumnStatsData.getAvgLength()
                                ? binaryColumnStatsData.getAvgLength().longValue()
                                : null,
                        null != binaryColumnStatsData.getMaxLength()
                                ? binaryColumnStatsData.getMaxLength()
                                : null);
            }
        }
        throw new CatalogException(
                String.format(
                        "Flink does not support convert ColumnStats '%s' for Paimon column "
                                + "type '%s' yet",
                        colStat, field.type()));
    }
}
