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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * A {@link BaseDataTableSource} implements {@link SupportsStatisticReport} and {@link
 * SupportsDynamicFiltering}.
 */
public class DataTableSource extends BaseDataTableSource
        implements SupportsStatisticReport, SupportsDynamicFiltering {

    @Nullable private List<String> dynamicPartitionFilteringFields;

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                null,
                null,
                null,
                null,
                null,
                false);
    }

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields,
            boolean isBatchCountStar) {
        super(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                isBatchCountStar);
        this.dynamicPartitionFilteringFields = dynamicPartitionFilteringFields;
    }

    @Override
    public DataTableSource copy() {
        return new DataTableSource(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                isBatchCountStar);
    }

    @Override
    public TableStats reportStatistics() {
        if (streaming) {
            return TableStats.UNKNOWN;
        }
        scanSplitsForInference();
        Optional<Statistics> optionStatistics = table.statistics();
        if (optionStatistics.isPresent()) {
            Statistics statistics = optionStatistics.get();
            if (statistics.mergedRecordCount().isPresent()) {
                Map<String, ColumnStats> flinkColStats =
                        statistics.colStats().entrySet().stream()
                                .map(
                                        entry ->
                                                new AbstractMap.SimpleEntry<>(
                                                        entry.getKey(),
                                                        toFlinkColumnStats(entry.getValue())))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return new TableStats(statistics.mergedRecordCount().getAsLong(), flinkColStats);
            }
        }
        return new TableStats(splitStatistics.totalRowCount());
    }

    @Override
    public List<String> listAcceptedFilterFields() {
        // note that streaming query doesn't support dynamic filtering
        return streaming ? Collections.emptyList() : table.partitionKeys();
    }

    @Override
    public void applyDynamicFiltering(List<String> candidateFilterFields) {
        checkState(
                !streaming,
                "Cannot apply dynamic filtering to Paimon table '%s' when streaming reading.",
                table.name());

        checkState(
                !table.partitionKeys().isEmpty(),
                "Cannot apply dynamic filtering to non-partitioned Paimon table '%s'.",
                table.name());

        this.dynamicPartitionFilteringFields = candidateFilterFields;
    }

    @Override
    protected List<String> dynamicPartitionFilteringFields() {
        return dynamicPartitionFilteringFields;
    }

    private ColumnStats toFlinkColumnStats(ColStats<?> colStats) {
        return ColumnStats.Builder.builder()
                .setNdv(
                        colStats.distinctCount().isPresent()
                                ? colStats.distinctCount().getAsLong()
                                : null)
                .setNullCount(
                        colStats.nullCount().isPresent() ? colStats.nullCount().getAsLong() : null)
                .setAvgLen(
                        colStats.avgLen().isPresent()
                                ? (double) colStats.avgLen().getAsLong()
                                : null)
                .setMaxLen(
                        colStats.maxLen().isPresent() ? (int) colStats.maxLen().getAsLong() : null)
                .setMax(colStats.max().isPresent() ? colStats.max().get() : null)
                .setMin(colStats.min().isPresent() ? colStats.min().get() : null)
                .build();
    }
}
