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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.flink.dataevolution.DataEvolutionRowLevelModificationScanContext;
import org.apache.paimon.flink.source.aggregate.PushedAggregateResult;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.table.system.RowTrackingTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan.RowLevelModificationType;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    @Nullable protected List<String> dynamicPartitionFilteringFields;
    @Nullable private Long rowLevelModificationSnapshotId;
    private List<String> metadataKeys = Collections.emptyList();

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean unbounded,
            DynamicTableFactory.Context context) {
        this(tableIdentifier, table, unbounded, context, null, null, null, null, null);
    }

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean unbounded,
            DynamicTableFactory.Context context,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields) {
        this(
                tableIdentifier,
                table,
                unbounded,
                context,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                null);
    }

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean unbounded,
            DynamicTableFactory.Context context,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields,
            @Nullable PushedAggregateResult pushedAggregateResult) {
        super(
                tableIdentifier,
                table,
                unbounded,
                context,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                pushedAggregateResult);
        this.dynamicPartitionFilteringFields = dynamicPartitionFilteringFields;
    }

    @Override
    public DataTableSource copy() {
        DataTableSource copied = newSource();
        copied.rowLevelModificationSnapshotId = rowLevelModificationSnapshotId;
        copied.metadataKeys = metadataKeys;
        return copied;
    }

    protected DataTableSource newSource() {
        return new DataTableSource(
                tableIdentifier,
                table,
                unbounded,
                context,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                pushedAggregateResult);
    }

    public RowLevelModificationScanContext applyRowLevelModificationScan(
            RowLevelModificationType rowLevelModificationType,
            @Nullable RowLevelModificationScanContext previousContext) {
        if (rowLevelModificationType != RowLevelModificationType.DELETE
                || !isDataEvolutionTable()
                || TimeTravelUtil.hasTimeTravelOptions(Options.fromMap(table.options()))) {
            return previousContext;
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        Long snapshotId = fileStoreTable.snapshotManager().latestSnapshotId();
        rowLevelModificationSnapshotId =
                snapshotId == null
                        ? DataEvolutionRowLevelModificationScanContext.EMPTY_TABLE_SNAPSHOT
                        : snapshotId;
        return DataEvolutionRowLevelModificationScanContext.addSnapshot(
                previousContext,
                fileStoreTable.location().toString(),
                fileStoreTable.snapshotManager().branch(),
                rowLevelModificationSnapshotId);
    }

    public Map<String, DataType> listReadableMetadata() {
        // Flink calls this after applyRowLevelModificationScan for row-level operations.
        if (rowLevelModificationSnapshotId == null || !isDataEvolutionTable()) {
            return Collections.emptyMap();
        }
        Map<String, DataType> metadata = new LinkedHashMap<>();
        metadata.put(SpecialFields.ROW_ID.name(), DataTypes.BIGINT().notNull());
        return metadata;
    }

    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        for (String metadataKey : metadataKeys) {
            if (!SpecialFields.ROW_ID.name().equals(metadataKey)) {
                throw new UnsupportedOperationException(
                        "Unsupported Paimon metadata column: " + metadataKey);
            }
        }
        this.metadataKeys = metadataKeys;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (rowLevelModificationSnapshotId == null
                || rowLevelModificationSnapshotId
                        != DataEvolutionRowLevelModificationScanContext.EMPTY_TABLE_SNAPSHOT) {
            return super.getScanRuntimeProvider(scanContext);
        }

        Table scanTable = tableForScan();
        org.apache.paimon.types.RowType rowType = scanTable.rowType();
        int[][] projection = projectFieldsForScan();
        if (projection != null) {
            rowType = Projection.of(projection).project(rowType);
        }
        StaticRowDataSource source = new StaticRowDataSource(Collections.emptyList(), rowType);
        return new PaimonDataStreamScanProvider(
                true,
                env ->
                        env.fromSource(
                                        source,
                                        WatermarkStrategy.noWatermarks(),
                                        tableIdentifier.asSummaryString())
                                .setParallelism(1),
                tableIdentifier.asSummaryString(),
                table);
    }

    @Override
    protected Table tableForScan() {
        if (rowLevelModificationSnapshotId == null) {
            return table;
        }

        FileStoreTable fileStoreTable = (FileStoreTable) table;
        if (rowLevelModificationSnapshotId
                != DataEvolutionRowLevelModificationScanContext.EMPTY_TABLE_SNAPSHOT) {
            Map<String, String> options = new HashMap<>();
            options.put(
                    CoreOptions.SCAN_SNAPSHOT_ID.key(),
                    String.valueOf(rowLevelModificationSnapshotId));
            options.put(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full");
            fileStoreTable = (FileStoreTable) fileStoreTable.copy(options);
        }

        return metadataKeys.isEmpty() ? fileStoreTable : new RowTrackingTable(fileStoreTable);
    }

    @Override
    protected int[][] projectFieldsForScan() {
        if (metadataKeys.isEmpty()) {
            return projectFields;
        }

        int physicalFieldCount = table.rowType().getFieldCount();
        int[][] physicalProjection = projectFields;
        if (physicalProjection == null) {
            physicalProjection = new int[physicalFieldCount][];
            for (int i = 0; i < physicalFieldCount; i++) {
                physicalProjection[i] = new int[] {i};
            }
        }

        int[][] projection =
                Arrays.copyOf(physicalProjection, physicalProjection.length + metadataKeys.size());
        for (int i = 0; i < metadataKeys.size(); i++) {
            projection[physicalProjection.length + i] = new int[] {physicalFieldCount};
        }
        return projection;
    }

    private boolean isDataEvolutionTable() {
        return table instanceof FileStoreTable
                && ((FileStoreTable) table).coreOptions().dataEvolutionEnabled();
    }

    @Override
    public TableStats reportStatistics() {
        if (unbounded) {
            return TableStats.UNKNOWN;
        }
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
        scanSplitsForInference();
        return new TableStats(splitStatistics.totalRowCount());
    }

    @Override
    public List<String> listAcceptedFilterFields() {
        // note that streaming query doesn't support dynamic filtering
        return unbounded || PostponeMergeOnRead.configured(table)
                ? Collections.emptyList()
                : table.partitionKeys();
    }

    @Override
    public void applyDynamicFiltering(List<String> candidateFilterFields) {
        checkState(
                !unbounded,
                "Cannot apply dynamic filtering to Paimon table '%s' when streaming reading.",
                table.name());

        checkState(
                !PostponeMergeOnRead.configured(table),
                "Cannot apply dynamic filtering to Paimon table '%s' when postpone merge-on-read is enabled.",
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
