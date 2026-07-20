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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for query authorization handling in chain table scans. */
public class ChainGroupReadTableTest {

    @Test
    public void testBatchScanUsesLogicalQueryAuth() throws Exception {
        TableSchema schema = tableSchema();
        TableQueryAuthResult logicalAuth = maskingAuth("logical");
        CatalogEnvironment catalogEnvironment = catalogEnvironment(logicalAuth);
        PrimaryKeyFileStoreTable snapshotTable =
                table(schema, "snapshot", true, catalogEnvironment);
        PrimaryKeyFileStoreTable deltaTable = table(schema, "delta", true, catalogEnvironment);
        PrimaryKeyFileStoreTable rawSnapshotTable =
                table(schema, "snapshot", false, catalogEnvironment);
        PrimaryKeyFileStoreTable rawDeltaTable = table(schema, "delta", false, catalogEnvironment);
        when(snapshotTable.copy(anyMap())).thenReturn(rawSnapshotTable);
        when(deltaTable.copy(anyMap())).thenReturn(rawDeltaTable);
        ChainGroupReadTable chainTable = new ChainGroupReadTable(snapshotTable, deltaTable);

        BinaryRow partition = BinaryRow.singleColumn(1);
        QueryAuthSplit sourceSplit = queryAuthSplit(dataSplit(partition), maskingAuth("physical"));
        DataTableScan snapshotScan = mock(DataTableScan.class);
        when(snapshotScan.plan())
                .thenReturn(new DataFilePlan<>(Collections.singletonList(sourceSplit)));
        when(snapshotScan.listPartitions()).thenReturn(Collections.singletonList(partition));

        DataTableScan deltaScan = mock(DataTableScan.class);
        when(deltaScan.listPartitions()).thenReturn(Collections.emptyList());

        ChainGroupReadTable.ChainTableBatchScan scan =
                new ChainGroupReadTable.ChainTableBatchScan(
                        schema,
                        chainTable,
                        table -> table == rawSnapshotTable ? snapshotScan : deltaScan);
        List<Split> splits = scan.plan().splits();

        assertThat(splits).hasSize(1);
        QueryAuthSplit result = (QueryAuthSplit) splits.get(0);
        assertThat(result.authResult()).isSameAs(logicalAuth);
        assertThat(result.authResult()).isNotSameAs(sourceSplit.authResult());
        assertThat(result.split()).isInstanceOf(ChainSplit.class);
        verifyQueryAuthDisabled(snapshotTable);
        verifyQueryAuthDisabled(deltaTable);

        InnerTableRead snapshotRead = mock(InnerTableRead.class);
        InnerTableRead deltaRead = mock(InnerTableRead.class);
        RecordReader<InternalRow> reader = mock(RecordReader.class);
        when(snapshotTable.newRead()).thenReturn(snapshotRead);
        when(deltaTable.newRead()).thenReturn(deltaRead);
        when(deltaRead.createReader(result)).thenReturn(reader);

        assertThat(chainTable.newRead().createReader(result)).isSameAs(reader);
        verify(deltaRead).createReader(result);
    }

    @Test
    public void testBatchScanKeepsHistoricalAnchorOutsideAuthorizedPartition() {
        TableSchema schema = tableSchema();
        RowType rowType = schema.logicalRowType();
        TableQueryAuthResult logicalAuth =
                new TableQueryAuthResult(
                        Collections.singletonList(
                                JsonSerdeUtil.toFlatJson(
                                        new PredicateBuilder(rowType).equal(0, 20260720))),
                        null);
        CatalogEnvironment catalogEnvironment = catalogEnvironment(logicalAuth);
        PrimaryKeyFileStoreTable snapshotTable =
                table(schema, "snapshot", true, catalogEnvironment);
        PrimaryKeyFileStoreTable deltaTable = table(schema, "delta", true, catalogEnvironment);
        PrimaryKeyFileStoreTable rawSnapshotTable =
                table(schema, "snapshot", false, catalogEnvironment);
        PrimaryKeyFileStoreTable rawDeltaTable = table(schema, "delta", false, catalogEnvironment);
        when(snapshotTable.copy(anyMap())).thenReturn(rawSnapshotTable);
        when(deltaTable.copy(anyMap())).thenReturn(rawDeltaTable);

        BinaryRow historicalPartition = BinaryRow.singleColumn(20260719);
        BinaryRow authorizedPartition = BinaryRow.singleColumn(20260720);
        DataTableScan snapshotScan = mock(DataTableScan.class);
        when(snapshotScan.listPartitions())
                .thenReturn(Collections.singletonList(historicalPartition));
        when(snapshotScan.plan())
                .thenReturn(
                        new DataFilePlan<>(
                                Collections.singletonList(
                                        queryAuthSplit(
                                                dataSplit(historicalPartition),
                                                maskingAuth("snapshot")))));
        DataTableScan deltaScan = mock(DataTableScan.class);
        when(deltaScan.listPartitions()).thenReturn(Collections.singletonList(authorizedPartition));
        when(deltaScan.plan())
                .thenReturn(
                        new DataFilePlan<>(
                                Collections.singletonList(
                                        queryAuthSplit(
                                                dataSplit(authorizedPartition),
                                                maskingAuth("delta")))));

        ChainGroupReadTable chainTable = new ChainGroupReadTable(snapshotTable, deltaTable);
        ChainGroupReadTable.ChainTableBatchScan scan =
                new ChainGroupReadTable.ChainTableBatchScan(
                        schema,
                        chainTable,
                        table -> table == rawSnapshotTable ? snapshotScan : deltaScan);
        scan.skipPreloadTargetSnapshot();
        scan.withLimit(1);

        List<Split> splits = scan.plan().splits();

        assertThat(splits).hasSize(1);
        QueryAuthSplit result = (QueryAuthSplit) splits.get(0);
        assertThat(result.authResult()).isSameAs(logicalAuth);
        ChainSplit chainSplit = (ChainSplit) result.split();
        assertThat(chainSplit.logicalPartition()).isEqualTo(authorizedPartition);
        assertThat(chainSplit.dataFiles()).hasSize(2);
        verify(snapshotScan, never()).withLimit(anyInt());
        verify(deltaScan, never()).withLimit(anyInt());
        verifyQueryAuthDisabled(snapshotTable);
        verifyQueryAuthDisabled(deltaTable);
    }

    @Test
    public void testStreamStartingPlanRetainsQueryAuth() {
        testStreamStartingPlanRetainsQueryAuth(false);
    }

    @Test
    public void testStreamMergedStartingPlanRetainsQueryAuth() {
        testStreamStartingPlanRetainsQueryAuth(true);
    }

    private void testStreamStartingPlanRetainsQueryAuth(boolean mergeSnapshot) {
        TableSchema schema = tableSchema(mergeSnapshot);
        TableQueryAuthResult logicalAuth = maskingAuth("logical");
        CatalogEnvironment catalogEnvironment = catalogEnvironment(logicalAuth);
        PrimaryKeyFileStoreTable snapshotTable =
                table(schema, "snapshot", true, catalogEnvironment);
        PrimaryKeyFileStoreTable deltaTable = table(schema, "delta", true, catalogEnvironment);
        PrimaryKeyFileStoreTable rawSnapshotTable =
                table(schema, "snapshot", false, catalogEnvironment);
        PrimaryKeyFileStoreTable rawDeltaTable = table(schema, "delta", false, catalogEnvironment);

        DataTableScan unusedScan = mock(DataTableScan.class);
        when(rawSnapshotTable.newScan()).thenReturn(unusedScan);
        when(rawDeltaTable.newScan()).thenReturn(unusedScan);

        DataTableStreamScan deltaStreamScan = mock(DataTableStreamScan.class);
        when(rawDeltaTable.newStreamScan()).thenReturn(deltaStreamScan);

        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(snapshotManager.latestSnapshotId()).thenReturn(null);
        when(snapshotTable.snapshotManager()).thenReturn(snapshotManager);

        SnapshotManager deltaSnapshotManager = mock(SnapshotManager.class);
        when(deltaSnapshotManager.latestSnapshotId()).thenReturn(1L);
        when(deltaTable.snapshotManager()).thenReturn(deltaSnapshotManager);

        BinaryRow partition = BinaryRow.singleColumn(1);
        QueryAuthSplit sourceSplit = queryAuthSplit(dataSplit(partition), maskingAuth("physical"));
        DataTableScan pinnedDeltaScan = mock(DataTableScan.class);
        when(pinnedDeltaScan.plan())
                .thenReturn(new DataFilePlan<>(Collections.singletonList(sourceSplit)));
        PrimaryKeyFileStoreTable pinnedDeltaTable =
                table(schema, "delta", false, catalogEnvironment);
        when(pinnedDeltaTable.newScan()).thenReturn(pinnedDeltaScan);
        when(snapshotTable.copy(anyMap())).thenReturn(rawSnapshotTable);
        when(deltaTable.copy(anyMap()))
                .thenAnswer(
                        invocation -> {
                            Map<String, String> options = invocation.getArgument(0);
                            return options.containsKey(CoreOptions.SCAN_SNAPSHOT_ID.key())
                                    ? pinnedDeltaTable
                                    : rawDeltaTable;
                        });

        ChainTableStreamScan scan =
                new ChainTableStreamScan(new ChainGroupReadTable(snapshotTable, deltaTable));
        List<Split> splits = scan.plan().splits();

        assertThat(splits).hasSize(1);
        QueryAuthSplit result = (QueryAuthSplit) splits.get(0);
        assertThat(result.authResult()).isSameAs(logicalAuth);
        assertThat(result.authResult()).isNotSameAs(sourceSplit.authResult());
        assertThat(result.split()).isInstanceOf(ChainSplit.class);
        verify(deltaStreamScan).restore(2L);
        verifyQueryAuthDisabled(snapshotTable);
        verifyQueryAuthDisabled(deltaTable);
    }

    private static PrimaryKeyFileStoreTable table(
            TableSchema schema,
            String branch,
            boolean queryAuthEnabled,
            CatalogEnvironment catalogEnvironment) {
        PrimaryKeyFileStoreTable table = mock(PrimaryKeyFileStoreTable.class);
        when(table.schema()).thenReturn(schema);
        Map<String, String> options = new HashMap<>(schema.options());
        options.put(CoreOptions.BRANCH.key(), branch);
        options.put(CoreOptions.QUERY_AUTH_ENABLED.key(), String.valueOf(queryAuthEnabled));
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(options));
        when(table.catalogEnvironment()).thenReturn(catalogEnvironment);
        return table;
    }

    private static CatalogEnvironment catalogEnvironment(TableQueryAuthResult authResult) {
        CatalogEnvironment catalogEnvironment = mock(CatalogEnvironment.class);
        when(catalogEnvironment.tableQueryAuth(any(CoreOptions.class)))
                .thenReturn(select -> authResult);
        return catalogEnvironment;
    }

    private static void verifyQueryAuthDisabled(PrimaryKeyFileStoreTable table) {
        verify(table, atLeastOnce())
                .copy(
                        argThat(
                                (Map<String, String> options) ->
                                        "false"
                                                .equals(
                                                        options.get(
                                                                CoreOptions.QUERY_AUTH_ENABLED
                                                                        .key()))));
    }

    private static TableSchema tableSchema() {
        return tableSchema(false);
    }

    private static TableSchema tableSchema(boolean mergeSnapshot) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "pt", DataTypes.INT()),
                        new DataField(1, "k", DataTypes.INT()),
                        new DataField(2, "v", DataTypes.INT()));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot");
        options.put(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta");
        options.put(CoreOptions.QUERY_AUTH_ENABLED.key(), "true");
        options.put(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$pt");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd");
        options.put(
                CoreOptions.CHAIN_TABLE_STREAMING_MERGE_SNAPSHOT.key(),
                String.valueOf(mergeSnapshot));
        return new TableSchema(
                0,
                fields,
                2,
                Collections.singletonList("pt"),
                Arrays.asList("pt", "k"),
                options,
                "");
    }

    private static DataSplit dataSplit(BinaryRow partition) {
        return DataSplit.builder()
                .withSnapshot(1L)
                .withPartition(partition)
                .withBucket(0)
                .withBucketPath("pt=1/bucket-0")
                .withTotalBuckets(1)
                .withDataFiles(
                        Collections.singletonList(
                                DataFileTestDataGenerator.builder().build().next().meta))
                .build();
    }

    private static QueryAuthSplit queryAuthSplit(DataSplit split, TableQueryAuthResult authResult) {
        return new QueryAuthSplit(split, authResult);
    }

    private static TableQueryAuthResult maskingAuth(String value) {
        return new TableQueryAuthResult(null, Collections.singletonMap("v", value));
    }
}
