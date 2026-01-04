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

package org.apache.paimon.flink.pipeline.cdc.source.enumerator;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.pipeline.cdc.CDCOptions;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.flink.source.FileSplitEnumeratorTestBase;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataFilePlan;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.toCDCOption;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link CDCSourceEnumerator}. */
public class CDCSourceEnumeratorTest
        extends FileSplitEnumeratorTestBase<TableAwareFileStoreSourceSplit> {
    @TempDir public java.nio.file.Path warehouseFolder;

    private static final String DATABASE = "default";
    private static final String TABLE = "test_table";
    private static final int NUM_TABLES = 20;
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .option("bucket", String.valueOf(NUM_TABLES))
                    .option("bucket-key", "a")
                    .build();

    private Catalog catalog;

    @BeforeEach
    public void beforeEach() throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouseFolder.toAbsolutePath().toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        catalog = CatalogFactory.createCatalog(catalogContext);
        catalog.createDatabase(DATABASE, true);
        for (int i = 0; i < NUM_TABLES; i++) {
            catalog.createTable(Identifier.create(DATABASE, TABLE + i), SCHEMA, false);
        }
    }

    @Test
    public void testSplitAllocationIsOrdered() {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

        List<TableAwareFileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            initialSplits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        List<TableAwareFileStoreSourceSplit> expectedSplits = new ArrayList<>(initialSplits);
        final CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .withSplitMaxPerTask(1)
                        .build();

        // The first time split is allocated, split1 and split2 should be allocated
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        Map<
                        Integer,
                        TestingSplitEnumeratorContext.SplitAssignmentState<
                                TableAwareFileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        List<TableAwareFileStoreSourceSplit> assignedSplits =
                assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(0, 2));

        // split1 and split2 is added back
        enumerator.addSplitsBack(assignedSplits, 0);
        context.getSplitAssignments().clear();
        assertThat(context.getSplitAssignments()).isEmpty();

        // The split is allocated for the second time, and split1 is allocated first
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(0, 2));

        // continuing to allocate split
        context.getSplitAssignments().clear();
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(2, 4));
    }

    @Test
    public void testSplitWithBatch() {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

        List<TableAwareFileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 18; i++) {
            initialSplits.add(createSnapshotSplit(i, i, Collections.emptyList()));
        }
        final CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .withSplitMaxPerTask(1)
                        .build();

        // The first time split is allocated, split1 and split2 should be allocated
        enumerator.handleSplitRequest(0, "test-host");
        Map<
                        Integer,
                        TestingSplitEnumeratorContext.SplitAssignmentState<
                                TableAwareFileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(1);

        // test second batch assign
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(2);

        // test third batch assign
        enumerator.handleSplitRequest(0, "test-host");

        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).hasSize(3);
    }

    @Test
    public void testSplitAllocationIsFair() {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

        List<TableAwareFileStoreSourceSplit> initialSplits = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            initialSplits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
            initialSplits.add(createSnapshotSplit(i, 1, Collections.emptyList()));
        }

        List<TableAwareFileStoreSourceSplit> expectedSplits = new ArrayList<>(initialSplits);

        final CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(initialSplits)
                        .setDiscoveryInterval(3)
                        .withSplitMaxPerTask(1)
                        .build();

        // each time a split is allocated from bucket-0 and bucket-1
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        Map<
                        Integer,
                        TestingSplitEnumeratorContext.SplitAssignmentState<
                                TableAwareFileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        List<TableAwareFileStoreSourceSplit> assignedSplits =
                assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(0, 2));

        // clear assignments
        context.getSplitAssignments().clear();
        assertThat(context.getSplitAssignments()).isEmpty();

        // continuing to allocate the rest splits
        enumerator.handleSplitRequest(0, "test-host");
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        // Only subtask-0 is allocated.
        assertThat(assignments).containsOnlyKeys(0);
        assignedSplits = assignments.get(0).getAssignedSplits();
        assertThat(assignedSplits).hasSameElementsAs(expectedSplits.subList(2, 4));
    }

    @Test
    public void testRemoveReadersAwaitSuccessful() {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        StreamDataTableScan scan = new MockScan(results);
        CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .withSplitMaxPerTask(1)
                        .unawareBucket(true)
                        .build();
        enumerator.start();
        enumerator.handleSplitRequest(1, "test-host");

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));

        context.registeredReaders().remove(1);
        // assign to task 0
        assertThatCode(() -> enumerator.handleSplitRequest(0, "test-host"))
                .doesNotThrowAnyException();
    }

    @Test
    public void testSnapshotEnumerator() {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        MockScan scan = new MockScan(results);
        CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .withSplitMaxPerTask(1)
                        .build();
        enumerator.start();

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            splits.add(createDataSplit(snapshot, i, Collections.emptyList()));
        }
        results.put(1L, new DataFilePlan(splits));
        context.triggerAllActions();

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        Map<
                        Integer,
                        TestingSplitEnumeratorContext.SplitAssignmentState<
                                TableAwareFileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0));

        // assign to task 0
        enumerator.handleSplitRequest(0, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0), splits.get(1));

        // always assign same table split to same task
        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsKey(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0), splits.get(1));
    }

    @Test
    public void testAssignSameTableSplitToSameTask() {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(2);

        TreeMap<Long, TableScan.Plan> results = new TreeMap<>();
        MockScan scan = new MockScan(results);
        CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(1)
                        .setScan(scan)
                        .withSplitMaxPerTask(1)
                        .build();
        enumerator.start();

        long snapshot = 0;
        List<DataSplit> splits = new ArrayList<>();
        splits.add(createDataSplit(snapshot, 0, Collections.emptyList()));
        results.put(1L, new DataFilePlan(splits));
        context.triggerAllActions();

        enumerator.handleSplitRequest(0, "test-host");
        Map<
                        Integer,
                        TestingSplitEnumeratorContext.SplitAssignmentState<
                                TableAwareFileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0));

        splits.add(createDataSplit(snapshot, 1, Collections.emptyList()));
        results.put(1L, new DataFilePlan(splits.subList(1, 2)));
        context.triggerAllActions();

        enumerator.handleSplitRequest(1, "test-host");
        assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0, 1);
        assertThat(toDataSplits(assignments.get(0).getAssignedSplits()))
                .containsExactly(splits.get(0));
        assertThat(toDataSplits(assignments.get(1).getAssignedSplits()))
                .containsExactly(splits.get(1));
    }

    @Test
    public void testEnumeratorSnapshotState() throws Exception {
        final TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                getSplitEnumeratorContext(1);

        final CDCSourceEnumerator enumerator =
                new Builder()
                        .setSplitEnumeratorContext(context)
                        .setInitialSplits(Collections.emptyList())
                        .setDiscoveryInterval(3)
                        .withSplitMaxPerTask(1)
                        .build();

        List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            Identifier identifier = Identifier.create(DATABASE, TABLE + 0);
            FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
            enumerator.setScan(identifier, fileStoreTable, new MockScan(new TreeMap<>()));
            splits.add(createSnapshotSplit(i, 0, Collections.emptyList()));
        }
        enumerator.addSplits(splits);
        enumerator.handleSplitRequest(0, "test-host");

        Map<
                        Integer,
                        TestingSplitEnumeratorContext.SplitAssignmentState<
                                TableAwareFileStoreSourceSplit>>
                assignments = context.getSplitAssignments();
        assertThat(assignments).containsOnlyKeys(0);
        assertThat(assignments.get(0).getAssignedSplits()).containsExactly(splits.get(0));
        CDCCheckpoint checkpoint = enumerator.snapshotState(1L);
        assertThat(checkpoint.getSplits()).containsExactly(splits.get(1));
    }

    private class Builder {
        protected SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context;
        protected Collection<TableAwareFileStoreSourceSplit> initialSplits =
                Collections.emptyList();
        protected long discoveryInterval = Long.MAX_VALUE;

        protected StreamDataTableScan scan;
        protected boolean unawareBucket = false;

        protected int splitMaxPerTask = 10;

        public Builder setSplitEnumeratorContext(
                SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context) {
            this.context = context;
            return this;
        }

        public Builder setInitialSplits(Collection<TableAwareFileStoreSourceSplit> initialSplits) {
            this.initialSplits = initialSplits;
            return this;
        }

        public Builder setDiscoveryInterval(long discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
            return this;
        }

        public Builder setScan(StreamDataTableScan scan) {
            this.scan = scan;
            return this;
        }

        public Builder unawareBucket(boolean unawareBucket) {
            this.unawareBucket = unawareBucket;
            return this;
        }

        public Builder withSplitMaxPerTask(int splitMaxPerTask) {
            this.splitMaxPerTask = splitMaxPerTask;
            return this;
        }

        public CDCSourceEnumerator build() {
            Options options = new Options();
            options.setString("warehouse", warehouseFolder.toAbsolutePath().toString());
            options.set(CoreOptions.SCAN_MAX_SPLITS_PER_TASK, splitMaxPerTask);
            Configuration cdcConfig = new Configuration();
            cdcConfig.set(toCDCOption(CDCOptions.DATABASE), DATABASE);

            Map<Identifier, Long> nextSnapshotIdMap = new HashMap<>();
            for (TableAwareFileStoreSourceSplit split : initialSplits) {
                nextSnapshotIdMap.put(split.getIdentifier(), 1L);
            }

            TestCDCSourceEnumerator enumerator;
            try {
                enumerator =
                        new TestCDCSourceEnumerator(
                                context,
                                new org.apache.flink.configuration.Configuration(),
                                discoveryInterval,
                                CatalogContext.create(options),
                                cdcConfig,
                                new CDCCheckpoint(initialSplits, nextSnapshotIdMap));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (scan != null) {
                try {
                    for (String tableName : catalog.listTables(DATABASE)) {
                        FileStoreTable table =
                                (FileStoreTable)
                                        catalog.getTable(Identifier.create(DATABASE, tableName));
                        enumerator.setScan(Identifier.create(DATABASE, tableName), table, scan);
                    }
                } catch (Catalog.DatabaseNotExistException | Catalog.TableNotExistException e) {
                    throw new RuntimeException(e);
                }
            }
            return enumerator;
        }
    }

    @Override
    protected TableAwareFileStoreSourceSplit createSnapshotSplit(
            int snapshotId, int bucket, List<DataFileMeta> files, int... partitions) {
        Identifier identifier = Identifier.create(DATABASE, TABLE + bucket);
        return new TableAwareFileStoreSourceSplit(
                UUID.randomUUID().toString(),
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(row(partitions))
                        .withBucket(bucket)
                        .withDataFiles(files)
                        .isStreaming(true)
                        .withBucketPath("/temp/xxx") // not used
                        .build(),
                0,
                identifier,
                null,
                1L);
    }

    private static class TestCDCSourceEnumerator extends CDCSourceEnumerator {

        public TestCDCSourceEnumerator(
                SplitEnumeratorContext<TableAwareFileStoreSourceSplit> context,
                org.apache.flink.configuration.Configuration flinkConfig,
                long discoveryInterval,
                CatalogContext catalogContext,
                Configuration cdcConfig,
                @Nullable CDCCheckpoint checkpoint) {
            super(context, flinkConfig, discoveryInterval, catalogContext, cdcConfig, checkpoint);
        }

        @Override
        protected TableAwareFileStoreSourceSplit toTableAwareSplit(
                String splitId,
                Split split,
                FileStoreTable table,
                Identifier identifier,
                @Nullable Long lastSchemaId) {
            Preconditions.checkState(split instanceof DataSplit);
            return new TableAwareFileStoreSourceSplit(
                    splitId, split, 0, identifier, lastSchemaId, 1L);
        }
    }
}
