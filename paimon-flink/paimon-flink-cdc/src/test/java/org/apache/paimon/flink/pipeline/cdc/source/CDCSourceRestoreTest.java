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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.pipeline.cdc.CDCOptions;
import org.apache.paimon.flink.pipeline.cdc.source.enumerator.CDCCheckpoint;
import org.apache.paimon.flink.pipeline.cdc.source.reader.CDCSourceSplitReader;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.flink.pipeline.cdc.CDCOptions.toCDCOption;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for restoring the CDC source after removing tables while the job is stopped. */
public class CDCSourceRestoreTest {

    private static final String DATABASE = "default";
    private static final Identifier FIRST = Identifier.create(DATABASE, "first");
    private static final Identifier SECOND = Identifier.create(DATABASE, "second");

    @TempDir private Path tempDir;

    private Catalog catalog;
    private CDCSource source;

    @BeforeEach
    void beforeEach() throws Exception {
        Options options = new Options();
        options.setString("warehouse", tempDir.resolve("warehouse").toUri().toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        catalog = CatalogFactory.createCatalog(catalogContext);
        catalog.createDatabase(DATABASE, true);
        Schema schema =
                Schema.newBuilder()
                        .column("v", DataTypes.INT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "v")
                        .build();
        int value = 1;
        for (Identifier identifier : Arrays.asList(FIRST, SECOND)) {
            catalog.createTable(identifier, schema, false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.write(GenericRow.of(value++));
                commit.commit(write.prepareCommit());
            }
        }
        Configuration cdcConfig = new Configuration();
        cdcConfig.set(toCDCOption(CDCOptions.DATABASE), DATABASE);
        source =
                new CDCSource(
                        catalogContext,
                        cdcConfig,
                        new org.apache.flink.configuration.Configuration());
    }

    @AfterEach
    void afterEach() throws Exception {
        catalog.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRestorePendingSplitsAfterDroppingTables(boolean dropAll) throws Exception {
        CDCCheckpoint checkpoint = createCheckpoint();
        catalog.dropTable(FIRST, false);
        if (dropAll) {
            catalog.dropTable(SECOND, false);
        }
        TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context = newContext();
        try (SplitEnumerator<TableAwareFileStoreSourceSplit, CDCCheckpoint> restored =
                source.restoreEnumerator(context, checkpoint)) {
            CDCCheckpoint restoredCheckpoint = restored.snapshotState(2L);
            assertThat(restoredCheckpoint.getTableProgressMap()).doesNotContainKey(FIRST);
            if (dropAll) {
                assertThat(restoredCheckpoint.getSplits()).isEmpty();
                assertThat(restoredCheckpoint.getTableProgressMap()).isEmpty();
            } else {
                assertThat(restoredCheckpoint.getSplits())
                        .extracting(TableAwareFileStoreSourceSplit::getIdentifier)
                        .containsExactly(SECOND);
                restored.handleSplitRequest(0, "test-host");
                assertThat(context.getSplitAssignments().get(0).getAssignedSplits())
                        .containsExactlyElementsOf(restoredCheckpoint.getSplits());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRestoreAssignedSplitsAfterDroppingTable(boolean dropAll) throws Exception {
        CDCCheckpoint checkpoint = createCheckpoint();
        TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context = newContext();
        List<TableAwareFileStoreSourceSplit> assigned;
        try (SplitEnumerator<TableAwareFileStoreSourceSplit, CDCCheckpoint> enumerator =
                source.restoreEnumerator(context, checkpoint)) {
            enumerator.handleSplitRequest(0, "test-host");
            assigned = context.getSplitAssignments().get(0).getAssignedSplits();
        }
        assertThat(assigned).hasSize(2);
        SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> serializer =
                source.getSplitSerializer();
        List<TableAwareFileStoreSourceSplit> restoredSplits = new ArrayList<>();
        for (TableAwareFileStoreSourceSplit split : assigned) {
            restoredSplits.add(
                    serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
        }
        TableAwareFileStoreSourceSplit dropped = restoredSplits.get(0);
        TableAwareFileStoreSourceSplit retained = restoredSplits.get(1);
        catalog.dropTable(dropped.getIdentifier(), false);
        if (dropAll) {
            catalog.dropTable(retained.getIdentifier(), false);
        }
        SourceReaderMetricGroup metricGroup =
                UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        try (IOManager ioManager = IOManager.create(tempDir.resolve("spill").toString());
                CDCSourceSplitReader reader =
                        new CDCSourceSplitReader(
                                new FileStoreSourceReaderMetrics(metricGroup),
                                new CDCSource.TableManager(catalog, ioManager, metricGroup))) {
            reader.handleSplitsChanges(new SplitsAddition<>(restoredSplits));
            RecordsWithSplitIds<RecordIterator<Event>> skipped = reader.fetch();
            assertThat(skipped.finishedSplits()).containsExactly(dropped.splitId());
            assertThat(skipped.nextSplit()).isNull();
            skipped.recycle();

            RecordsWithSplitIds<RecordIterator<Event>> records = reader.fetch();
            if (dropAll) {
                assertThat(records.finishedSplits()).containsExactly(retained.splitId());
                assertThat(records.nextSplit()).isNull();
                records.recycle();
                return;
            }
            assertThat(records.nextSplit()).isEqualTo(retained.splitId());
            RecordIterator<Event> iterator = records.nextRecordFromSplit();
            List<DataChangeEvent> changes = new ArrayList<>();
            RecordAndPosition<Event> record;
            while ((record = iterator.next()) != null) {
                if (record.getRecord() instanceof DataChangeEvent) {
                    changes.add((DataChangeEvent) record.getRecord());
                }
            }
            records.recycle();
            assertThat(changes).hasSize(1);
            assertThat(changes.get(0).tableId())
                    .isEqualTo(TableId.tableId(DATABASE, retained.getIdentifier().getObjectName()));
            assertThat(changes.get(0).after().getInt(0))
                    .isEqualTo(retained.getIdentifier().equals(FIRST) ? 1 : 2);
        }
    }

    private CDCCheckpoint createCheckpoint() throws Exception {
        TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context = newContext();
        CDCCheckpoint checkpoint;
        try (SplitEnumerator<TableAwareFileStoreSourceSplit, CDCCheckpoint> enumerator =
                source.createEnumerator(context)) {
            enumerator.start();
            context.triggerAllActions();
            context.triggerAllActions();
            checkpoint = enumerator.snapshotState(1L);
        }
        assertThat(checkpoint.getSplits()).hasSize(2);
        assertThat(checkpoint.getTableProgressMap()).containsOnlyKeys(FIRST, SECOND);
        SimpleVersionedSerializer<CDCCheckpoint> serializer =
                source.getEnumeratorCheckpointSerializer();
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));
    }

    private TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> newContext() {
        TestingSplitEnumeratorContext<TableAwareFileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "test-host");
        return context;
    }
}
