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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.flink.utils.TestingMetricUtils;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class StoreMultiCommitterTest {

    private String initialCommitUser;
    private Path warehouse;
    private Catalog.Loader catalogLoader;
    private Catalog catalog;
    private Identifier firstTable;
    private Identifier secondTable;
    private Path firstTablePath;
    private Path secondTablePath;
    @TempDir public java.nio.file.Path tempDir;

    @SafeVarargs
    private final void createTestTables(Catalog catalog, Tuple2<Identifier, Schema>... tableSpecs)
            throws Exception {
        for (Tuple2<Identifier, Schema> spec : tableSpecs) {
            catalog.createTable(spec.f0, spec.f1, false);
        }
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        initialCommitUser = UUID.randomUUID().toString();
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        String databaseName = "test_db";
        firstTable = Identifier.create(databaseName, "test_table1");
        secondTable = Identifier.create(databaseName, "test_table2");

        catalogLoader = createCatalogLoader();
        catalog = catalogLoader.load();
        catalog.createDatabase(databaseName, true);

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"a", "b"});
        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.VARCHAR(5),
                        },
                        new String[] {"a", "b", "c"});

        Options firstOptions = new Options();
        firstOptions.set(
                CoreOptions.TAG_AUTOMATIC_CREATION, CoreOptions.TagCreationMode.PROCESS_TIME);
        firstOptions.setString("bucket", "-1");
        Schema firstTableSchema =
                new Schema(
                        rowType1.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        firstOptions.toMap(),
                        "");

        Schema secondTableSchema =
                new Schema(
                        rowType2.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap("bucket", "1"),
                        "");
        createTestTables(
                catalog,
                Tuple2.of(firstTable, firstTableSchema),
                Tuple2.of(secondTable, secondTableSchema));
        firstTablePath = ((FileStoreTable) catalog.getTable(firstTable)).location();
        secondTablePath = ((FileStoreTable) catalog.getTable(secondTable)).location();
    }

    // ------------------------------------------------------------------------
    //  Recoverable operator tests
    // ------------------------------------------------------------------------

    @SuppressWarnings("CatchMayIgnoreException")
    @Test
    public void testFailIntentionallyAfterRestore() throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(firstTable);

        // write to first table
        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
                testHarness = createRecoverableTestHarness();
        testHarness.open();
        StreamTableWrite write =
                table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
        write.write(GenericRow.of(1, 10L));
        write.write(GenericRow.of(2, 20L));

        long timestamp = 1;
        for (CommitMessage committable : write.prepareCommit(false, 8)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            firstTable, new Committable(8, Committable.Kind.FILE, committable)),
                    timestamp++);
        }
        // checkpoint is completed but not notified, so no snapshot is committed
        OperatorSubtaskState snapshot = testHarness.snapshot(0, timestamp++);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();
        testHarness.close();

        testHarness = createRecoverableTestHarness();
        try {
            // commit snapshot from state, fail intentionally
            testHarness.initializeState(snapshot);
            testHarness.open();
            fail("Expecting intentional exception");
        } catch (Exception e) {
            assertThat(e)
                    .hasMessageContaining(
                            "This exception is intentionally thrown "
                                    + "after committing the restored checkpoints. "
                                    + "By restarting the job we hope that "
                                    + "writers can start writing based on these new commits.");
        }
        assertResultsForFirstTable(table, "1, 10", "2, 20");
        testHarness.close();

        // snapshot is successfully committed, no failure is needed
        testHarness = createRecoverableTestHarness();
        testHarness.initializeState(snapshot);
        testHarness.open();
        assertResultsForFirstTable(table, "1, 10", "2, 20");

        // write to second table
        table = (FileStoreTable) catalog.getTable(secondTable);

        write = table.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
        write.write(GenericRow.of(3, 30.0, BinaryString.fromString("s3")));
        write.write(GenericRow.of(4, 40.0, BinaryString.fromString("s4")));

        for (CommitMessage committable : write.prepareCommit(false, 9)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            secondTable, new Committable(9, Committable.Kind.FILE, committable)),
                    timestamp++);
        }

        // test restore and fail for second table
        // checkpoint is completed but not notified, so no snapshot is committed
        snapshot = testHarness.snapshot(1, timestamp);
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();
        testHarness.close();

        testHarness = createRecoverableTestHarness();
        try {
            // commit snapshot from state, fail intentionally
            testHarness.initializeState(snapshot);
            testHarness.open();
            fail("Expecting intentional exception");
        } catch (Exception e) {
            assertThat(e)
                    .hasMessageContaining(
                            "This exception is intentionally thrown "
                                    + "after committing the restored checkpoints. "
                                    + "By restarting the job we hope that "
                                    + "writers can start writing based on these new commits.");
        }
        assertResultsForSecondTable(table, "3, 30.0, s3", "4, 40.0, s4");
        testHarness.close();

        // snapshot is successfully committed, no failure is needed
        testHarness = createRecoverableTestHarness();
        testHarness.initializeState(snapshot);
        testHarness.open();
        assertResultsForSecondTable(table, "3, 30.0, s3", "4, 40.0, s4");
        testHarness.close();
    }

    @Test
    public void testCheckpointAbort() throws Exception {
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);
        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
                testHarness = createRecoverableTestHarness();
        testHarness.open();

        StreamTableWrite write1 =
                table1.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        StreamTableWrite write2 =
                table2.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        // only write to first table
        // files from multiple checkpoint
        // but no snapshot
        long cpId = 0;
        for (int i = 0; i < 10; i++) {
            cpId++;
            write1.write(GenericRow.of(1, 10L));
            write1.write(GenericRow.of(2, 20L));
            for (CommitMessage committable : write1.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        getMultiTableCommittable(
                                firstTable,
                                new Committable(cpId, Committable.Kind.FILE, committable)),
                        1);
            }
        }

        // checkpoint is completed but not notified, so no snapshot is committed
        testHarness.snapshot(cpId, 1);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        SnapshotManager snapshotManager1 =
                new SnapshotManager(LocalFileIO.create(), firstTablePath);
        SnapshotManager snapshotManager2 =
                new SnapshotManager(LocalFileIO.create(), secondTablePath);

        // should create 10 snapshots for first table
        assertThat(snapshotManager1.latestSnapshotId()).isEqualTo(cpId);
        // should not create snapshot for second table
        assertThat(snapshotManager2.latestSnapshotId()).isNull();

        // another 10 checkpoints where records are written to first and second table
        for (int i = 0; i < 10; i++) {
            cpId++;
            write1.write(GenericRow.of(3, 30L));
            write1.write(GenericRow.of(3, 40L));
            write2.write(GenericRow.of(3, 30.0, BinaryString.fromString("s3")));
            write2.write(GenericRow.of(3, 40.0, BinaryString.fromString("s4")));
            for (CommitMessage committable : write1.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        getMultiTableCommittable(
                                firstTable,
                                new Committable(cpId, Committable.Kind.FILE, committable)),
                        1);
            }

            for (CommitMessage committable : write2.prepareCommit(false, cpId)) {
                testHarness.processElement(
                        getMultiTableCommittable(
                                secondTable,
                                new Committable(cpId, Committable.Kind.FILE, committable)),
                        1);
            }
        }

        testHarness.snapshot(cpId, 2);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        // should create 20 snapshots in total for first table
        assertThat(snapshotManager1.latestSnapshotId()).isEqualTo(20);
        // should create 10 snapshots for second table
        assertThat(snapshotManager2.latestSnapshotId()).isEqualTo(10);
        testHarness.close();
    }

    // ------------------------------------------------------------------------
    //  Lossy operator tests
    // ------------------------------------------------------------------------

    @Test
    public void testSnapshotLostWhenFailed() throws Exception {
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);

        StreamTableWrite write1 =
                table1.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        StreamTableWrite write2 =
                table2.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
                testHarness = createLossyTestHarness();
        testHarness.open();

        long timestamp = 1;

        StreamWriteBuilder streamWriteBuilder1 =
                table1.newStreamWriteBuilder().withCommitUser(initialCommitUser);
        StreamWriteBuilder streamWriteBuilder2 =
                table2.newStreamWriteBuilder().withCommitUser(initialCommitUser);
        // this checkpoint is notified, should be committed
        write1.write(GenericRow.of(1, 10L));
        write1.write(GenericRow.of(2, 20L));
        for (CommitMessage committable : write1.prepareCommit(false, 1)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            firstTable, new Committable(1, Committable.Kind.FILE, committable)),
                    timestamp++);
        }
        testHarness.snapshot(1, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(1);

        // this checkpoint is not notified, should not be committed
        write1.write(GenericRow.of(3, 30L));
        write1.write(GenericRow.of(4, 40L));
        write2.write(GenericRow.of(3, 30.0, BinaryString.fromString("s3")));
        write2.write(GenericRow.of(3, 40.0, BinaryString.fromString("s4")));
        for (CommitMessage committable : write1.prepareCommit(false, 2)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            firstTable, new Committable(2, Committable.Kind.FILE, committable)),
                    timestamp++);
        }
        for (CommitMessage committable : write2.prepareCommit(false, 2)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            secondTable, new Committable(2, Committable.Kind.FILE, committable)),
                    timestamp++);
        }
        OperatorSubtaskState snapshot = testHarness.snapshot(2, timestamp++);

        // reopen test harness
        write1.close();
        write2.close();
        testHarness.close();

        testHarness = createLossyTestHarness();
        testHarness.initializeState(snapshot);
        testHarness.open();

        // this checkpoint is notified, should be committed
        write1 = streamWriteBuilder1.newWrite();
        write1.write(GenericRow.of(5, 50L));
        write1.write(GenericRow.of(6, 60L));
        write2 = streamWriteBuilder2.newWrite();
        write2.write(GenericRow.of(5, 50.0, BinaryString.fromString("s5")));
        write2.write(GenericRow.of(6, 60.0, BinaryString.fromString("s6")));
        for (CommitMessage committable : write1.prepareCommit(false, 3)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            firstTable, new Committable(3, Committable.Kind.FILE, committable)),
                    timestamp++);
        }
        for (CommitMessage committable : write2.prepareCommit(false, 2)) {
            testHarness.processElement(
                    getMultiTableCommittable(
                            secondTable, new Committable(2, Committable.Kind.FILE, committable)),
                    timestamp++);
        }
        testHarness.snapshot(3, timestamp);
        testHarness.notifyOfCompletedCheckpoint(3);

        write1.close();
        write2.close();
        testHarness.close();

        assertResultsForFirstTable(table1, "1, 10", "2, 20", "5, 50", "6, 60");
        assertResultsForSecondTable(table2, "5, 50.0, s5", "6, 60.0, s6");
    }

    @Test
    public void testWatermarkCommit() throws Exception {
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);

        StreamTableWrite write1 =
                table1.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        StreamTableWrite write2 =
                table2.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
                testHarness = createRecoverableTestHarness();
        testHarness.open();
        long timestamp = 0;
        long cpId = 1;

        // only write to first table on before first watermark
        write1.write(GenericRow.of(1, 10L));
        testHarness.processElement(
                getMultiTableCommittable(
                        firstTable,
                        new Committable(
                                cpId,
                                Committable.Kind.FILE,
                                write1.prepareCommit(true, cpId).get(0))),
                timestamp++);
        testHarness.processWatermark(new Watermark(1024));
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        assertThat(Objects.requireNonNull(table1.snapshotManager().latestSnapshot()).watermark())
                .isEqualTo(1024L);
        assertThat(table2.snapshotManager().latestSnapshot()).isNull();

        // write to both tables on second watermark
        cpId = 2;
        write1.write(GenericRow.of(1, 20L));
        write2.write(GenericRow.of(1, 20.0, BinaryString.fromString("s2")));
        testHarness.processElement(
                getMultiTableCommittable(
                        firstTable,
                        new Committable(
                                cpId,
                                Committable.Kind.FILE,
                                write1.prepareCommit(true, cpId).get(0))),
                timestamp++);
        testHarness.processWatermark(new Watermark(2048));
        testHarness.snapshot(cpId, timestamp);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        testHarness.close();
        assertThat(Objects.requireNonNull(table1.snapshotManager().latestSnapshot()).watermark())
                .isEqualTo(2048L);
        assertThat(Objects.requireNonNull(table1.snapshotManager().latestSnapshot()).watermark())
                .isEqualTo(2048L);
    }

    @Test
    public void testEmptyCommit() throws Exception {
        // table1: TagCreationMode.PROCESS_TIME
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);

        StreamTableWrite write1 =
                table1.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        StreamTableWrite write2 =
                table2.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
                testHarness = createRecoverableTestHarness();
        testHarness.open();

        // write to both tables
        long timestamp = 0;
        long cpId = 1;
        write1.write(GenericRow.of(1, 20L));
        write2.write(GenericRow.of(1, 20.0, BinaryString.fromString("s2")));
        testHarness.processElement(
                getMultiTableCommittable(
                        firstTable,
                        new Committable(
                                cpId,
                                Committable.Kind.FILE,
                                write1.prepareCommit(true, cpId).get(0))),
                timestamp++);
        testHarness.processElement(
                getMultiTableCommittable(
                        secondTable,
                        new Committable(
                                cpId,
                                Committable.Kind.FILE,
                                write2.prepareCommit(true, cpId).get(0))),
                timestamp++);
        testHarness.processWatermark(new Watermark(2048));
        testHarness.snapshot(cpId, timestamp++);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        assertThat(Objects.requireNonNull(table1.snapshotManager().latestSnapshot()).watermark())
                .isEqualTo(2048L);
        assertThat(Objects.requireNonNull(table2.snapshotManager().latestSnapshot()).watermark())
                .isEqualTo(2048L);

        cpId++;
        testHarness.snapshot(cpId, timestamp);
        testHarness.notifyOfCompletedCheckpoint(cpId);
        assertThat(Objects.requireNonNull(table1.snapshotManager().latestSnapshot()).id())
                .isEqualTo(1);
        assertThat(Objects.requireNonNull(table2.snapshotManager().latestSnapshot()).id())
                .isEqualTo(1);

        testHarness.close();
    }

    // ------------------------------------------------------------------------
    //  Metrics tests
    // ------------------------------------------------------------------------

    @Test
    public void testCommitMetrics() throws Exception {
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);

        StreamTableWrite write1 =
                table1.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();
        StreamTableWrite write2 =
                table2.newStreamWriteBuilder().withCommitUser(initialCommitUser).newWrite();

        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
                testHarness = createRecoverableTestHarness();
        testHarness.open();
        long timestamp = 0;
        long cpId = 1;

        write1.write(GenericRow.of(1, 10L));
        write2.write(GenericRow.of(1, 1.1, BinaryString.fromString("AAA")));
        write2.compact(BinaryRow.EMPTY_ROW, 0, false);
        write2.write(GenericRow.of(1, 1.2, BinaryString.fromString("aaa")));
        write2.compact(BinaryRow.EMPTY_ROW, 0, false);
        write2.write(GenericRow.of(2, 2.1, BinaryString.fromString("BBB")));
        write2.compact(BinaryRow.EMPTY_ROW, 0, true);
        testHarness.processElement(
                getMultiTableCommittable(
                        firstTable,
                        new Committable(
                                cpId,
                                Committable.Kind.FILE,
                                write1.prepareCommit(true, cpId).get(0))),
                timestamp++);
        testHarness.processElement(
                getMultiTableCommittable(
                        secondTable,
                        new Committable(
                                cpId,
                                Committable.Kind.FILE,
                                write2.prepareCommit(true, cpId).get(0))),
                timestamp++);
        testHarness.snapshot(cpId, timestamp);
        testHarness.notifyOfCompletedCheckpoint(cpId);

        OperatorMetricGroup operatorMetricGroup =
                testHarness.getOperator().getRuntimeContext().getMetricGroup();
        MetricGroup commitMetricGroup1 =
                operatorMetricGroup
                        .addGroup("paimon")
                        .addGroup("table", table1.name())
                        .addGroup("commit");
        MetricGroup commitMetricGroup2 =
                operatorMetricGroup
                        .addGroup("paimon")
                        .addGroup("table", table2.name())
                        .addGroup("commit");

        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup1, "lastTableFilesAdded")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup1, "lastTableFilesDeleted")
                                .getValue())
                .isEqualTo(0L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup1, "lastTableFilesAppended")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        TestingMetricUtils.getGauge(
                                        commitMetricGroup1, "lastTableFilesCommitCompacted")
                                .getValue())
                .isEqualTo(0L);

        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup2, "lastTableFilesAdded")
                                .getValue())
                .isEqualTo(4L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup2, "lastTableFilesDeleted")
                                .getValue())
                .isEqualTo(3L);
        assertThat(
                        TestingMetricUtils.getGauge(commitMetricGroup2, "lastTableFilesAppended")
                                .getValue())
                .isEqualTo(3L);
        assertThat(
                        TestingMetricUtils.getGauge(
                                        commitMetricGroup2, "lastTableFilesCommitCompacted")
                                .getValue())
                .isEqualTo(4L);

        testHarness.close();
        write1.close();
        write2.close();
    }

    // ------------------------------------------------------------------------
    //  Test utils
    // ------------------------------------------------------------------------

    private OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
            createRecoverableTestHarness() throws Exception {
        CommitterOperator<MultiTableCommittable, WrappedManifestCommittable> operator =
                new CommitterOperator<>(
                        true,
                        false,
                        initialCommitUser,
                        (user, metricGroup) ->
                                new StoreMultiCommitter(
                                        catalogLoader, initialCommitUser, metricGroup),
                        new RestoreAndFailCommittableStateManager<>(
                                () ->
                                        new VersionedSerializerWrapper<>(
                                                new WrappedManifestCommittableSerializer())));
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
            createLossyTestHarness() throws Exception {
        CommitterOperator<MultiTableCommittable, WrappedManifestCommittable> operator =
                new CommitterOperator<>(
                        true,
                        false,
                        initialCommitUser,
                        (user, metricGroup) ->
                                new StoreMultiCommitter(
                                        catalogLoader, initialCommitUser, metricGroup),
                        new CommittableStateManager<WrappedManifestCommittable>() {
                            @Override
                            public void initializeState(
                                    StateInitializationContext context,
                                    Committer<?, WrappedManifestCommittable> committer) {}

                            @Override
                            public void snapshotState(
                                    StateSnapshotContext context,
                                    List<WrappedManifestCommittable> committables) {}
                        });
        return createTestHarness(operator);
    }

    private OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable>
            createTestHarness(
                    CommitterOperator<MultiTableCommittable, WrappedManifestCommittable> operator)
                    throws Exception {
        TypeSerializer<MultiTableCommittable> serializer =
                new MultiTableCommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<MultiTableCommittable, MultiTableCommittable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, serializer);
        harness.setup(serializer);
        return harness;
    }

    private Catalog.Loader createCatalogLoader() {
        Options catalogOptions = createCatalogOptions(warehouse);
        return () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
    }

    private Options createCatalogOptions(Path warehouse) {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, warehouse.toString());
        conf.set(CatalogOptions.URI, "");

        return conf;
    }

    protected void assertResultsForFirstTable(FileStoreTable table, String... expected) {
        TableRead read = table.newReadBuilder().newRead();
        List<String> actual = new ArrayList<>();
        table.newReadBuilder()
                .newScan()
                .plan()
                .splits()
                .forEach(
                        s -> {
                            try {
                                RecordReader<InternalRow> recordReader = read.createReader(s);
                                CloseableIterator<InternalRow> it =
                                        new RecordReaderIterator<>(recordReader);
                                while (it.hasNext()) {
                                    InternalRow row = it.next();
                                    actual.add(row.getInt(0) + ", " + row.getLong(1));
                                }
                                it.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        Collections.sort(actual);
        assertThat(actual).isEqualTo(Arrays.asList(expected));
    }

    private void assertResultsForSecondTable(FileStoreTable table, String... expected) {
        TableRead read = table.newReadBuilder().newRead();
        List<String> actual = new ArrayList<>();
        table.newReadBuilder()
                .newScan()
                .plan()
                .splits()
                .forEach(
                        s -> {
                            try {
                                RecordReader<InternalRow> recordReader = read.createReader(s);
                                CloseableIterator<InternalRow> it =
                                        new RecordReaderIterator<>(recordReader);
                                while (it.hasNext()) {
                                    InternalRow row = it.next();
                                    actual.add(
                                            row.getInt(0)
                                                    + ", "
                                                    + row.getDouble(1)
                                                    + ", "
                                                    + row.getString(2));
                                }
                                it.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        Collections.sort(actual);
        assertThat(actual).isEqualTo(Arrays.asList(expected));
    }

    private MultiTableCommittable getMultiTableCommittable(
            Identifier tableId, Committable committable) {
        return MultiTableCommittable.fromCommittable(tableId, committable);
    }
}
