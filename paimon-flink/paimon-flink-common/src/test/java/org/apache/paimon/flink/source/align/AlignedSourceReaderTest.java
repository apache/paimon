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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import static org.apache.paimon.CoreOptions.CONSUMER_ID;
import static org.apache.paimon.flink.FlinkConnectorOptions.CheckpointAlignMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link AlignedSourceReader}. */
public class AlignedSourceReaderTest {

    private static final Predicate<CompletableFuture<Void>> UNCOMPLETED_PREDICATE =
            future -> !future.isDone();

    @TempDir Path tempDir;

    private FileStoreTable table;

    @BeforeEach
    public void before()
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableNotExistException, Catalog.DatabaseAlreadyExistException {
        Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new org.apache.paimon.fs.Path(tempDir.toUri())));
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .primaryKey("a")
                        .options(Collections.singletonMap(CONSUMER_ID.key(), "my_consumer"))
                        .build();
        Identifier identifier = Identifier.create("default", "t");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        this.table = (FileStoreTable) catalog.getTable(identifier);
    }

    @Test
    public void testStrictlyAlignMode() throws Exception {
        ManuallyTriggeredScheduledExecutorService executors =
                new ManuallyTriggeredScheduledExecutorService();
        AlignedSourceReader reader =
                createAndStartReader(
                        CheckpointAlignMode.STRICTLY, executors, Collections.emptyList());
        StreamTableScan scan = table.newStreamScan();
        TestingCollectReaderOutput<Split> output = new TestingCollectReaderOutput<>();

        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                Collections.emptyList(),
                null,
                UNCOMPLETED_PREDICATE);

        // assume checkpoint arrives first
        reader.handleSourceEvents(new CheckpointEvent(1L));
        assertThat(reader.shouldTriggerCheckpoint()).isEmpty();

        // write 1 snapshot and scan the first snapshot
        writeAndScanForReader(1L, 10L, GenericRow.of(1, 2, 3), executors);
        List<Split> firstSplits = scan.plan().splits();

        // send the first snapshot
        CompletableFuture<Void> availableFuture = reader.isAvailable();
        assertThat(availableFuture).isDone();
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                firstSplits,
                new Watermark(10L),
                UNCOMPLETED_PREDICATE);

        // after send the first snapshot, we can trigger checkpoint
        assertThat(reader.shouldTriggerCheckpoint()).isEqualTo(Optional.of(1L));
        List<AlignedSourceSplit> state = reader.snapshotState(1L);
        assertThat(state)
                .containsExactly(new AlignedSourceSplit(Collections.emptyList(), 2L, null, true));
        reader.notifyCheckpointComplete(1L);

        // write 2 snapshots and scan the remaining snapshot
        writeAndScanForReader(2L, 20L, GenericRow.of(4, 5, 6), executors);
        writeAndScanForReader(3L, 30L, GenericRow.of(7, 8, 9), executors);
        List<Split> secondSplits = scan.plan().splits();
        List<Split> thirdSplits = scan.plan().splits();

        // assume checkpoint arrives late.
        availableFuture = reader.isAvailable();
        assertThat(availableFuture).isDone();
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                secondSplits,
                new Watermark(20L),
                UNCOMPLETED_PREDICATE);

        // no checkpoint is triggered and no more snapshots are expected to be sent.
        availableFuture = reader.isAvailable();
        assertThat(availableFuture).isNotDone();
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                Collections.emptyList(),
                null,
                UNCOMPLETED_PREDICATE);

        // trigger checkpoint
        reader.handleSourceEvents(new CheckpointEvent(2L));
        assertThat(reader.shouldTriggerCheckpoint()).isEqualTo(Optional.of(2L));
        state = reader.snapshotState(2L);
        assertThat(state).containsExactly(new AlignedSourceSplit(thirdSplits, 4L, 30L, false));
        reader.notifyCheckpointComplete(2L);

        // after checkpoint triggered, we can poll last snapshot
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                thirdSplits,
                new Watermark(30L),
                UNCOMPLETED_PREDICATE);
    }

    @Test
    public void testLooselyAlignMode() throws Exception {
        ManuallyTriggeredScheduledExecutorService executors =
                new ManuallyTriggeredScheduledExecutorService();
        AlignedSourceReader reader =
                createAndStartReader(
                        CheckpointAlignMode.LOOSELY, executors, Collections.emptyList());
        StreamTableScan scan = table.newStreamScan();
        TestingCollectReaderOutput<Split> output = new TestingCollectReaderOutput<>();

        // first call
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                Collections.emptyList(),
                null,
                UNCOMPLETED_PREDICATE);

        // checkpoint arrives first
        reader.handleSourceEvents(new CheckpointEvent(1L));
        assertThat(reader.shouldTriggerCheckpoint()).isEqualTo(Optional.of(1L));
        List<AlignedSourceSplit> state = reader.snapshotState(1L);
        assertThat(state).isEmpty();
        reader.notifyCheckpointComplete(1L);

        // write 1 snapshot and scan the first snapshot
        writeAndScanForReader(1L, 10L, GenericRow.of(1, 2, 3), executors);
        List<Split> firstSplits = scan.plan().splits();

        // send the first snapshot
        CompletableFuture<Void> availableFuture = reader.isAvailable();
        assertThat(availableFuture).isDone();
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                firstSplits,
                new Watermark(10L),
                UNCOMPLETED_PREDICATE);

        // write 2 snapshots and scan the remaining snapshot
        writeAndScanForReader(2L, 20L, GenericRow.of(4, 5, 6), executors);
        writeAndScanForReader(3L, 30L, GenericRow.of(7, 8, 9), executors);
        List<Split> secondSplits = scan.plan().splits();
        List<Split> thirdSplits = scan.plan().splits();

        // send the second snapshot
        availableFuture = reader.isAvailable();
        assertThat(availableFuture).isDone();
        pollNextAndCheck(
                reader,
                output,
                InputStatus.MORE_AVAILABLE,
                secondSplits,
                new Watermark(20L),
                CompletableFuture::isDone);

        // send the third snapshot
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                thirdSplits,
                new Watermark(30L),
                UNCOMPLETED_PREDICATE);

        // trigger checkpoint
        reader.handleSourceEvents(new CheckpointEvent(2L));
        assertThat(reader.shouldTriggerCheckpoint()).isEqualTo(Optional.of(2L));
        state = reader.snapshotState(2L);
        assertThat(state)
                .containsExactly(new AlignedSourceSplit(Collections.emptyList(), 4L, null, true));
    }

    @ParameterizedTest
    @EnumSource(
            value = CheckpointAlignMode.class,
            names = {"UNALIGNED"},
            mode = EnumSource.Mode.EXCLUDE)
    public void testRestore(CheckpointAlignMode alignMode) throws Exception {
        ManuallyTriggeredScheduledExecutorService executors =
                new ManuallyTriggeredScheduledExecutorService();
        AlignedSourceReader reader =
                createAndStartReader(
                        CheckpointAlignMode.STRICTLY, executors, Collections.emptyList());
        StreamTableScan scan = table.newStreamScan();
        TestingCollectReaderOutput<Split> output = new TestingCollectReaderOutput<>();

        // write a snapshot and scan the first snapshot
        writeAndScanForReader(1L, 10L, GenericRow.of(1, 2, 3), executors);
        List<Split> firstSplits = scan.plan().splits();

        // send the first snapshot
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                firstSplits,
                new Watermark(10L),
                UNCOMPLETED_PREDICATE);

        // trigger first checkpoint and shutdown
        List<AlignedSourceSplit> state =
                triggerCheckpoint(
                        reader,
                        1L,
                        Collections.singletonList(
                                new AlignedSourceSplit(Collections.emptyList(), 2L, null, true)));
        reader.close();

        // restore from the first checkpoint which contain a placeholder state
        executors = new ManuallyTriggeredScheduledExecutorService();
        reader = createAndStartReader(alignMode, executors, state);

        // first call with empty splits
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                Collections.emptyList(),
                null,
                UNCOMPLETED_PREDICATE);

        // write 2 snapshots and scan the remaining snapshots
        writeAndScanForReader(2L, 20L, GenericRow.of(4, 5, 6), executors);
        writeAndScanForReader(3L, 30L, GenericRow.of(7, 8, 9), executors);
        List<Split> secondSplits = scan.plan().splits();
        List<Split> thirdSplits = scan.plan().splits();

        // poll second snapshot
        InputStatus expectedStatus =
                alignMode == CheckpointAlignMode.STRICTLY
                        ? InputStatus.NOTHING_AVAILABLE
                        : InputStatus.MORE_AVAILABLE;
        Predicate<CompletableFuture<Void>> expectedPredicate =
                alignMode == CheckpointAlignMode.STRICTLY
                        ? UNCOMPLETED_PREDICATE
                        : CompletableFuture::isDone;
        pollNextAndCheck(
                reader,
                output,
                expectedStatus,
                secondSplits,
                new Watermark(20L),
                expectedPredicate);

        // trigger second checkpoint and shutdown
        state =
                triggerCheckpoint(
                        reader,
                        2L,
                        Collections.singletonList(
                                new AlignedSourceSplit(thirdSplits, 4L, 30L, false)));
        reader.close();

        // restore from second checkpoint
        executors = new ManuallyTriggeredScheduledExecutorService();
        reader = createAndStartReader(alignMode, executors, state);

        // poll third snapshot
        pollNextAndCheck(
                reader,
                output,
                InputStatus.NOTHING_AVAILABLE,
                thirdSplits,
                new Watermark(30L),
                UNCOMPLETED_PREDICATE);
    }

    private AlignedSourceReader createAndStartReader(
            CheckpointAlignMode alignMode,
            ScheduledExecutorService executors,
            List<AlignedSourceSplit> restoredSplits) {
        AlignedSourceReader reader =
                new AlignedSourceReader(table.newReadBuilder(), 10, true, alignMode, executors);
        reader.addSplits(restoredSplits);
        reader.start();
        return reader;
    }

    private void pollNextAndCheck(
            AlignedSourceReader reader,
            TestingCollectReaderOutput<Split> output,
            InputStatus expectedStatus,
            List<Split> expectedRecords,
            @Nullable Watermark expectedWatermark,
            Predicate<CompletableFuture<Void>> availablePredicate)
            throws Exception {
        output.clear();
        InputStatus status = reader.pollNext(output);
        assertThat(status).isEqualTo(expectedStatus);
        assertThat(output.getEmittedRecords()).containsExactlyInAnyOrderElementsOf(expectedRecords);
        if (expectedWatermark != null) {
            assertThat(output.getEmittedWatermarks()).containsExactly(expectedWatermark);
        } else {
            assertThat(output.getEmittedWatermarks()).isEmpty();
        }
        assertThat(reader.isAvailable()).matches(availablePredicate);
    }

    private List<AlignedSourceSplit> triggerCheckpoint(
            AlignedSourceReader reader, long checkpointId, List<AlignedSourceSplit> expectedState)
            throws Exception {
        reader.handleSourceEvents(new CheckpointEvent(checkpointId));
        assertThat(reader.shouldTriggerCheckpoint()).isEqualTo(Optional.of(checkpointId));
        List<AlignedSourceSplit> state = reader.snapshotState(checkpointId);
        assertThat(state).containsExactlyElementsOf(expectedState);
        reader.notifyCheckpointComplete(checkpointId);
        return state;
    }

    private void writeAndScanForReader(
            long identifier,
            long watermark,
            InternalRow row,
            ManuallyTriggeredScheduledExecutorService executors)
            throws Exception {
        writeToTable(identifier, watermark, row);
        executors.triggerScheduledTasks();
    }

    private void writeToTable(long identifier, long watermark, InternalRow row) throws Exception {
        TableWriteImpl<?> write = table.newWrite("commitUser");
        TableCommitImpl commit = table.newCommit("commitUser");
        write.write(row);
        ManifestCommittable committable = new ManifestCommittable(identifier, watermark);
        write.prepareCommit(true, 0).forEach(committable::addFileCommittable);
        commit.commit(committable);
        commit.close();
        write.close();
    }

    /** A {@code ReaderOutput} for testing that collects the emitted records and watermarks. */
    private static final class TestingCollectReaderOutput<E> extends TestingReaderOutput<E> {

        private final ArrayList<Watermark> emittedWatermarks = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            emittedWatermarks.add(watermark);
        }

        public ArrayList<Watermark> getEmittedWatermarks() {
            return emittedWatermarks;
        }

        public void clear() {
            super.clearEmittedRecords();
            emittedWatermarks.clear();
        }
    }
}
