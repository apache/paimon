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

package org.apache.paimon.flink;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.FixedBucketSink;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.flink.utils.InternalTypeInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests writer metadata restoration after a task failure in a running Flink job. */
public class WriterCoordinatorRecoveryITCase extends AbstractTestBase {

    private static final Map<String, SourceControl> CONTROLS = new ConcurrentHashMap<>();

    @ParameterizedTest(name = "writerCoordinator = {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(180)
    public void testReplayedUpdateAfterCheckpointRecovery(
            boolean writerCoordinator, @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        FileStoreTable table = createTable(writerCoordinator);
        StreamWriteBuilder builder = table.newStreamWriteBuilder();
        try (StreamTableWrite write = builder.newWrite();
                StreamTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(1L, 3));
            commit.commit(0, write.prepareCommit(false, 0));
        }

        String sourceId = UUID.randomUUID().toString();
        SourceControl control = new SourceControl();
        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        // Only the two explicitly triggered checkpoints may refresh the coordinator's cache.
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1));
        DataStream<InternalRow> source =
                env.fromSource(
                        new ReplayingSource(sourceId),
                        WatermarkStrategy.noWatermarks(),
                        "replaying-source",
                        InternalTypeInfo.fromRowType(table.rowType()));
        new FixedBucketSink(table, null).sinkFrom(source);
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        // collect() must return only after the writer has processed each replayed record, before
        // checkpoint 2 refreshes the coordinator. A shuffle would make that ordering racy.
        assertThat(jobGraph.getVertices())
                .anySatisfy(
                        vertex ->
                                assertThat(vertex.getName())
                                        .contains("replaying-source", "Writer"));
        boolean submitted = false;
        CONTROLS.put(sourceId, control);
        try {
            miniCluster.submitJob(jobGraph).get(60, TimeUnit.SECONDS);
            submitted = true;
            control.initialRowsWritten.get(60, TimeUnit.SECONDS);
            miniCluster.triggerCheckpoint(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
            awaitSnapshot(table, 2);
            assertVersion(table, 4);

            // The first attempt writes the change without checkpointing it and then fails. The
            // same source restores position 21 and replays it while the coordinator stays alive.
            control.failSource.complete(null);
            assertThat(control.restoredPosition.get(60, TimeUnit.SECONDS)).isEqualTo(21);
            control.replayedRowsWritten.get(60, TimeUnit.SECONDS);
            miniCluster.triggerCheckpoint(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
            awaitSnapshot(table, 3);
            assertVersion(table, 5);
        } finally {
            try {
                if (submitted) {
                    miniCluster.cancelJob(jobGraph.getJobID()).get(30, TimeUnit.SECONDS);
                }
            } finally {
                CONTROLS.remove(sourceId);
            }
        }
    }

    private FileStoreTable createTable(boolean writerCoordinator) throws Exception {
        Path path = new Path(getTempDirPath());
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("version", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("bucket-key", "id")
                        .option("merge-engine", "deduplicate")
                        .option("write-only", "true")
                        .option("changelog-producer", "none")
                        .option("file.format", "parquet")
                        .option(
                                "sink.writer-coordinator.enabled",
                                String.valueOf(writerCoordinator))
                        .build();
        new FileSystemSchemaManager(LocalFileIO.create(), path).createTable(schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), path);
    }

    private static void awaitSnapshot(FileStoreTable table, long snapshotId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            Long latest = table.snapshotManager().latestSnapshotId();
            if (latest != null && latest >= snapshotId) {
                assertThat(latest).isEqualTo(snapshotId);
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Snapshot " + snapshotId + " was not committed");
    }

    private static void assertVersion(FileStoreTable table, int expected) throws Exception {
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).equal(0, 1L));
        try (RecordReaderIterator<InternalRow> rows =
                new RecordReaderIterator<>(read.newRead().createReader(read.newScan().plan()))) {
            int matchingRows = 0;
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                if (row.getLong(0) == 1L) {
                    assertThat(row.getInt(1)).isEqualTo(expected);
                    matchingRows++;
                }
            }
            assertThat(matchingRows).isEqualTo(1);
        }
    }

    private static class SourceControl {
        private final CompletableFuture<Void> initialRowsWritten = new CompletableFuture<>();
        private final CompletableFuture<Void> failSource = new CompletableFuture<>();
        private final CompletableFuture<Integer> restoredPosition = new CompletableFuture<>();
        private final CompletableFuture<Void> replayedRowsWritten = new CompletableFuture<>();
    }

    private static class ReplayingSource extends AbstractNonCoordinatedSource<InternalRow> {

        private static final long serialVersionUID = 1L;

        private final String sourceId;

        private ReplayingSource(String sourceId) {
            this.sourceId = sourceId;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<InternalRow, SimpleSourceSplit> createReader(
                SourceReaderContext readerContext) {
            return new ReplayingReader(CONTROLS.get(sourceId));
        }
    }

    private static class ReplayingReader extends AbstractNonCoordinatedSourceReader<InternalRow> {

        private final SourceControl control;
        private final CompletableFuture<Void> idle = new CompletableFuture<>();
        private int position;
        private boolean restored;

        private ReplayingReader(SourceControl control) {
            this.control = control;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<InternalRow> output) {
            if (position == 0) {
                // Make the committed row's sequence number greater than any replay initialized
                // from the stale snapshot containing only version 3.
                for (long id = 2; id <= 21; id++) {
                    output.collect(GenericRow.of(id, 0));
                }
                output.collect(GenericRow.of(1L, 4));
                position = 21;
                control.initialRowsWritten.complete(null);
            } else if (position == 21 && control.failSource.isDone()) {
                output.collect(GenericRow.ofKind(RowKind.DELETE, 1L, 4));
                output.collect(GenericRow.of(1L, 5));
                position = 23;
                if (!restored) {
                    throw new RuntimeException("Injected source failure before checkpoint");
                }
                control.replayedRowsWritten.complete(null);
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            if (position == 0) {
                return CompletableFuture.completedFuture(null);
            }
            return position == 21 ? control.failSource : idle;
        }

        @Override
        public List<SimpleSourceSplit> snapshotState(long checkpointId) {
            return Collections.singletonList(
                    new SimpleSourceSplit("position", String.valueOf(position)));
        }

        @Override
        public void addSplits(List<SimpleSourceSplit> splits) {
            assertThat(splits).hasSize(1);
            position = Integer.parseInt(splits.get(0).value());
            restored = true;
            control.restoredPosition.complete(position);
        }
    }
}
