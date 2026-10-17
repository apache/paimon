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
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_COVER_ALL_OPERATORS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Steps 3 and 4 of the migration {@code docs/docs/flink/savepoint.md} prescribes for turning {@code
 * sink.operator-uid.cover-all-operators} on under a running job.
 *
 * <p>Restores go through {@code execution.state-recovery.path}, which {@code
 * Checkpoints.loadAndValidateCheckpoint} guards: unlike the high-availability store it skips an
 * orphaned entry holding nothing and rejects one holding state. {@link
 * OperatorUidSuffixRestoreITCase} covers the store route.
 */
class OperatorUidMigrationITCase {

    private static final String UID_SUFFIX = "test-uid";
    private static final String TABLE_NAME = "append_table";
    private static final long DEADLINE_MILLIS = 120_000;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("k", new IntType()),
                            new RowType.RowField("pt", new VarCharType(10))));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.INT()),
                    DataTypes.FIELD("pt", DataTypes.STRING()));

    @TempDir private java.nio.file.Path warehouse;
    @TempDir private java.nio.file.Path checkpoints;
    @TempDir private java.nio.file.Path input;

    /** Guide step 3: the operators an unaware-bucket append table covers all hold nothing. */
    @Test
    @Timeout(300)
    void testStatelessOperatorsMigrateWithoutIgnoringUnclaimedState() throws Exception {
        FileStoreTable before = table("stateless", false, false);
        String path = runAndCheckpoint(before);

        assertRestored(table("stateless", true, false), path, false, snapshot(before));
    }

    /**
     * Guide step 4. {@code Collect Statistics} has an operator coordinator, whose entry is never
     * empty, so the same restore is rejected until it is told to drop unclaimed state. Which
     * entries it then drops is asserted by name, because the log prints only opaque ids.
     */
    @Test
    @Timeout(300)
    void testPartitionDynamicNeedsIgnoreUnclaimedStateOnce() throws Exception {
        FileStoreTable before = table("partition-dynamic", false, true);
        String path = runAndCheckpoint(before);
        Snapshot committed = snapshot(before);
        FileStoreTable after = table("partition-dynamic", true, true);

        assertThatThrownBy(() -> assertRestored(after, path, false, committed))
                .as("the statistics coordinator's entry holds state, so it cannot be skipped")
                .hasStackTraceContaining("Cannot map checkpoint/savepoint state for operator");

        assertRestored(after, path, true, committed);

        Map<String, Operator> claimed = operatorsById(job(before, path, false));
        Set<String> dropped = new HashSet<>(operatorIdsIn(path));
        dropped.retainAll(claimed.keySet());
        dropped.removeAll(operatorsById(job(after, path, false)).keySet());
        List<String> names = new ArrayList<>();
        List<String> uids = new ArrayList<>();
        for (String id : dropped) {
            names.add(claimed.get(id).name);
            uids.add(claimed.get(id).uid);
        }

        assertThat(names)
                .as("the statistics coordinator's is the dropped entry that held state")
                .contains("Collect Statistics: " + TABLE_NAME)
                .doesNotContain("Writer : " + TABLE_NAME, "Global Committer : " + TABLE_NAME);
        assertThat(uids)
                .as("nothing that already carried a uid loses its entry")
                .containsOnlyNulls();
    }

    // ------------------------------------------------------------------------
    //  running jobs
    // ------------------------------------------------------------------------

    /**
     * The commit user is a fresh UUID per job, kept in the Global Committer's {@code
     * commit_user_state}, so a committer whose entry had been dropped would commit under a new one.
     * Reaching {@code RUNNING} would prove nothing; a later snapshot under the same user can only
     * come from restored state.
     */
    private void assertRestored(
            FileStoreTable table, String path, boolean ignoreUnclaimedState, Snapshot committed)
            throws Exception {
        JobClient client = job(table, path, ignoreUnclaimedState).executeAsync();
        awaitCheckpoint(client);
        // a second file, because the restored source resumes past the first one
        Files.write(input.resolve("more.txt"), Arrays.asList("3", "4"));
        Snapshot restored = awaitSnapshotAfter(client, table, committed.id());
        client.cancel().get();

        assertThat(restored.commitUser())
                .as("the committer kept its uid, so its commit user came back from state")
                .isEqualTo(committed.commitUser());
    }

    /** Writes three rows, checkpoints once they are committed, and returns the path. */
    private String runAndCheckpoint(FileStoreTable table) throws Exception {
        Files.write(input.resolve("rows.txt"), Arrays.asList("0", "1", "2"));

        JobClient client = job(table, null, false).executeAsync();
        awaitCheckpoint(client);
        awaitSnapshotAfter(client, table, 0);
        // one more, so the checkpoint is no older than the snapshot the restore has to continue
        String path = miniCluster(client).triggerCheckpoint(client.getJobID()).get();
        client.cancel().get();
        return path;
    }

    /** Checkpoints until the table has a snapshot later than {@code id}, and returns it. */
    private Snapshot awaitSnapshotAfter(JobClient client, FileStoreTable table, long id)
            throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (true) {
            Snapshot snapshot = snapshot(table);
            if (snapshot != null && snapshot.id() > id) {
                return snapshot;
            }
            if (System.currentTimeMillis() > deadline) {
                fail("no snapshot after %d was committed in time", id);
            }
            miniCluster(client).triggerCheckpoint(client.getJobID()).get();
        }
    }

    /**
     * A restore Flink rejects surfaces here, as the job going terminal rather than from the submit,
     * so this waits on the status itself: {@code CommonTestUtils.waitForAllTaskRunning} would block
     * forever on a job whose tasks never start.
     */
    private static void awaitCheckpoint(JobClient client) throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (true) {
            JobStatus status = client.getJobStatus().get();
            if (status.isGloballyTerminalState()) {
                client.getJobExecutionResult().get(20, TimeUnit.SECONDS);
                fail("the job reached %s without checkpointing", status);
            }
            if (status == JobStatus.RUNNING && allTasksRunning(client)) {
                miniCluster(client).triggerCheckpoint(client.getJobID()).get();
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                fail("the job never checkpointed, last status was %s", status);
            }
            Thread.sleep(500);
        }
    }

    private static boolean allTasksRunning(JobClient client) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        miniCluster(client)
                .getExecutionGraph(client.getJobID())
                .thenAccept(
                        graph ->
                                graph.getAllExecutionVertices()
                                        .forEach(
                                                vertex -> {
                                                    if (vertex.getExecutionState()
                                                            != ExecutionState.RUNNING) {
                                                        running.set(false);
                                                    }
                                                }))
                .get();
        return running.get();
    }

    /** As {@code org.apache.paimon.flink.FlinkJobRecoveryITCase} does, for want of a public API. */
    private static MiniCluster miniCluster(JobClient client) throws Exception {
        Field field = client.getClass().getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(client);
    }

    /**
     * One topology across the migration, as the guide requires, with everything outside the sink
     * uid'd, so only the sink's operators can move.
     */
    private StreamExecutionEnvironment job(
            FileStoreTable table, String recoverFrom, boolean ignoreUnclaimedState) {
        Configuration conf = new Configuration();
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri().toString());
        if (recoverFrom != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, recoverFrom);
            conf.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, ignoreUnclaimedState);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(Duration.ofDays(1).toMillis());

        // the directory is monitored, so the job idles between files instead of finishing, and a
        // file dropped in after a restore reaches the sink
        FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(input.toUri()))
                        .monitorContinuously(Duration.ofMillis(200))
                        .build();

        DataStream<Row> rows =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "rows")
                        .uid("rows")
                        .map(line -> Row.of(Integer.parseInt(line), "pt-" + line))
                        .returns(Types.ROW(Types.INT, Types.STRING))
                        .uid("to-row");

        new FlinkSinkBuilder(table).forRow(rows, INPUT_TYPE).build();
        return env;
    }

    // ------------------------------------------------------------------------
    //  operator ids, on both sides of the restore
    // ------------------------------------------------------------------------

    /** The operator ids a checkpoint holds an entry for. */
    private static List<String> operatorIdsIn(String path) throws Exception {
        Path metadata = new Path(path, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        try (FSDataInputStream in = metadata.getFileSystem().open(metadata)) {
            CheckpointMetadata checkpoint =
                    Checkpoints.loadCheckpointMetadata(
                            new DataInputStream(in),
                            OperatorUidMigrationITCase.class.getClassLoader(),
                            path);
            List<String> ids = new ArrayList<>();
            for (OperatorState state : checkpoint.getOperatorStates()) {
                ids.add(state.getOperatorID().toHexString());
            }
            return ids;
        }
    }

    /**
     * Keyed by the id Flink compares a checkpoint entry against, so a dropped entry can be named.
     */
    private static Map<String, Operator> operatorsById(StreamExecutionEnvironment env) {
        StreamGraph graph = env.getStreamGraph();
        Map<Integer, byte[]> hashes =
                new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
        Map<String, Operator> operators = new HashMap<>();
        for (StreamNode node : graph.getStreamNodes()) {
            operators.put(
                    new OperatorID(hashes.get(node.getId())).toHexString(),
                    new Operator(node.getOperatorName(), node.getTransformationUID()));
        }
        return operators;
    }

    private static final class Operator {

        private final String name;
        private final String uid;

        private Operator(String name, String uid) {
            this.name = name;
            this.uid = uid;
        }
    }

    // ------------------------------------------------------------------------
    //  the table
    // ------------------------------------------------------------------------

    private static Snapshot snapshot(FileStoreTable table) {
        return table.snapshotManager().latestSnapshotFromFileSystem();
    }

    /** One directory per uid layout, so both jobs write a table of the same name. */
    private FileStoreTable table(String directory, boolean coverAll, boolean partitionDynamic)
            throws Exception {
        Options options = new Options();
        options.set(PATH, warehouse.resolve(directory).resolve(TABLE_NAME).toString());
        options.set(SINK_OPERATOR_UID_SUFFIX, UID_SUFFIX);
        options.set(SINK_OPERATOR_UID_COVER_ALL_OPERATORS, coverAll);
        if (partitionDynamic) {
            options.set(
                    CoreOptions.PARTITION_SINK_STRATEGY,
                    CoreOptions.PartitionSinkStrategy.PARTITION_DYNAMIC);
        }

        java.nio.file.Path path = warehouse.resolve(directory).resolve(TABLE_NAME);
        if (!Files.exists(path)) {
            new FileSystemSchemaManager(
                            LocalFileIO.create(), new CoreOptions(options.toMap()).path())
                    .createTable(
                            new Schema(
                                    toDataType(TABLE_TYPE).getFields(),
                                    partitionDynamic
                                            ? Collections.singletonList("pt")
                                            : Collections.emptyList(),
                                    Collections.emptyList(),
                                    options.toMap(),
                                    ""));
        }
        return FileStoreTableFactory.create(LocalFileIO.create(), options);
    }
}
