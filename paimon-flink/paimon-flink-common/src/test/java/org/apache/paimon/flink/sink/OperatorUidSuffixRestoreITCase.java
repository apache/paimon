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
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_COVER_ALL_OPERATORS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

/**
 * A job must still restore from a retained checkpoint after an operator is added upstream of a
 * Paimon sink, which is what {@code sink.operator-uid.suffix} is for. On its own the suffix does
 * not deliver that: some sink operators carry no uid and Flink then derives their ids from the
 * shape of the stream graph.
 *
 * <p>The table asks for full coverage through {@link
 * org.apache.paimon.flink.FlinkConnectorOptions#SINK_OPERATOR_UID_COVER_ALL_OPERATORS}, without
 * which the suffix reaches only the writer and the committer.
 *
 * <p>Only a checkpoint recovered from the {@link CompletedCheckpointStore} shows this, which is
 * what a JobManager restores from when it is highly available. The savepoint route hides it. {@code
 * org.apache.paimon.flink.FlinkJobRecoveryITCase#testRestoreFromSavepointWithJobGraphChange} takes
 * a savepoint and restores through a configured {@code execution.state-recovery.path}, which runs
 * {@code Checkpoints.loadAndValidateCheckpoint} first. That silently skips an unmatched operator
 * holding no state, and the row conversions and the compaction operators hold none, so the orphaned
 * entries are gone before the JobMaster ever compares ids. Recovering from the store compares every
 * id and fails on the first it cannot place, which is what production does.
 */
class OperatorUidSuffixRestoreITCase {

    private static final String UID_SUFFIX = "test-uid";
    private static final String TABLE_NAME = "append_table";

    /**
     * Two of these run in sequence per test, plus the 20 second net below, so the worst case stays
     * inside the {@code @Timeout} and the informative message is the one that gets printed.
     */
    private static final long DEADLINE_MILLIS = 60_000;

    /** Read on every recovery, so each test gets a fresh store from a class scoped cluster. */
    private static volatile RetainingCheckpointStore retainedStore;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(
                                    new Configuration()
                                            .set(
                                                    HighAvailabilityOptions.HA_MODE,
                                                    RetainingHaServicesFactory.class.getName()))
                            .build());

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("k", new IntType()),
                            new RowType.RowField("v", new VarCharType(10))));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.INT()),
                    DataTypes.FIELD("v", DataTypes.STRING()));

    @TempDir private Path warehouse;
    @TempDir private Path emptySource;

    @BeforeEach
    void setUp() {
        retainedStore = new RetainingCheckpointStore();
    }

    /** What the suffix promises. Fails as soon as one sink operator loses its uid. */
    @Test
    @Timeout(180)
    void testRestoreAfterUpstreamTopologyChange() throws Exception {
        FileStoreTable table = unawareBucketTable("unchanged-suffix", UID_SUFFIX);

        runUntilCheckpointed(buildSinkJob(table, 0));

        Throwable failure = restoreFailure(buildSinkJob(table, 1).executeAsync());
        assertThat(failure)
                .as(
                        "the job did not restore from the retained checkpoint after one operator "
                                + "was added upstream of the sink:%n%s",
                        failure == null ? "" : ExceptionUtils.stringifyException(failure))
                .isNull();
    }

    /**
     * The permanent negative. Changing the suffix renames every uid Paimon assigns, so the retained
     * checkpoint holds entries no operator in the new graph claims and the restore has to fail. The
     * exception is the only proof that a restore is being attempted at all, and it is the same one
     * production reported:
     *
     * <pre>
     * IllegalStateException: There is no operator for the state &lt;hash&gt;
     * </pre>
     *
     * <p>The two jobs are otherwise identical, down to the table name the uids are built from, so
     * nothing but the suffix can account for the failure.
     */
    @Test
    @Timeout(180)
    void testRestoreRejectsAChangedUidSuffix() throws Exception {
        runUntilCheckpointed(buildSinkJob(unawareBucketTable("one-suffix", UID_SUFFIX), 0));

        Throwable failure =
                restoreFailure(
                        buildSinkJob(unawareBucketTable("another-suffix", "other-uid"), 0)
                                .executeAsync());

        assertThat(failure)
                .as("a renamed uid orphans a checkpoint entry, which has to be rejected")
                .isNotNull();
        assertThat(ExceptionUtils.stringifyException(failure))
                .contains("There is no operator for the state");
    }

    private static void runUntilCheckpointed(StreamExecutionEnvironment env) throws Exception {
        JobClient client = env.executeAsync();
        long deadline = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (retainedStore.getAllCheckpoints().isEmpty()) {
            if (System.currentTimeMillis() > deadline) {
                fail("no checkpoint completed in time");
            }
            Thread.sleep(200);
        }
        client.cancel().get();
        assertThat(retainedStore.getAllCheckpoints()).isNotEmpty();
    }

    /**
     * Why the job stopped, or null if it reached {@code RUNNING}. Null means only that nothing
     * rejected the checkpoint, never that a restore took place.
     */
    private static Throwable restoreFailure(JobClient client) throws Exception {
        long deadline = System.currentTimeMillis() + DEADLINE_MILLIS;
        while (true) {
            JobStatus status = client.getJobStatus().get();
            if (status == JobStatus.RUNNING) {
                client.cancel().get();
                return null;
            }
            if (status.isGloballyTerminalState()) {
                // the job is already terminal, so the future is complete; this is only a net
                return catchThrowable(
                        () -> client.getJobExecutionResult().get(20, TimeUnit.SECONDS));
            }
            if (System.currentTimeMillis() > deadline) {
                fail("job never reached %s, last status was %s", JobStatus.RUNNING, status);
            }
            Thread.sleep(200);
        }
    }

    /**
     * Without a primary key the table is BUCKET_UNAWARE, the mode {@link AppendTableSink} serves.
     * One parent directory per table, so two tables can differ only in their uid suffix and still
     * be called the same thing, which is the other half of what a uid is built from.
     */
    private FileStoreTable unawareBucketTable(String directory, String uidSuffix) throws Exception {
        Options options = new Options();
        options.set(PATH, warehouse.resolve(directory).resolve(TABLE_NAME).toString());
        options.set(SINK_OPERATOR_UID_SUFFIX, uidSuffix);
        options.set(SINK_OPERATOR_UID_COVER_ALL_OPERATORS, true);

        Schema schema =
                new Schema(
                        toDataType(TABLE_TYPE).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options.toMap(),
                        "");
        new FileSystemSchemaManager(LocalFileIO.create(), new CoreOptions(options.toMap()).path())
                .createTable(schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), options);
    }

    /** Everything outside the sink is uid'd, so only the sink can lose its operator ids. */
    private StreamExecutionEnvironment buildSinkJob(FileStoreTable table, int extraOperators) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);

        // an empty directory monitored forever: no records, and the job never finishes
        FileSource<String> idle =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new org.apache.flink.core.fs.Path(emptySource.toUri()))
                        .monitorContinuously(Duration.ofDays(1))
                        .build();

        DataStream<Row> source =
                env.fromSource(idle, WatermarkStrategy.noWatermarks(), "idle")
                        .uid("idle")
                        .map(line -> Row.of(0, line))
                        .returns(Types.ROW(Types.INT, Types.STRING))
                        .uid("to-row");

        for (int i = 0; i < extraOperators; i++) {
            source =
                    source.map(row -> row)
                            .returns(Types.ROW(Types.INT, Types.STRING))
                            .uid("extra-" + i);
        }

        // deliberately not uid'd here: a uid on the returned sink would override Paimon's own
        // uid on the final 'end' operator, leaving its coverage untested
        new FlinkSinkBuilder(table).forRow(source, INPUT_TYPE).build();
        return env;
    }

    /** Keeps its checkpoints when the job goes away, as an external store would. */
    private static final class RetainingCheckpointStore extends StandaloneCompletedCheckpointStore {

        RetainingCheckpointStore() {
            super(1);
        }

        @Override
        public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner) {
            // Deliberately does not call super, which clears the checkpoint list in a finally
            // whatever the job status, leaving the next job nothing to restore from. Retaining
            // externalized checkpoints does not help either, that only spares the files. Outliving
            // the JobManager is the one property of a real store being stood in for here.
        }
    }

    /** Instantiated by Flink from {@code high-availability.type}. */
    public static final class RetainingHaServicesFactory
            implements HighAvailabilityServicesFactory {

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            // ignoring the job id hands every job the one store, so the next job is offered it
            return new EmbeddedHaServicesWithLeadershipControl(
                    executor,
                    new PerJobCheckpointRecoveryFactory<RetainingCheckpointStore>(
                            (maxCheckpoints, previous, registry, ioExecutor, restoreMode) ->
                                    retainedStore));
        }
    }
}
