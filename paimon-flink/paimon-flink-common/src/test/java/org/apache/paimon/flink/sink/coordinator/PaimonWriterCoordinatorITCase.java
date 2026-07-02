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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSource;
import org.apache.paimon.flink.source.AbstractNonCoordinatedSourceReader;
import org.apache.paimon.flink.source.SimpleSourceSplit;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link PaimonWriterCoordinator}. */
@SuppressWarnings("BusyWait")
public class PaimonWriterCoordinatorITCase extends CatalogITCaseBase {

    private static final String MINI_CLUSTER_FIELD = "miniCluster";
    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("k", new IntType()),
                            new RowType.RowField("v", new VarCharType())));

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE unaware_table (k INT, v STRING) WITH ("
                        + "'bucket'='-1',"
                        + "'sink.committer-coordinator-operator.enabled'='true')",
                "CREATE TABLE fixed_table (k INT, v STRING) WITH ("
                        + "'bucket'='1',"
                        + "'bucket-key'='k',"
                        + "'sink.committer-coordinator-operator.enabled'='true')",
                "CREATE TABLE dynamic_table (k INT PRIMARY KEY NOT ENFORCED, v STRING) WITH ("
                        + "'bucket'='-1',"
                        + "'sink.committer-coordinator-operator.enabled'='true')");
    }

    @Test
    @Timeout(120)
    public void testStreamingCheckpointWriteUnawareTableWithWriterCoordinator() throws Exception {
        testStreamingCheckpointWriteWithWriterCoordinator("unaware_table");
    }

    @Test
    public void testFixedTableIgnoresWriterCoordinatorOption() throws Exception {
        assertUsesGlobalCommitter(buildPaimonSink("fixed_table"), "fixed_table");
    }

    @Test
    public void testDynamicTableIgnoresWriterCoordinatorOption() throws Exception {
        assertUsesGlobalCommitter(buildPaimonSink("dynamic_table"), "dynamic_table");
    }

    private void testStreamingCheckpointWriteWithWriterCoordinator(String tableName)
            throws Exception {
        StreamExecutionEnvironment env = buildPaimonSink(tableName);
        assertThat(transformationNames(env)).doesNotContain("Global Committer : " + tableName);

        JobClient jobClient = env.executeAsync();
        triggerCheckpointAndWaitForWrites(jobClient, tableName, 4);
        jobClient.cancel().get();

        sqlAssertWithRetry(
                "SELECT * FROM " + tableName,
                rows ->
                        rows.containsExactlyInAnyOrder(
                                Row.of(1, "one"),
                                Row.of(2, "two"),
                                Row.of(3, "three"),
                                Row.of(4, "four")));
    }

    private StreamExecutionEnvironment buildPaimonSink(String tableName) throws Exception {
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .checkpointIntervalMs(100)
                        .build();

        new FlinkSinkBuilder(paimonTable(tableName))
                .forRowData(
                        env.fromSource(
                                        new EmitOnceAndWaitSource(),
                                        WatermarkStrategy.noWatermarks(),
                                        "EmitOnceAndWaitSource",
                                        InternalTypeInfo.of(ROW_TYPE))
                                .setParallelism(1))
                .build();
        return env;
    }

    private void assertUsesGlobalCommitter(StreamExecutionEnvironment env, String tableName) {
        assertThat(transformationNames(env)).contains("Global Committer : " + tableName);
    }

    private List<String> transformationNames(StreamExecutionEnvironment env) {
        List<String> names = new ArrayList<>();
        List<Transformation<?>> pending = new ArrayList<>(env.getTransformations());
        Set<Integer> visited = new HashSet<>();
        while (!pending.isEmpty()) {
            Transformation<?> transformation = pending.remove(pending.size() - 1);
            if (visited.add(transformation.getId())) {
                names.add(transformation.getName());
                pending.addAll(transformation.getInputs());
            }
        }
        return names;
    }

    @SuppressWarnings("unchecked")
    private <T> T reflectGetMiniCluster(Object instance)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(MINI_CLUSTER_FIELD);
        field.setAccessible(true);
        return (T) field.get(instance);
    }

    private void triggerCheckpointAndWaitForWrites(
            JobClient jobClient, String tableName, long totalRecords) throws Exception {
        MiniCluster miniCluster = reflectGetMiniCluster(jobClient);
        JobID jobID = jobClient.getJobID();
        waitForJobRunning(jobClient, miniCluster, jobID);

        long lastSnapshotId = -1L;
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            miniCluster.triggerCheckpoint(jobID).get();
            Snapshot snapshot = waitForNewSnapshot(tableName, lastSnapshotId, deadline);
            lastSnapshotId = snapshot.id();
            if (snapshot.totalRecordCount() >= totalRecords) {
                return;
            }
        }
        throw new AssertionError("Timed out waiting for records committed by PWC.");
    }

    private void waitForJobRunning(JobClient jobClient, MiniCluster miniCluster, JobID jobID)
            throws Exception {
        JobStatus jobStatus = jobClient.getJobStatus().get();
        while (jobStatus == JobStatus.INITIALIZING || jobStatus == JobStatus.CREATED) {
            Thread.sleep(500L);
            jobStatus = jobClient.getJobStatus().get();
        }

        if (jobStatus != JobStatus.RUNNING) {
            throw new IllegalStateException("Job status is not RUNNING");
        }

        AtomicBoolean allTaskRunning = new AtomicBoolean(false);
        while (!allTaskRunning.get()) {
            allTaskRunning.set(true);
            Thread.sleep(500L);
            miniCluster
                    .getExecutionGraph(jobID)
                    .thenAccept(
                            graph ->
                                    graph.getAllExecutionVertices()
                                            .forEach(
                                                    vertex -> {
                                                        if (vertex.getExecutionState()
                                                                != ExecutionState.RUNNING) {
                                                            allTaskRunning.set(false);
                                                        }
                                                    }))
                    .get();
        }
    }

    private Snapshot waitForNewSnapshot(String tableName, long initialSnapshotId, long deadline)
            throws InterruptedException {
        Snapshot snapshot = findLatestSnapshot(tableName);
        while (System.currentTimeMillis() < deadline
                && (snapshot == null || snapshot.id() == initialSnapshotId)) {
            Thread.sleep(500L);
            snapshot = findLatestSnapshot(tableName);
        }
        if (snapshot == null || snapshot.id() == initialSnapshotId) {
            throw new AssertionError("Timed out waiting for a new Paimon snapshot.");
        }
        return snapshot;
    }

    private static class EmitOnceAndWaitSource extends AbstractNonCoordinatedSource<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<RowData, SimpleSourceSplit> createReader(
                SourceReaderContext sourceReaderContext) {
            return new Reader();
        }

        private static class Reader extends AbstractNonCoordinatedSourceReader<RowData> {

            private boolean emitted;

            @Override
            public InputStatus pollNext(ReaderOutput<RowData> output) {
                if (!emitted) {
                    output.collect(row(1, "one"));
                    output.collect(row(2, "two"));
                    output.collect(row(3, "three"));
                    output.collect(row(4, "four"));
                    emitted = true;
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        private static RowData row(int k, String v) {
            return GenericRowData.of(k, StringData.fromString(v));
        }
    }
}
