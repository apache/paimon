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

package org.apache.paimon.flink.action;

import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.flink.expire.DeletionReport;
import org.apache.paimon.flink.expire.ExpireSnapshotsPlan;
import org.apache.paimon.flink.expire.ExpireSnapshotsPlanner;
import org.apache.paimon.flink.expire.RangePartitionedExpireFunction;
import org.apache.paimon.flink.expire.SnapshotExpireContext;
import org.apache.paimon.flink.expire.SnapshotExpireSink;
import org.apache.paimon.flink.expire.SnapshotExpireTask;
import org.apache.paimon.flink.util.MiniClusterWithClientExtension;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.ExpireSnapshotsTest;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * IT Case for parallel expire snapshots using Flink execution. This class extends {@link
 * ExpireSnapshotsTest} and overrides {@link #doExpire} to test parallel execution mode with real
 * Flink DataStream API.
 *
 * <p>This test validates that the Flink-based parallel expire logic produces the same results as
 * the serial implementation. It extends the production {@link RangePartitionedExpireFunction} and
 * {@link SnapshotExpireSink} classes, overriding {@code initContext} to inject test contexts.
 */
public class ParallelExpireSnapshotsActionITCase extends ExpireSnapshotsTest {

    private static final int TEST_PARALLELISM = 2;

    private static final Identifier TEST_IDENTIFIER = Identifier.create("default", "test_table");

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(TEST_PARALLELISM)
                            .build());

    // Static supplier for test context (used by inner test classes)
    private static volatile Supplier<SnapshotExpireContext> contextSupplier;

    // Controls failure injection: when >= 0, TestExpireFunction throws after this many tasks.
    private static volatile int failAfterTasks = -1;

    @Override
    protected int doExpire(ExpireConfig config) {
        SnapshotDeletion snapshotDeletion = store.newSnapshotDeletion();
        ExpireSnapshotsPlanner planner =
                new ExpireSnapshotsPlanner(
                        snapshotManager,
                        new ConsumerManager(
                                fileIO, new Path(tempDir.toUri()), snapshotManager.branch()),
                        snapshotDeletion,
                        store.newTagManager());
        ExpireSnapshotsPlan plan = planner.plan(config);

        List<Snapshot> taggedSnapshots = plan.protectionSet().taggedSnapshots();
        Map<Long, Snapshot> snapshotCache = plan.snapshotCache();
        contextSupplier =
                () -> {
                    SnapshotDeletion deletion = store.newSnapshotDeletion();
                    deletion.setChangelogDecoupled(config.isChangelogDecoupled());
                    return new SnapshotExpireContext(
                            snapshotManager,
                            deletion,
                            changelogManager,
                            taggedSnapshots,
                            snapshotCache);
                };

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.setParallelism(TEST_PARALLELISM);

            List<List<SnapshotExpireTask>> partitionedGroups =
                    plan.partitionTasksBySnapshotRange(TEST_PARALLELISM);

            DataStreamSource<List<SnapshotExpireTask>> source =
                    env.fromCollection(
                                    partitionedGroups,
                                    TypeInformation.of(new TypeHint<List<SnapshotExpireTask>>() {}))
                            .setParallelism(1);

            DataStream<DeletionReport> reports =
                    source.rebalance()
                            .flatMap(
                                    new TestExpireFunction(
                                            Collections.emptyMap(),
                                            TEST_IDENTIFIER,
                                            plan.protectionSet().taggedSnapshots(),
                                            plan.snapshotCache(),
                                            config.isChangelogDecoupled()))
                            .returns(new JavaTypeInfo<>(DeletionReport.class))
                            .setParallelism(TEST_PARALLELISM)
                            .name("RangePartitionedExpire");

            reports.sinkTo(
                            new TestExpireSink(
                                    Collections.emptyMap(),
                                    TEST_IDENTIFIER,
                                    plan.endExclusiveId(),
                                    plan.protectionSet().manifestSkippingSet(),
                                    plan.snapshotCache(),
                                    plan.manifestTasks(),
                                    plan.snapshotFileTasks(),
                                    config.isChangelogDecoupled()))
                    .setParallelism(1)
                    .name("SnapshotExpireCommit");

            env.execute("ExpireSnapshotsFlinkTest");

            return plan.snapshotFileTasks().size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            contextSupplier = null;
        }
    }

    // ==================== Test Subclasses ====================

    /**
     * Test subclass of {@link RangePartitionedExpireFunction} that overrides {@code initContext} to
     * use the test context supplier, completely bypassing catalog access.
     */
    private static class TestExpireFunction extends RangePartitionedExpireFunction {

        private static final long serialVersionUID = 1L;
        private int processedCount = 0;

        public TestExpireFunction(
                Map<String, String> catalogConfig,
                Identifier identifier,
                List<Snapshot> taggedSnapshots,
                Map<Long, Snapshot> snapshotCache,
                boolean changelogDecoupled) {
            super(catalogConfig, identifier, taggedSnapshots, snapshotCache, changelogDecoupled);
        }

        @Override
        protected SnapshotExpireContext initContext() {
            return contextSupplier.get();
        }

        @Override
        public void flatMap(List<SnapshotExpireTask> tasks, Collector<DeletionReport> out)
                throws Exception {
            for (SnapshotExpireTask task : tasks) {
                if (failAfterTasks >= 0 && processedCount++ >= failAfterTasks) {
                    throw new RuntimeException(
                            "Simulated failure after " + failAfterTasks + " tasks");
                }
                DeletionReport report = task.execute(context);
                report.setDeletionBuckets(context.snapshotDeletion().drainDeletionBuckets());
                out.collect(report);
            }
        }
    }

    /**
     * Test subclass of {@link SnapshotExpireSink} that overrides {@code initContext} to use the
     * test context supplier, completely bypassing catalog access.
     */
    private static class TestExpireSink extends SnapshotExpireSink {

        private static final long serialVersionUID = 1L;

        public TestExpireSink(
                Map<String, String> catalogConfig,
                Identifier identifier,
                long endExclusiveId,
                Set<String> manifestSkippingSet,
                Map<Long, Snapshot> snapshotCache,
                List<SnapshotExpireTask> manifestTasks,
                List<SnapshotExpireTask> snapshotFileTasks,
                boolean changelogDecoupled) {
            super(
                    catalogConfig,
                    identifier,
                    endExclusiveId,
                    manifestSkippingSet,
                    snapshotCache,
                    manifestTasks,
                    snapshotFileTasks,
                    changelogDecoupled);
        }

        @Override
        protected SnapshotExpireContext initContext() {
            return contextSupplier.get();
        }
    }

    // ==================== Additional Tests ====================

    /**
     * Tests that if the expired job fails midway (some data files deleted, but snapshot metadata
     * intact), a subsequent expire run can still complete successfully.
     */
    @Test
    public void testExpireRecoveryAfterPartialFailure() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(6, allData, snapshotPositions);

        ExpireConfig config =
                ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(1)
                        .snapshotTimeRetain(Duration.ofMillis(1))
                        .build();

        // First attempt: inject failure after processing 2 tasks
        // Some data files are deleted, but sink phase never runs,
        // so snapshot/manifest metadata remains intact
        failAfterTasks = 2;
        try {
            assertThatThrownBy(() -> doExpire(config)).isInstanceOf(RuntimeException.class);
        } finally {
            failAfterTasks = -1;
        }

        // Snapshot metadata should still exist since sink phase didn't run
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
        }

        // Recovery: run expire again, should complete successfully
        // despite some data files already being deleted
        doExpire(config);

        // Verify: only latest snapshot remains
        for (int i = 1; i < latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        assertThat(snapshotManager.snapshotExists(latestSnapshotId)).isTrue();
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
    }

    /**
     * Tests that when there are no snapshots to expire, the Flink job still starts and completes
     * successfully with an empty plan.
     */
    @Test
    public void testEmptyPlanFlinkJobCompletes() throws Exception {
        // Create a table with snapshots, but configure retention to keep all of them
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(3, allData, snapshotPositions);

        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();

        // Configure to retain more snapshots than exist → empty plan
        ExpireConfig config =
                ExpireConfig.builder()
                        .snapshotRetainMin(latestSnapshotId + 1)
                        .snapshotRetainMax(latestSnapshotId + 1)
                        .build();

        // Should complete without error even with empty plan
        int expired = doExpire(config);
        assertThat(expired).isEqualTo(0);

        // All snapshots should still exist
        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
        }
    }

    // ==================== Disabled Tests ====================
    // The following tests are disabled because they are not suitable for parallel Flink execution.

    @Test
    @Disabled("Tests SnapshotDeletion directly, not the full expire flow")
    @Override
    public void testExpireExtraFiles() {}

    @Test
    @Disabled("Tests SnapshotDeletion directly, not the full expire flow")
    @Override
    public void testExpireExtraFilesWithExternalPath() {}

    @Test
    @Disabled(
            "This test runs 10 concurrent doExpire calls, each submitting a Flink job in this"
                    + " subclass, which is not feasible in MiniCluster with limited slots")
    @Override
    public void testExpireEmptySnapshot() {}
}
