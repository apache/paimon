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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.flink.expire.RangePartitionedExpireFunction;
import org.apache.paimon.flink.expire.SnapshotExpireSink;
import org.apache.paimon.flink.util.MiniClusterWithClientExtension;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.ExpireSnapshotsTest;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.expire.DeletionReport;
import org.apache.paimon.operation.expire.ExpireSnapshotsExecutor;
import org.apache.paimon.operation.expire.ExpireSnapshotsPlan;
import org.apache.paimon.operation.expire.ExpireSnapshotsPlanner;
import org.apache.paimon.operation.expire.SnapshotExpireTask;
import org.apache.paimon.options.ExpireConfig;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * IT Case for parallel expire snapshots using Flink execution. This class extends {@link
 * ExpireSnapshotsTest} and overrides {@link #doExpire} to test parallel execution mode with real
 * Flink DataStream API.
 *
 * <p>This test validates that the Flink-based parallel expire logic produces the same results as
 * the serial implementation. It extends the production {@link RangePartitionedExpireFunction} and
 * {@link SnapshotExpireSink} classes, overriding {@code initExecutor} to inject test executors.
 */
public class ExpireSnapshotsActionITCase extends ExpireSnapshotsTest {

    private static final int TEST_PARALLELISM = 2;

    // Placeholder identifier for test (not actually used since we override initExecutor)
    private static final Identifier TEST_IDENTIFIER = Identifier.create("default", "test_table");

    @RegisterExtension
    protected static final MiniClusterWithClientExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(TEST_PARALLELISM)
                            .build());

    // Static supplier for test executor (used by inner test classes)
    private static volatile Supplier<ExpireSnapshotsExecutor> executorSupplier;

    @Override
    protected int doExpire(ExpireConfig config) {
        // 1. Create planner and generate plan
        SnapshotDeletion snapshotDeletion = store.newSnapshotDeletion();
        ExpireSnapshotsPlanner planner =
                new ExpireSnapshotsPlanner(
                        snapshotManager,
                        new ConsumerManager(
                                fileIO, new Path(tempDir.toUri()), snapshotManager.branch()),
                        snapshotDeletion,
                        store.newTagManager());
        ExpireSnapshotsPlan plan = planner.plan(config);

        if (plan.isEmpty()) {
            return 0;
        }

        // 2. Set up executor supplier for test subclasses
        executorSupplier =
                () ->
                        new ExpireSnapshotsExecutor(
                                snapshotManager, snapshotDeletion, changelogManager);

        try {
            // 3. Create Flink execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.setParallelism(TEST_PARALLELISM);

            // 4. Build worker phase: partition tasks by snapshot range
            List<List<SnapshotExpireTask>> partitionedGroups =
                    plan.partitionTasksBySnapshotRange(TEST_PARALLELISM);

            DataStreamSource<List<SnapshotExpireTask>> source =
                    env.fromCollection(partitionedGroups).setParallelism(1);

            // 5. Apply test worker function (extends production class, overrides initExecutor)
            DataStream<DeletionReport> reports =
                    source.rebalance()
                            .flatMap(
                                    new TestExpireFunction(
                                            Collections.emptyMap(),
                                            TEST_IDENTIFIER,
                                            plan.protectionSet().taggedSnapshots()))
                            .returns(new JavaTypeInfo<>(DeletionReport.class))
                            .setParallelism(TEST_PARALLELISM)
                            .name("RangePartitionedExpire");

            // 6. Apply test sink (extends SnapshotExpireSink, overrides initExecutor)
            reports.sinkTo(
                            new TestExpireSink(
                                    Collections.emptyMap(),
                                    TEST_IDENTIFIER,
                                    plan.endExclusiveId(),
                                    plan.protectionSet().manifestSkippingSet(),
                                    plan.manifestTasks(),
                                    plan.snapshotFileTasks()))
                    .setParallelism(1)
                    .name("SnapshotExpireSink");

            // 7. Execute Flink job
            env.execute("ExpireSnapshotsFlinkTest");

            return plan.snapshotFileTasks().size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            executorSupplier = null;
        }
    }

    // ==================== Test Subclasses ====================

    /**
     * Test subclass of {@link RangePartitionedExpireFunction} that overrides {@code initExecutor}
     * to use the test executor supplier, completely bypassing catalog access.
     */
    private static class TestExpireFunction extends RangePartitionedExpireFunction {

        private static final long serialVersionUID = 1L;

        public TestExpireFunction(
                Map<String, String> catalogConfig,
                Identifier identifier,
                List<Snapshot> taggedSnapshots) {
            super(catalogConfig, identifier, taggedSnapshots);
        }

        @Override
        protected ExpireSnapshotsExecutor initExecutor() {
            // Use the static executor supplier from test setup, bypassing catalog
            return executorSupplier.get();
        }
    }

    /**
     * Test subclass of {@link SnapshotExpireSink} that overrides {@code initExecutor} to use the
     * test executor supplier, completely bypassing catalog access.
     */
    private static class TestExpireSink extends SnapshotExpireSink {

        private static final long serialVersionUID = 1L;

        public TestExpireSink(
                Map<String, String> catalogConfig,
                Identifier identifier,
                long endExclusiveId,
                Set<String> manifestSkippingSet,
                List<SnapshotExpireTask> manifestTasks,
                List<SnapshotExpireTask> snapshotFileTasks) {
            super(
                    catalogConfig,
                    identifier,
                    endExclusiveId,
                    manifestSkippingSet,
                    manifestTasks,
                    snapshotFileTasks);
        }

        @Override
        protected ExpireSnapshotsExecutor initExecutor() {
            // Use the static executor supplier from test setup, bypassing catalog
            return executorSupplier.get();
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
            "Tests concurrent expire scenario which has different semantics in Flink parallel mode")
    @Override
    public void testExpireEmptySnapshot() {}
}
