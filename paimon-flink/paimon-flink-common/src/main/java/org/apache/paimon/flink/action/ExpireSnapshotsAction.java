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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.expire.RangePartitionedExpireFunction;
import org.apache.paimon.flink.expire.SnapshotExpireSink;
import org.apache.paimon.flink.procedure.ExpireSnapshotsProcedure;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.operation.expire.DeletionReport;
import org.apache.paimon.operation.expire.ExpireSnapshotsPlan;
import org.apache.paimon.operation.expire.ExpireSnapshotsPlanner;
import org.apache.paimon.operation.expire.SnapshotExpireTask;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Expire snapshots action for Flink.
 *
 * <p>This action supports both serial and parallel execution modes based on parallelism:
 *
 * <ul>
 *   <li>Serial mode (parallelism is null or <= 1): Executes locally or starts a local Flink job
 *   <li>Parallel mode (parallelism > 1): Uses Flink distributed execution for deletion
 * </ul>
 *
 * <p>In parallel mode:
 *
 * <ul>
 *   <li>Worker phase (map): deletes data files/changelog files in parallel
 *   <li>Sink phase (commit): deletes manifests and snapshot metadata serially to avoid concurrent
 *       deletion issues
 * </ul>
 */
public class ExpireSnapshotsAction extends ActionBase implements LocalAction {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsAction.class);

    private final String database;
    private final String table;

    private final Integer retainMax;
    private final Integer retainMin;
    private final String olderThan;
    private final Integer maxDeletes;
    private final String options;
    private final Integer parallelism;

    public ExpireSnapshotsAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            @Nullable Integer retainMax,
            @Nullable Integer retainMin,
            @Nullable String olderThan,
            @Nullable Integer maxDeletes,
            @Nullable String options,
            @Nullable Integer parallelism) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.retainMax = retainMax;
        this.retainMin = retainMin;
        this.olderThan = olderThan;
        this.maxDeletes = maxDeletes;
        this.options = options;
        this.parallelism = parallelism;
    }

    /** Returns true if forceStartFlinkJob is enabled and parallelism is greater than 1. */
    private boolean isParallelMode() {
        return forceStartFlinkJob && parallelism != null && parallelism > 1;
    }

    @Override
    public void run() throws Exception {
        if (parallelism != null && parallelism > 1 && !forceStartFlinkJob) {
            throw new IllegalArgumentException(
                    "Parallel expire mode requires both --parallelism > 1 and --force_start_flink_job enabled.");
        }
        if (isParallelMode()) {
            // Parallel mode: build and execute Flink job (multi parallelism)
            build();
            execute(this.getClass().getSimpleName());
        } else if (forceStartFlinkJob) {
            // Serial mode but forced to run as Flink job (single parallelism)
            super.run();
        } else {
            // Serial mode: execute locally
            executeLocally();
        }
    }

    @Override
    public void executeLocally() throws Exception {
        ExpireSnapshotsProcedure expireSnapshotsProcedure = new ExpireSnapshotsProcedure();
        expireSnapshotsProcedure.withCatalog(catalog);
        expireSnapshotsProcedure.call(
                null, database + "." + table, retainMax, retainMin, olderThan, maxDeletes, options);
    }

    @Override
    public void build() throws Exception {
        if (!isParallelMode()) {
            // Not in parallel mode, nothing to build
            return;
        }

        // Prepare table and config using shared method
        Pair<FileStoreTable, ExpireConfig> prepared =
                resolveExpireTableAndConfig(
                        catalog.getTable(Identifier.fromString(database + "." + table)),
                        options,
                        retainMax,
                        retainMin,
                        olderThan,
                        maxDeletes);
        FileStoreTable fileStoreTable = prepared.getLeft();
        ExpireConfig expireConfig = prepared.getRight();

        // Create planner using factory method
        ExpireSnapshotsPlanner planner = ExpireSnapshotsPlanner.create(fileStoreTable);

        // Plan the expiration
        ExpireSnapshotsPlan plan = planner.plan(expireConfig);
        if (plan.isEmpty()) {
            LOG.info("No snapshots to expire");
            return;
        }

        LOG.info(
                "Planning to expire {} snapshots, range=[{}, {})",
                plan.endExclusiveId() - plan.beginInclusiveId(),
                plan.beginInclusiveId(),
                plan.endExclusiveId());

        Identifier identifier = new Identifier(database, table);

        // Build worker phase
        DataStream<DeletionReport> reports = buildWorkerPhase(plan, identifier);

        // Build sink phase
        buildSinkPhase(reports, plan, identifier);
    }

    /**
     * Build the worker phase of the Flink job.
     *
     * <p>Workers process data file and changelog file deletion tasks in parallel. Tasks are
     * partitioned by snapshot range to ensure:
     *
     * <ul>
     *   <li>Same snapshot range tasks are processed by the same worker
     *   <li>Within each worker, data files are deleted before changelog files
     *   <li>Cache locality is maximized (adjacent snapshots often share manifest files)
     * </ul>
     */
    private DataStream<DeletionReport> buildWorkerPhase(
            ExpireSnapshotsPlan plan, Identifier identifier) {
        // Partition by snapshot range: each worker gets a contiguous range of snapshots
        // with dataFileTasks first, then changelogFileTasks
        List<List<SnapshotExpireTask>> partitionedGroups =
                plan.partitionTasksBySnapshotRange(parallelism);

        DataStreamSource<List<SnapshotExpireTask>> source =
                env.fromCollection(partitionedGroups).setParallelism(1);

        return source.rebalance()
                .flatMap(
                        new RangePartitionedExpireFunction(
                                catalogOptions.toMap(),
                                identifier,
                                plan.protectionSet().taggedSnapshots()))
                // Use JavaTypeInfo to ensure proper Java serialization of DeletionReport,
                // avoiding Kryo's FieldSerializer which cannot handle BinaryRow correctly.
                // This approach is compatible with both Flink 1.x and 2.x.
                .returns(new JavaTypeInfo<>(DeletionReport.class))
                .setParallelism(parallelism)
                .name("RangePartitionedExpire");
    }

    /**
     * Build the sink phase of the Flink job.
     *
     * <p>The sink collects deletion reports from workers, then serially deletes manifests and
     * snapshot metadata files to avoid concurrent deletion issues.
     */
    private void buildSinkPhase(
            DataStream<DeletionReport> reports, ExpireSnapshotsPlan plan, Identifier identifier) {
        reports.sinkTo(
                        new SnapshotExpireSink(
                                catalogOptions.toMap(),
                                identifier,
                                plan.endExclusiveId(),
                                plan.protectionSet().manifestSkippingSet(),
                                plan.manifestTasks(),
                                plan.snapshotFileTasks()))
                .setParallelism(1)
                .name("SnapshotExpire");
    }

    /**
     * Prepares the table with dynamic options and builds the ExpireConfig.
     *
     * @param table the original table
     * @param options dynamic options string
     * @param retainMax maximum snapshots to retain
     * @param retainMin minimum snapshots to retain
     * @param olderThan expire snapshots older than this timestamp
     * @param maxDeletes maximum number of snapshots to delete
     * @return a pair of (FileStoreTable with dynamic options applied, ExpireConfig)
     */
    private Pair<FileStoreTable, ExpireConfig> resolveExpireTableAndConfig(
            Table table,
            String options,
            Integer retainMax,
            Integer retainMin,
            String olderThan,
            Integer maxDeletes) {
        HashMap<String, String> dynamicOptions = new HashMap<>();
        ProcedureUtils.putAllOptions(dynamicOptions, options);
        FileStoreTable fileStoreTable = (FileStoreTable) table.copy(dynamicOptions);

        CoreOptions tableOptions = fileStoreTable.store().options();
        ExpireConfig expireConfig =
                ProcedureUtils.fillInSnapshotOptions(
                                tableOptions, retainMax, retainMin, olderThan, maxDeletes)
                        .build();
        return Pair.of(fileStoreTable, expireConfig);
    }
}
