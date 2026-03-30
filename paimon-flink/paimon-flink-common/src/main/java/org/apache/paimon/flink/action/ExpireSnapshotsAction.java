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
import org.apache.paimon.flink.expire.DeletionReport;
import org.apache.paimon.flink.expire.ExpireSnapshotsPlan;
import org.apache.paimon.flink.expire.ExpireSnapshotsPlanner;
import org.apache.paimon.flink.expire.RangePartitionedExpireFunction;
import org.apache.paimon.flink.expire.SnapshotExpireSink;
import org.apache.paimon.flink.expire.SnapshotExpireTask;
import org.apache.paimon.flink.procedure.ExpireSnapshotsProcedure;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * <p>This action supports both local and parallel execution modes:
 *
 * <ul>
 *   <li>Local mode: when {@code forceStartFlinkJob} is false, executes locally via {@link
 *       #executeLocally()}
 *   <li>Flink mode: when {@code forceStartFlinkJob} is true, uses Flink distributed execution for
 *       deletion.
 * </ul>
 *
 * <p>In parallel mode:
 *
 * <ul>
 *   <li>Worker phase (flatMap): deletes data files/changelog files in parallel
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
    private final int parallelism;

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
        this.parallelism = resolveParallelism(parallelism);
    }

    private int resolveParallelism(Integer parallelism) {
        if (parallelism != null) {
            return parallelism;
        }
        int envParallelism = env.getParallelism();
        return envParallelism > 0 ? envParallelism : 1;
    }

    @Override
    public void run() throws Exception {
        if (forceStartFlinkJob) {
            // Flink mode: always build and submit a Flink job
            build();
            execute(this.getClass().getSimpleName());
        } else {
            // Default: ActionBase handles LocalAction → executeLocally()
            super.run();
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
        Identifier identifier = new Identifier(database, table);

        // Prepare table with dynamic options
        HashMap<String, String> dynamicOptions = new HashMap<>();
        ProcedureUtils.putAllOptions(dynamicOptions, options);
        FileStoreTable fileStoreTable =
                (FileStoreTable) catalog.getTable(identifier).copy(dynamicOptions);

        // Build expire config
        CoreOptions tableOptions = fileStoreTable.store().options();
        ExpireConfig expireConfig =
                ProcedureUtils.fillInSnapshotOptions(
                                tableOptions, retainMax, retainMin, olderThan, maxDeletes)
                        .build();

        // Create planner using factory method
        ExpireSnapshotsPlanner planner = ExpireSnapshotsPlanner.create(fileStoreTable);

        // Plan the expiration
        ExpireSnapshotsPlan plan = planner.plan(expireConfig);
        if (plan.isEmpty()) {
            LOG.info("No snapshots to expire.");
        } else {
            LOG.info(
                    "Planning to expire {} snapshots, range=[{}, {})",
                    plan.endExclusiveId() - plan.beginInclusiveId(),
                    plan.beginInclusiveId(),
                    plan.endExclusiveId());
        }

        // Build worker phase
        DataStream<DeletionReport> reports = buildWorkerPhase(plan, identifier, expireConfig);

        // Build sink phase
        buildSinkPhase(reports, plan, identifier, expireConfig);
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
            ExpireSnapshotsPlan plan, Identifier identifier, ExpireConfig expireConfig) {
        // Partition by snapshot range: each worker gets a contiguous range of snapshots
        // with dataFileTasks first, then changelogFileTasks
        List<List<SnapshotExpireTask>> partitionedGroups =
                plan.partitionTasksBySnapshotRange(parallelism);

        DataStreamSource<List<SnapshotExpireTask>> source =
                env.fromCollection(
                                partitionedGroups,
                                TypeInformation.of(new TypeHint<List<SnapshotExpireTask>>() {}))
                        .setParallelism(1);

        return source.rebalance()
                .flatMap(
                        new RangePartitionedExpireFunction(
                                catalogOptions.toMap(),
                                identifier,
                                plan.protectionSet().taggedSnapshots(),
                                plan.snapshotCache(),
                                expireConfig.isChangelogDecoupled()))
                // Use JavaTypeInfo to ensure proper Java serialization of DeletionReport,
                // avoiding Kryo's FieldSerializer which cannot handle BinaryRow correctly.
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
            DataStream<DeletionReport> reports,
            ExpireSnapshotsPlan plan,
            Identifier identifier,
            ExpireConfig expireConfig) {
        reports.sinkTo(
                        new SnapshotExpireSink(
                                catalogOptions.toMap(),
                                identifier,
                                plan.endExclusiveId(),
                                plan.protectionSet().manifestSkippingSet(),
                                plan.snapshotCache(),
                                plan.manifestTasks(),
                                plan.snapshotFileTasks(),
                                expireConfig.isChangelogDecoupled()))
                .setParallelism(1)
                .name("SnapshotExpireCommit");
    }
}
