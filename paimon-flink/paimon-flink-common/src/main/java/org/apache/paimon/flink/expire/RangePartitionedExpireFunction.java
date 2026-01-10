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

package org.apache.paimon.flink.expire;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.operation.expire.DeletionReport;
import org.apache.paimon.operation.expire.ExpireSnapshotsExecutor;
import org.apache.paimon.operation.expire.SnapshotExpireTask;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Flink flatMap function for range-partitioned snapshot expiration (worker phase).
 *
 * <p>This function processes a batch of {@link SnapshotExpireTask}s that belong to the same
 * contiguous range. Each subtask receives a list of tasks (e.g., subtask 0 gets snap 1-4, subtask 1
 * gets snap 5-8) and processes them sequentially in order.
 *
 * <p>Processing tasks in order within each subtask maximizes cache locality since adjacent
 * snapshots often share manifest files.
 *
 * <p>In worker phase, this function only deletes data files and changelog data files. Manifest and
 * snapshot metadata deletion is deferred to the sink phase to avoid concurrent deletion issues.
 *
 * <p>This function uses {@link ExpireSnapshotsExecutor#execute} which loads tag data files
 * on-demand with internal caching.
 */
public class RangePartitionedExpireFunction
        extends RichFlatMapFunction<List<SnapshotExpireTask>, DeletionReport> {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> catalogConfig;
    private final Identifier identifier;
    private final List<Snapshot> taggedSnapshots;

    private transient ExpireSnapshotsExecutor executor;

    public RangePartitionedExpireFunction(
            Map<String, String> catalogConfig,
            Identifier identifier,
            List<Snapshot> taggedSnapshots) {
        this.catalogConfig = catalogConfig;
        this.identifier = identifier;
        this.taggedSnapshots = taggedSnapshots;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.executor = initExecutor();
    }

    /**
     * Initializes and returns the executor for processing expire tasks. Subclasses can override
     * this method to provide a custom executor for testing without catalog access.
     *
     * <p>Default implementation creates executor from catalog using {@link #catalogConfig} and
     * {@link #identifier}.
     */
    protected ExpireSnapshotsExecutor initExecutor() throws Exception {
        Options options = Options.fromMap(catalogConfig);
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        return new ExpireSnapshotsExecutor(
                table.snapshotManager(), table.store().newSnapshotDeletion());
    }

    @Override
    public void flatMap(List<SnapshotExpireTask> tasks, Collector<DeletionReport> out)
            throws Exception {
        // Process tasks sequentially in order to maximize cache locality
        for (SnapshotExpireTask task : tasks) {
            DeletionReport report = processTask(task);
            out.collect(report);
        }
    }

    private DeletionReport processTask(SnapshotExpireTask task) {
        // Execute task (worker phase only deletes data files, skippingSet is null)
        DeletionReport report = executor.execute(task, taggedSnapshots, null);
        report.setDeletionBuckets(executor.drainDeletionBuckets());
        return report;
    }
}
