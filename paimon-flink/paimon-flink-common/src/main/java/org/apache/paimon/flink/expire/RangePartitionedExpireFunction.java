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
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
public class RangePartitionedExpireFunction
        extends RichFlatMapFunction<List<SnapshotExpireTask>, DeletionReport> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RangePartitionedExpireFunction.class);

    private final Map<String, String> catalogConfig;
    private final Identifier identifier;
    private final List<Snapshot> taggedSnapshots;
    private final Map<Long, Snapshot> snapshotCache;
    private final boolean changelogDecoupled;

    protected transient SnapshotExpireContext context;

    public RangePartitionedExpireFunction(
            Map<String, String> catalogConfig,
            Identifier identifier,
            List<Snapshot> taggedSnapshots,
            Map<Long, Snapshot> snapshotCache,
            boolean changelogDecoupled) {
        this.catalogConfig = catalogConfig;
        this.identifier = identifier;
        this.taggedSnapshots = taggedSnapshots;
        this.snapshotCache = snapshotCache;
        this.changelogDecoupled = changelogDecoupled;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.context = initContext();
    }

    /**
     * Initializes and returns the context for processing expire tasks. Subclasses can override this
     * method to provide a custom context for testing without catalog access.
     */
    protected SnapshotExpireContext initContext() throws Exception {
        Options options = Options.fromMap(catalogConfig);
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        SnapshotDeletion deletion = table.store().newSnapshotDeletion();
        deletion.setChangelogDecoupled(changelogDecoupled);
        return new SnapshotExpireContext(
                table.snapshotManager(), deletion, null, taggedSnapshots, snapshotCache);
    }

    @Override
    public void flatMap(List<SnapshotExpireTask> tasks, Collector<DeletionReport> out)
            throws Exception {
        LOG.info("Start processing {} expire tasks", tasks.size());
        long start = System.currentTimeMillis();
        for (int i = 0; i < tasks.size(); i++) {
            SnapshotExpireTask task = tasks.get(i);
            LOG.info("Processing expire task {}/{}, {}", i + 1, tasks.size(), task);
            DeletionReport report = task.execute(context);
            report.setDeletionBuckets(context.snapshotDeletion().drainDeletionBuckets());
            out.collect(report);
        }
        LOG.info(
                "End processing {} expire tasks, spend {} ms",
                tasks.size(),
                System.currentTimeMillis() - start);
    }
}
