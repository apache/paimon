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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.operation.expire.DeletionReport;
import org.apache.paimon.operation.expire.ExpireSnapshotsExecutor;
import org.apache.paimon.operation.expire.SnapshotExpireTask;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sink that collects deletion reports from parallel expire workers, aggregates the results, and
 * performs the final commit operations using Sink V2 API.
 *
 * <p>In the sink phase (committer), this sink:
 *
 * <ul>
 *   <li>Collects all deletion reports from workers
 *   <li>Deletes manifest files serially in snapshot ID order (to avoid concurrent deletion issues)
 *   <li>Deletes snapshot metadata files
 *   <li>Commits changelogs (for changelogDecoupled mode)
 *   <li>Cleans empty directories
 *   <li>Updates earliest hint
 * </ul>
 */
public class SnapshotExpireSink implements Sink<DeletionReport> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotExpireSink.class);

    private final Map<String, String> catalogConfig;
    private final Identifier identifier;
    private final long endExclusiveId;
    private final Set<String> manifestSkippingSet;
    private final List<SnapshotExpireTask> manifestTasks;
    private final List<SnapshotExpireTask> snapshotFileTasks;

    public SnapshotExpireSink(
            Map<String, String> catalogConfig,
            Identifier identifier,
            long endExclusiveId,
            Set<String> manifestSkippingSet,
            List<SnapshotExpireTask> manifestTasks,
            List<SnapshotExpireTask> snapshotFileTasks) {
        this.catalogConfig = catalogConfig;
        this.identifier = identifier;
        this.endExclusiveId = endExclusiveId;
        this.manifestSkippingSet = manifestSkippingSet;
        this.manifestTasks = manifestTasks;
        this.snapshotFileTasks = snapshotFileTasks;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public SinkWriter<DeletionReport> createWriter(InitContext context) throws IOException {
        return new ExpireSinkWriter(initExecutor());
    }

    @Override
    public SinkWriter<DeletionReport> createWriter(WriterInitContext context) throws IOException {
        return new ExpireSinkWriter(initExecutor());
    }

    /**
     * Initializes and returns the executor. Subclasses can override this method to provide a custom
     * executor for testing.
     */
    @VisibleForTesting
    protected ExpireSnapshotsExecutor initExecutor() {
        try {
            Options options = Options.fromMap(catalogConfig);
            Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            return new ExpireSnapshotsExecutor(
                    table.snapshotManager(),
                    table.store().newSnapshotDeletion(),
                    table.changelogManager());
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize executor", e);
        }
    }

    /** SinkWriter that collects reports and performs commit on flush. */
    private class ExpireSinkWriter implements SinkWriter<DeletionReport> {

        private final Map<BinaryRow, Set<Integer>> globalDeletionBuckets = new HashMap<>();
        private final ExpireSnapshotsExecutor executor;

        ExpireSinkWriter(ExpireSnapshotsExecutor executor) {
            this.executor = executor;
        }

        @Override
        public void write(DeletionReport report, Context context) {
            if (!report.isSkipped()) {
                report.deletionBuckets()
                        .forEach(
                                (partition, buckets) ->
                                        globalDeletionBuckets
                                                .computeIfAbsent(partition, k -> new HashSet<>())
                                                .addAll(buckets));
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            if (!endOfInput) {
                return;
            }

            LOG.info(
                    "Expire sink received: {} manifest tasks, {} snapshot tasks",
                    manifestTasks.size(),
                    snapshotFileTasks.size());

            // 1. Clean empty directories
            executor.cleanEmptyDirectories(globalDeletionBuckets);

            // 2. Execute manifest deletion tasks
            if (manifestSkippingSet != null) {
                Set<String> skippingSet = new HashSet<>(manifestSkippingSet);
                for (SnapshotExpireTask task : manifestTasks) {
                    executor.execute(task, null, skippingSet);
                }
            }

            // 3. Execute snapshot file deletion tasks
            for (SnapshotExpireTask task : snapshotFileTasks) {
                executor.execute(task, null, null);
            }

            executor.writeEarliestHint(endExclusiveId);
        }

        @Override
        public void close() {}
    }
}
