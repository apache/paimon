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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sink that collects deletion reports from parallel expire workers, aggregates the results, and
 * performs the final commit operations.
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
    private final Map<Long, Snapshot> snapshotCache;
    private final List<SnapshotExpireTask> manifestTasks;
    private final List<SnapshotExpireTask> snapshotFileTasks;
    private final boolean changelogDecoupled;

    public SnapshotExpireSink(
            Map<String, String> catalogConfig,
            Identifier identifier,
            long endExclusiveId,
            Set<String> manifestSkippingSet,
            Map<Long, Snapshot> snapshotCache,
            List<SnapshotExpireTask> manifestTasks,
            List<SnapshotExpireTask> snapshotFileTasks,
            boolean changelogDecoupled) {
        this.catalogConfig = catalogConfig;
        this.identifier = identifier;
        this.endExclusiveId = endExclusiveId;
        this.manifestSkippingSet = manifestSkippingSet;
        this.snapshotCache = snapshotCache;
        this.manifestTasks = manifestTasks;
        this.snapshotFileTasks = snapshotFileTasks;
        this.changelogDecoupled = changelogDecoupled;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public SinkWriter<DeletionReport> createWriter(InitContext initContext) throws IOException {
        return new ExpireSinkWriter(initContext());
    }

    /**
     * Do not annotate with <code>@Override</code> here to maintain compatibility with Flink 1.18-.
     */
    public SinkWriter<DeletionReport> createWriter(WriterInitContext initContext)
            throws IOException {
        return new ExpireSinkWriter(initContext());
    }

    /**
     * Initializes and returns the context. Subclasses can override this method to provide a custom
     * context for testing.
     */
    @VisibleForTesting
    protected SnapshotExpireContext initContext() {
        try {
            Options options = Options.fromMap(catalogConfig);
            Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            SnapshotDeletion deletion = table.store().newSnapshotDeletion();
            deletion.setChangelogDecoupled(changelogDecoupled);
            return new SnapshotExpireContext(
                    table.snapshotManager(),
                    deletion,
                    table.changelogManager(),
                    null,
                    snapshotCache);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize context", e);
        }
    }

    /** SinkWriter that collects reports and performs commit on flush. */
    private class ExpireSinkWriter implements SinkWriter<DeletionReport> {

        private final Map<BinaryRow, Set<Integer>> globalDeletionBuckets = new HashMap<>();
        private final SnapshotExpireContext context;

        ExpireSinkWriter(SnapshotExpireContext context) {
            this.context = context;
        }

        @Override
        public void write(DeletionReport report, Context writeContext) {
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
            context.snapshotDeletion().cleanEmptyDirectories(globalDeletionBuckets);

            // 2. Execute manifest deletion tasks
            if (manifestSkippingSet != null) {
                context.setSkippingSet(new HashSet<>(manifestSkippingSet));
                for (SnapshotExpireTask task : manifestTasks) {
                    task.execute(context);
                }
            }

            // 3. Execute snapshot file deletion tasks
            for (SnapshotExpireTask task : snapshotFileTasks) {
                task.execute(context);
            }

            try {
                context.snapshotManager().commitEarliestHint(endExclusiveId);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {}
    }
}
