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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.ExecutorUtils;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.lookup.rocksdb.RocksDBOptions.LOOKUP_CACHE_ROWS;

/** Manages partition refresh logic for {@link FullCacheLookupTable}. */
public class PartitionRefresher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRefresher.class);

    private final boolean partitionRefreshAsync;
    private final String tableName;
    private final String tmpDirectory;
    private volatile File path;
    private ExecutorService partitionRefreshExecutor;
    private AtomicReference<LookupTable> pendingLookupTable;
    private AtomicReference<Exception> partitionRefreshException;

    /** Current partitions being used for lookup. Updated when partition refresh completes. */
    private List<BinaryRow> currentPartitions;

    public PartitionRefresher(
            boolean partitionRefreshAsync,
            String tableName,
            String tmpDirectory,
            List<BinaryRow> initialPartitions) {
        this.partitionRefreshAsync = partitionRefreshAsync;
        this.tableName = tableName;
        this.tmpDirectory = tmpDirectory;
        this.currentPartitions = initialPartitions;
        if (!partitionRefreshAsync) {
            return;
        }
        this.pendingLookupTable = new AtomicReference<>(null);
        this.partitionRefreshException = new AtomicReference<>(null);
        this.partitionRefreshExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                String.format(
                                        "%s-lookup-refresh-partition",
                                        Thread.currentThread().getName())));
    }

    /** Get the current partitions being used for lookup. */
    public List<BinaryRow> currentPartitions() {
        return currentPartitions;
    }

    /**
     * Start partition refresh. Chooses sync or async mode based on configuration.
     *
     * @param newPartitions the new partitions to refresh to
     * @param partitionFilter the partition filter for the new partitions
     * @param lookupTable the current lookup table to refresh
     * @param cacheRowFilter the cache row filter, may be null
     */
    public void startRefresh(
            List<BinaryRow> newPartitions,
            @Nullable Predicate partitionFilter,
            LookupTable lookupTable,
            @Nullable Filter<InternalRow> cacheRowFilter)
            throws Exception {
        if (partitionRefreshAsync) {
            asyncPartitionRefresh(
                    newPartitions,
                    partitionFilter,
                    ((FullCacheLookupTable) lookupTable).context,
                    cacheRowFilter);
        } else {
            syncPartitionRefresh(newPartitions, partitionFilter, lookupTable);
        }
    }

    private void syncPartitionRefresh(
            List<BinaryRow> newPartitions,
            @Nullable Predicate partitionFilter,
            LookupTable lookupTable)
            throws Exception {
        LOG.info(
                "Synchronously refreshing partition for table {}, new partitions detected.",
                tableName);
        lookupTable.close();
        lookupTable.specifyPartitions(newPartitions, partitionFilter);
        lookupTable.open();
        this.currentPartitions = newPartitions;
        LOG.info("Synchronous partition refresh completed for table {}.", tableName);
    }

    private void asyncPartitionRefresh(
            List<BinaryRow> newPartitions,
            @Nullable Predicate partitionFilter,
            FullCacheLookupTable.Context context,
            @Nullable Filter<InternalRow> cacheRowFilter) {

        LOG.info(
                "Starting async partition refresh for table {}, new partitions detected.",
                tableName);

        partitionRefreshExecutor.submit(
                () -> {
                    try {
                        this.path = new File(tmpDirectory, "lookup-" + UUID.randomUUID());
                        if (!path.mkdirs()) {
                            throw new RuntimeException("Failed to create dir: " + path);
                        }
                        FullCacheLookupTable.Context newContext = context.copy(path);
                        Options options = Options.fromMap(context.table.options());
                        FullCacheLookupTable newTable =
                                FullCacheLookupTable.create(
                                        newContext, options.get(LOOKUP_CACHE_ROWS));
                        if (cacheRowFilter != null) {
                            newTable.specifyCacheRowFilter(cacheRowFilter);
                        }
                        newTable.specifyPartitions(newPartitions, partitionFilter);
                        newTable.open();

                        pendingLookupTable.set(newTable);
                        LOG.info("Async partition refresh completed for table {}.", tableName);
                    } catch (Exception e) {
                        LOG.error("Async partition refresh failed for table {}.", tableName, e);
                        partitionRefreshException.set(e);
                        if (path != null) {
                            FileIOUtils.deleteDirectoryQuietly(path);
                        }
                    }
                });
    }

    /**
     * Check if an async partition refresh has completed.
     *
     * @param newPartitions the new partitions to update after refresh completes
     * @return a Pair containing the new lookup table and its temp path if ready, or null if no
     *     switch is needed
     */
    @Nullable
    public LookupTable getNewLookupTable(List<BinaryRow> newPartitions) throws Exception {
        if (!partitionRefreshAsync) {
            return null;
        }

        Exception asyncException = partitionRefreshException.getAndSet(null);
        if (asyncException != null) {
            LOG.error(
                    "Async partition refresh failed for table {}, will stop running.",
                    tableName,
                    asyncException);
            throw asyncException;
        }

        LookupTable newTable = pendingLookupTable.getAndSet(null);
        if (newTable == null) {
            return null;
        }

        this.currentPartitions = newPartitions;
        LOG.info("Switched to new lookup table for table {} with new partitions.", tableName);
        return newTable;
    }

    /** Close partition refresh resources. */
    @Override
    public void close() throws IOException {
        if (partitionRefreshExecutor != null) {
            ExecutorUtils.gracefulShutdown(1L, TimeUnit.MINUTES, partitionRefreshExecutor);
        }
        if (pendingLookupTable != null) {
            LookupTable pending = pendingLookupTable.getAndSet(null);
            if (pending != null) {
                pending.close();
            }
        }
    }

    public boolean isPartitionRefreshAsync() {
        return partitionRefreshAsync;
    }

    public File path() {
        return path;
    }
}
