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

    @Nullable private ExecutorService partitionRefreshExecutor;
    private AtomicReference<LookupTable> pendingLookupTable;
    private AtomicReference<Exception> partitionRefreshException;

    public PartitionRefresher(boolean partitionRefreshAsync, String tableName) {
        this.partitionRefreshAsync = partitionRefreshAsync;
        this.tableName = tableName;
    }

    /** Initialize partition refresh resources. Should be called during table initialization. */
    public void init() {
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

    /**
     * Start partition refresh. Chooses sync or async mode based on configuration.
     *
     * @param newPartitions the new partitions to refresh to
     * @param partitionFilter the partition filter for the new partitions
     * @param lookupTable the current lookup table to refresh
     * @param context the context of the current lookup table
     * @param cacheRowFilter the cache row filter, may be null
     */
    public void startPartitionRefresh(
            List<BinaryRow> newPartitions,
            @Nullable Predicate partitionFilter,
            LookupTable lookupTable,
            FullCacheLookupTable.Context context,
            @Nullable Filter<InternalRow> cacheRowFilter)
            throws Exception {
        if (partitionRefreshAsync) {
            asyncPartitionRefresh(
                    newPartitions, partitionFilter, lookupTable, context, cacheRowFilter);
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
        LOG.info("Synchronous partition refresh completed for table {}.", tableName);
    }

    private void asyncPartitionRefresh(
            List<BinaryRow> newPartitions,
            @Nullable Predicate partitionFilter,
            LookupTable lookupTable,
            FullCacheLookupTable.Context context,
            @Nullable Filter<InternalRow> cacheRowFilter) {

        LOG.info(
                "Starting async partition refresh for table {}, new partitions detected.",
                tableName);

        partitionRefreshExecutor.submit(
                () -> {
                    File newPath = null;
                    try {
                        newPath =
                                new File(
                                        context.tempPath.getParent(),
                                        "lookup-" + UUID.randomUUID());
                        if (!newPath.mkdirs()) {
                            throw new RuntimeException("Failed to create dir: " + newPath);
                        }
                        LookupTable newTable =
                                copyWithNewPath(newPath, context, cacheRowFilter);
                        newTable.specifyPartitions(newPartitions, partitionFilter);
                        newTable.open();

                        pendingLookupTable.set(newTable);
                        LOG.info(
                                "Async partition refresh completed for table {}.", tableName);
                    } catch (Exception e) {
                        LOG.error(
                                "Async partition refresh failed for table {}.", tableName, e);
                        partitionRefreshException.set(e);
                        if (newPath != null) {
                            FileIOUtils.deleteDirectoryQuietly(newPath);
                        }
                    }
                });
    }

    /**
     * Create a new LookupTable instance with the same configuration but a different temp path.
     *
     * @param newPath the new temp path
     * @param context the context of the current lookup table
     * @param cacheRowFilter the cache row filter, may be null
     * @return a new LookupTable instance (not yet opened)
     */
    public LookupTable copyWithNewPath(
            File newPath,
            FullCacheLookupTable.Context context,
            @Nullable Filter<InternalRow> cacheRowFilter) {
        FullCacheLookupTable.Context newContext = context.copy(newPath);
        Options options = Options.fromMap(context.table.options());
        FullCacheLookupTable newTable =
                FullCacheLookupTable.create(newContext, options.get(LOOKUP_CACHE_ROWS));
        if (cacheRowFilter != null) {
            newTable.specifyCacheRowFilter(cacheRowFilter);
        }
        return newTable;
    }

    /**
     * Check if an async partition refresh has completed.
     *
     * @return the new lookup table if ready, or null if no switch is needed
     */
    @Nullable
    public LookupTable checkPartitionRefreshCompletion() throws Exception {
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
}
