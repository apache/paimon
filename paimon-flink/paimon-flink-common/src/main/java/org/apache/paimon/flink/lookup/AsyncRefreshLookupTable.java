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

import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.ExecutorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC;
import static org.apache.paimon.flink.FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT;

/** A {@link LookupTable} supports async refresh. */
public abstract class AsyncRefreshLookupTable implements LookupTable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncRefreshLookupTable.class);
    private final FileStoreTable table;

    private final int maxPendingSnapshotCount;

    @Nullable private final ExecutorService refreshExecutor;

    private final AtomicReference<Exception> cachedException;

    private Future<?> refreshFuture;

    protected final boolean refreshAsync;

    public AsyncRefreshLookupTable(FileStoreTable table) {
        Options options = Options.fromMap(table.options());
        this.table = table;
        this.refreshAsync = options.get(LOOKUP_REFRESH_ASYNC);
        this.refreshExecutor =
                this.refreshAsync
                        ? Executors.newSingleThreadExecutor(
                                new ExecutorThreadFactory(
                                        String.format(
                                                "%s-lookup-refresh",
                                                Thread.currentThread().getName())))
                        : null;
        this.cachedException = new AtomicReference<>();
        this.maxPendingSnapshotCount = options.get(LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT);
    }

    @Override
    public void refresh() throws Exception {
        if (!refreshAsync) {
            doRefresh();
            return;
        }

        Long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        Long nextSnapshotId = nextSnapshotId();
        if (latestSnapshotId != null
                && nextSnapshotId != null
                && latestSnapshotId - nextSnapshotId > maxPendingSnapshotCount) {
            LOG.warn(
                    "The latest snapshot id {} is much greater than the next snapshot id {} for {}}, "
                            + "you may need to increase the parallelism of lookup operator.",
                    latestSnapshotId,
                    nextSnapshotId,
                    maxPendingSnapshotCount);
            sync();
        }

        refreshAsync();
    }

    private void refreshAsync() {
        try {
            refreshFuture =
                    refreshExecutor.submit(
                            () -> {
                                try {
                                    doRefresh();
                                } catch (Exception e) {
                                    LOG.error("Refresh lookup table {} failed", table.name(), e);
                                    cachedException.set(e);
                                }
                            });
        } catch (RejectedExecutionException e) {
            LOG.warn("Add refresh task for lookup table {} failed", table.name(), e);
        }
    }

    /** Wait until the previous refresh task to be finished. */
    public void sync() throws Exception {
        if (refreshFuture != null) {
            this.refreshFuture.get();
        }
    }

    public abstract void doRefresh() throws Exception;

    public abstract Long nextSnapshotId();

    @Override
    public void close() throws IOException {
        if (refreshExecutor != null) {
            ExecutorUtils.gracefulShutdown(1L, TimeUnit.MINUTES, refreshExecutor);
        }
    }
}
