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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.flink.compact.UnawareBucketCompactor;
import org.apache.paimon.flink.source.BucketUnawareCompactSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An abstract Operator to execute {@link UnawareAppendCompactionTask} passed from {@link
 * BucketUnawareCompactSource} for compacting table. This operator is always in async mode.
 */
public abstract class AppendCompactWorkerOperator<IN>
        extends PrepareCommitOperator<IN, Committable> {

    private static final Logger LOG = LoggerFactory.getLogger(AppendCompactWorkerOperator.class);

    private final FileStoreTable table;
    private final String commitUser;

    protected transient UnawareBucketCompactor unawareBucketCompactor;

    private transient ExecutorService lazyCompactExecutor;

    public AppendCompactWorkerOperator(FileStoreTable table, String commitUser) {
        super(Options.fromMap(table.options()));
        this.table = table;
        this.commitUser = commitUser;
    }

    @VisibleForTesting
    Iterable<Future<CommitMessage>> result() {
        return unawareBucketCompactor.result();
    }

    @Override
    public void open() throws Exception {
        LOG.debug("Opened a append-only table compaction worker.");
        this.unawareBucketCompactor =
                new UnawareBucketCompactor(
                        table, commitUser, this::workerExecutor, getMetricGroup());
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        return this.unawareBucketCompactor.prepareCommit(waitCompaction, checkpointId);
    }

    private ExecutorService workerExecutor() {
        if (lazyCompactExecutor == null) {
            lazyCompactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName()
                                            + "-append-only-compact-worker"));
        }
        return lazyCompactExecutor;
    }

    @Override
    public void close() throws Exception {
        if (lazyCompactExecutor != null) {
            // ignore runnable tasks in queue
            lazyCompactExecutor.shutdownNow();
            if (!lazyCompactExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
                LOG.warn(
                        "Executors shutdown timeout, there may be some files aren't deleted correctly");
            }
            this.unawareBucketCompactor.close();
        }
    }
}
