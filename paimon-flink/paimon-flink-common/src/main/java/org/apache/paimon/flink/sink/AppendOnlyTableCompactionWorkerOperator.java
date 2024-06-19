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
import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.flink.source.BucketUnawareCompactSource;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitApi;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Operator to execute {@link AppendOnlyCompactionTask} passed from {@link
 * BucketUnawareCompactSource}.
 */
public class AppendOnlyTableCompactionWorkerOperator
        extends PrepareCommitOperator<AppendOnlyCompactionTask, Committable> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AppendOnlyTableCompactionWorkerOperator.class);

    private final FileStoreTable table;
    private final String commitUser;

    private transient AppendOnlyFileStoreWrite write;
    private transient ExecutorService lazyCompactExecutor;
    private transient Queue<Future<CommitMessage>> result;

    public AppendOnlyTableCompactionWorkerOperator(FileStoreTable table, String commitUser) {
        super(Options.fromMap(table.options()));
        this.table = table;
        this.commitUser = commitUser;
    }

    @VisibleForTesting
    Iterable<Future<CommitMessage>> result() {
        return result;
    }

    @Override
    public void open() throws Exception {
        LOG.debug("Opened a append-only table compaction worker.");
        this.write = (AppendOnlyFileStoreWrite) table.store().newWrite(commitUser);
        this.result = new LinkedList<>();
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<CommitMessage> tempList = new ArrayList<>();
        try {
            while (!result.isEmpty()) {
                Future<CommitMessage> future = result.peek();
                if (!future.isDone() && !waitCompaction) {
                    break;
                }

                result.poll();
                tempList.add(future.get());
            }
            return tempList.stream()
                    .map(s -> new Committable(checkpointId, Committable.Kind.FILE, s))
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting tasks done.", e);
        } catch (Exception e) {
            throw new RuntimeException("Encountered an error while do compaction", e);
        }
    }

    @Override
    public void processElement(StreamRecord<AppendOnlyCompactionTask> element) throws Exception {
        AppendOnlyCompactionTask task = element.getValue();
        result.add(workerExecutor().submit(() -> task.doCompact(write)));
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
        shutdown();
    }

    @VisibleForTesting
    void shutdown() throws Exception {
        if (lazyCompactExecutor != null) {
            // ignore runnable tasks in queue
            lazyCompactExecutor.shutdownNow();
            if (!lazyCompactExecutor.awaitTermination(120, TimeUnit.SECONDS)) {
                LOG.warn(
                        "Executors shutdown timeout, there may be some files aren't deleted correctly");
            }
            List<CommitMessage> messages = new ArrayList<>();
            for (Future<CommitMessage> resultFuture : result) {
                if (!resultFuture.isDone()) {
                    // the later tasks should be stopped running
                    break;
                }
                try {
                    messages.add(resultFuture.get());
                } catch (Exception exception) {
                    // exception should already be handled
                }
            }
            if (messages.isEmpty()) {
                return;
            }

            try (TableCommitApi tableCommit = table.newCommit(commitUser)) {
                tableCommit.abort(messages);
            }
        }
    }
}
