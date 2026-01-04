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

package org.apache.paimon.flink.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CompactRefresher;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite.State;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;

import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** The Compactor of unaware bucket table to execute {@link AppendCompactTask}. */
public class AppendTableCompactor {

    private static final Logger LOG = LoggerFactory.getLogger(AppendTableCompactor.class);

    private FileStoreTable table;
    private BaseAppendFileStoreWrite write;

    private final String commitUser;
    protected final Queue<Future<CommitMessage>> result;
    private final Supplier<ExecutorService> compactExecutorsupplier;
    @Nullable private final CompactionMetrics compactionMetrics;
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    @Nullable protected final CompactRefresher compactRefresher;

    public AppendTableCompactor(
            FileStoreTable table,
            String commitUser,
            Supplier<ExecutorService> lazyCompactExecutor,
            @Nullable MetricGroup metricGroup,
            boolean isStreaming) {
        this.table = table;
        this.commitUser = commitUser;
        CoreOptions coreOptions = table.coreOptions();
        this.write = (BaseAppendFileStoreWrite) table.store().newWrite(commitUser);
        if (coreOptions.rowTrackingEnabled()) {
            write.withWriteType(SpecialFields.rowTypeWithRowTracking(table.rowType()));
        }
        this.result = new LinkedList<>();
        this.compactExecutorsupplier = lazyCompactExecutor;
        this.compactionMetrics =
                metricGroup == null
                        ? null
                        : new CompactionMetrics(new FlinkMetricRegistry(metricGroup), table.name());
        this.metricsReporter =
                compactionMetrics == null
                        ? null
                        // partition and bucket fields are no use.
                        : this.compactionMetrics.createReporter(BinaryRow.EMPTY_ROW, 0);
        this.compactRefresher = CompactRefresher.create(isStreaming, table, this::replace);
    }

    public void processElement(AppendCompactTask task) throws Exception {
        result.add(
                compactExecutorsupplier
                        .get()
                        .submit(
                                () -> {
                                    MetricUtils.safeCall(this::startTimer, LOG);

                                    try {
                                        long startMillis = System.currentTimeMillis();
                                        CommitMessage commitMessage = task.doCompact(table, write);
                                        MetricUtils.safeCall(
                                                () -> {
                                                    if (metricsReporter != null) {
                                                        metricsReporter.reportCompactionTime(
                                                                System.currentTimeMillis()
                                                                        - startMillis);
                                                        metricsReporter
                                                                .increaseCompactionsCompletedCount();
                                                    }
                                                },
                                                LOG);
                                        return commitMessage;
                                    } finally {
                                        MetricUtils.safeCall(this::stopTimer, LOG);
                                        MetricUtils.safeCall(
                                                this::decreaseCompactionsQueuedCount, LOG);
                                    }
                                }));
        recordCompactionsQueuedRequest();
    }

    private void recordCompactionsQueuedRequest() {
        if (metricsReporter != null) {
            metricsReporter.increaseCompactionsQueuedCount();
            metricsReporter.increaseCompactionsTotalCount();
        }
    }

    private void decreaseCompactionsQueuedCount() {
        if (metricsReporter != null) {
            metricsReporter.decreaseCompactionsQueuedCount();
        }
    }

    private void startTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().start();
        }
    }

    private void stopTimer() {
        if (metricsReporter != null) {
            metricsReporter.getCompactTimer().finish();
        }
    }

    public void close() throws Exception {
        shutdown();
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG);
        }

        if (compactionMetrics != null) {
            MetricUtils.safeCall(compactionMetrics::close, LOG);
        }
    }

    @VisibleForTesting
    void shutdown() throws Exception {
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

        try (TableCommitImpl tableCommit = table.newCommit(commitUser)) {
            tableCommit.abort(messages);
        }
    }

    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
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
                    .map(s -> new Committable(checkpointId, s))
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting tasks done.", e);
        } catch (Exception e) {
            throw new RuntimeException("Encountered an error while do compaction", e);
        }
    }

    public Iterable<Future<CommitMessage>> result() {
        return result;
    }

    private void replace(FileStoreTable newTable) throws Exception {
        this.table = newTable;
        List<State<InternalRow>> states = write.checkpoint();
        this.write.close();
        this.write = (BaseAppendFileStoreWrite) newTable.store().newWrite(commitUser);
        this.write.restore(states);
    }

    public void tryRefreshWrite(List<DataFileMeta> files) {
        if (commitUser == null) {
            return;
        }
        if (compactRefresher != null && (!files.isEmpty())) {
            compactRefresher.tryRefresh(files);
        }
    }
}
