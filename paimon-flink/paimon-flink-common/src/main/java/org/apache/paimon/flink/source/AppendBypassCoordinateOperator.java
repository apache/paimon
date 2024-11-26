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

package org.apache.paimon.flink.source;

import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.append.UnawareAppendTableCompactionCoordinator;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ExecutorUtils;

import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Either;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/**
 * A {@link OneInputStreamOperator} to accept commit messages and send append compact coordinate
 * compact task to downstream operators.
 */
public class AppendBypassCoordinateOperator<CommitT>
        extends AbstractStreamOperator<Either<CommitT, UnawareAppendCompactionTask>>
        implements OneInputStreamOperator<CommitT, Either<CommitT, UnawareAppendCompactionTask>>,
                ProcessingTimeCallback {

    private static final long MAX_PENDING_TASKS = 5000;

    private final FileStoreTable table;

    private transient ScheduledExecutorService executorService;
    private transient LinkedBlockingQueue<UnawareAppendCompactionTask> compactTasks;

    public AppendBypassCoordinateOperator(
            FileStoreTable table, ProcessingTimeService processingTimeService) {
        this.table = table;
        this.processingTimeService = processingTimeService;
        this.chainingStrategy = ChainingStrategy.HEAD;
    }

    @Override
    public void open() throws Exception {
        super.open();
        checkArgument(
                RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()) == 1,
                "Compaction Coordinator parallelism in paimon MUST be one.");
        long intervalMs = table.coreOptions().continuousDiscoveryInterval().toMillis();
        this.compactTasks = new LinkedBlockingQueue<>();
        UnawareAppendTableCompactionCoordinator coordinator =
                new UnawareAppendTableCompactionCoordinator(table, true, null);
        this.executorService =
                Executors.newSingleThreadScheduledExecutor(
                        newDaemonThreadFactory("Compaction Coordinator"));
        this.executorService.scheduleWithFixedDelay(
                () -> asyncPlan(coordinator), 0, intervalMs, TimeUnit.MILLISECONDS);
        this.getProcessingTimeService().scheduleWithFixedDelay(this, 0, intervalMs);
    }

    private void asyncPlan(UnawareAppendTableCompactionCoordinator coordinator) {
        while (compactTasks.size() < MAX_PENDING_TASKS) {
            List<UnawareAppendCompactionTask> tasks = coordinator.run();
            compactTasks.addAll(tasks);
            if (tasks.isEmpty()) {
                break;
            }
        }
    }

    @Override
    public void onProcessingTime(long time) {
        while (true) {
            UnawareAppendCompactionTask task = compactTasks.poll();
            if (task == null) {
                return;
            }
            output.collect(new StreamRecord<>(Either.Right(task)));
        }
    }

    @Override
    public void processElement(StreamRecord<CommitT> record) throws Exception {
        output.collect(new StreamRecord<>(Either.Left(record.getValue())));
    }

    @Override
    public void close() throws Exception {
        ExecutorUtils.gracefulShutdown(1, TimeUnit.MINUTES, executorService);
        super.close();
    }
}
