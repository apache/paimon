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

package org.apache.paimon.flink.sink.partition;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Coordinator for collecting and broadcasting global partition data statistics. */
class DataStatisticsCoordinator implements OperatorCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinator.class);

    private final String operatorName;
    private final OperatorCoordinator.Context context;
    private final ExecutorService coordinatorExecutor;
    private final SubtaskGateways subtaskGateways;
    private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;
    private final TypeSerializer<DataStatistics> statisticsSerializer;

    private transient boolean started;
    private transient AggregatedStatisticsTracker aggregatedStatisticsTracker;

    DataStatisticsCoordinator(String operatorName, OperatorCoordinator.Context context) {
        this.operatorName = operatorName;
        this.context = context;
        this.coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        "DataStatisticsCoordinator-" + operatorName,
                        context.getUserCodeClassloader());
        this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        this.subtaskGateways = new SubtaskGateways(operatorName, context.currentParallelism());
        this.statisticsSerializer = new DataStatisticsSerializer();
    }

    @Override
    public void start() throws Exception {
        LOG.debug("Starting data statistics coordinator: {}.", operatorName);
        this.started = true;
        this.aggregatedStatisticsTracker =
                new AggregatedStatisticsTracker(operatorName, context.currentParallelism());
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        runInCoordinatorThread(
                () -> {
                    LOG.debug(
                            "Handling event from subtask {} (#{}) of {}: {}",
                            subtask,
                            attemptNumber,
                            operatorName,
                            event);
                    if (event instanceof StatisticsEvent) {
                        handleDataStatisticRequest(subtask, (StatisticsEvent) event);
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid operator event type: "
                                        + event.getClass().getCanonicalName());
                    }
                },
                String.format(
                        Locale.ROOT,
                        "handling operator event %s from subtask %d (#%d)",
                        event.getClass(),
                        subtask,
                        attemptNumber));
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        resultFuture.complete(new byte[0]);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        checkState(
                !started,
                "The coordinator %s can only be reset if it was not yet started",
                operatorName);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        runInCoordinatorThread(
                () -> {
                    LOG.info(
                            "Operator {} subtask {} is reset to checkpoint {}",
                            operatorName,
                            subtask,
                            checkpointId);
                    subtaskGateways.reset(subtask);
                },
                String.format(
                        Locale.ROOT,
                        "handling subtask %d recovery to checkpoint %d",
                        subtask,
                        checkpointId));
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        runInCoordinatorThread(
                () -> {
                    LOG.info(
                            "Unregistering gateway after failure for subtask {} (#{}) of {}",
                            subtask,
                            attemptNumber,
                            operatorName);
                    subtaskGateways.unregisterSubtaskGateway(subtask, attemptNumber);
                },
                String.format(
                        Locale.ROOT, "handling subtask %d (#%d) failure", subtask, attemptNumber));
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        checkArgument(subtask == gateway.getSubtask());
        checkArgument(attemptNumber == gateway.getExecution().getAttemptNumber());
        runInCoordinatorThread(
                () -> subtaskGateways.registerSubtaskGateway(gateway),
                String.format(
                        Locale.ROOT,
                        "making event gateway to subtask %d (#%d) available",
                        subtask,
                        attemptNumber));
    }

    @Override
    public void close() throws Exception {
        coordinatorExecutor.shutdown();
        this.aggregatedStatisticsTracker = null;
        this.started = false;
        LOG.info("Closed data statistics coordinator: {}.", operatorName);
    }

    private void runInCoordinatorThread(Runnable runnable) {
        this.coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                this.coordinatorThreadFactory.uncaughtException(
                                        Thread.currentThread(), throwable),
                        runnable));
    }

    private void runInCoordinatorThread(ThrowingRunnable<Throwable> action, String actionString) {
        ensureStarted();
        runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                        LOG.error(
                                "Uncaught exception in the data statistics coordinator: {} while {}. Triggering job failover",
                                operatorName,
                                actionString,
                                t);
                        context.failJob(t);
                    }
                });
    }

    private void ensureStarted() {
        checkState(started, "The coordinator of %s has not started yet.", operatorName);
    }

    private void handleDataStatisticRequest(int subtask, StatisticsEvent event) {
        DataStatistics maybeCompleted =
                aggregatedStatisticsTracker.updateAndCheckCompletion(subtask, event);
        if (maybeCompleted != null) {
            if (maybeCompleted.isEmpty()) {
                LOG.debug(
                        "Skip aggregated statistics for checkpoint {} as it is empty.",
                        event.getCheckpointId());
            } else {
                LOG.debug(
                        "Completed statistics aggregation for checkpoint {}",
                        event.getCheckpointId());
                sendGlobalStatisticsToSubtasks(maybeCompleted, event.getCheckpointId());
            }
        }
    }

    private void sendGlobalStatisticsToSubtasks(DataStatistics statistics, long checkpointId) {
        LOG.info(
                "Broadcast latest global statistics from checkpoint {} to all subtasks",
                checkpointId);
        StatisticsEvent statisticsEvent =
                StatisticsEvent.createStatisticsEvent(
                        checkpointId, statistics, statisticsSerializer);
        for (int i = 0; i < context.currentParallelism(); ++i) {
            final int subtaskIndex = i;
            subtaskGateways
                    .getSubtaskGateway(subtaskIndex)
                    .sendEvent(statisticsEvent)
                    .whenComplete(
                            (ack, error) -> {
                                if (error != null) {
                                    LOG.warn(
                                            "Failed to send global statistics to subtask {}",
                                            subtaskIndex,
                                            error);
                                }
                            });
        }
    }

    static class SubtaskGateways {
        private final String operatorName;
        private final Map<Integer, SubtaskGateway>[] gateways;

        @SuppressWarnings("unchecked")
        SubtaskGateways(String operatorName, int parallelism) {
            this.operatorName = operatorName;
            gateways = new Map[parallelism];
            for (int i = 0; i < parallelism; ++i) {
                gateways[i] = new HashMap<>();
            }
        }

        void registerSubtaskGateway(OperatorCoordinator.SubtaskGateway gateway) {
            int subtaskIndex = gateway.getSubtask();
            int attemptNumber = gateway.getExecution().getAttemptNumber();
            checkState(
                    !gateways[subtaskIndex].containsKey(attemptNumber),
                    "Coordinator of %s already has a subtask gateway for %d (#%d)",
                    operatorName,
                    subtaskIndex,
                    attemptNumber);
            gateways[subtaskIndex].put(attemptNumber, gateway);
        }

        void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
            gateways[subtaskIndex].remove(attemptNumber);
        }

        OperatorCoordinator.SubtaskGateway getSubtaskGateway(int subtaskIndex) {
            checkState(
                    !gateways[subtaskIndex].isEmpty(),
                    "Coordinator of %s subtask %d is not ready yet to receive events",
                    operatorName,
                    subtaskIndex);
            return gateways[subtaskIndex].values().iterator().next();
        }

        void reset(int subtaskIndex) {
            gateways[subtaskIndex].clear();
        }
    }

    private static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private final String coordinatorThreadName;
        private final ClassLoader classLoader;
        private final Thread.UncaughtExceptionHandler errorHandler;

        private Thread thread;

        CoordinatorExecutorThreadFactory(
                String coordinatorThreadName, ClassLoader contextClassLoader) {
            this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
        }

        CoordinatorExecutorThreadFactory(
                String coordinatorThreadName,
                ClassLoader contextClassLoader,
                Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.classLoader = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(@Nonnull Runnable runnable) {
            thread = new Thread(runnable, coordinatorThreadName);
            thread.setContextClassLoader(classLoader);
            thread.setUncaughtExceptionHandler(this);
            return thread;
        }

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e);
        }
    }
}
