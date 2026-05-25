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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link OperatorCoordinator} for {@link TableWriteOperator}, as global commit node to obtain the
 * file info for write operators and do commit.
 */
public class PaimonWriterCoordinator implements OperatorCoordinator, CoordinationRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonWriterCoordinator.class);
    private final PendingSubtask pendingSubtask;

    private final String configuredStateDir;

    private volatile boolean isFreshInstance = true;
    private ScheduledExecutorService coordinatorExecutor;
    private final PaimonWriterCoordinator.CoordinatorExecutorThreadFactory coordinatorThreadFactory;

    private final CompletableFuture<Void> finalCheckpointCompleted = new CompletableFuture<>();
    private OperatorCoordinator.Context operatorCoordinatorContext;

    private CommitterCoordinator coordinator;

    private boolean started;

    public PaimonWriterCoordinator(
            String configuredStateDir,
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            Committer.Factory committerFactory,
            SerializableSupplier committableSerializer,
            Context operatorCoordinatorContext,
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            Long endInputWatermark) {
        this.configuredStateDir = configuredStateDir;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;
        this.coordinator =
                new CommitterCoordinator(
                        streamingCheckpointEnabled,
                        initialCommitUser,
                        committerFactory,
                        committableSerializer,
                        endInputWatermark);
        this.pendingSubtask = new PendingSubtask(this.coordinator);
    }

    @Override
    public void start() throws Exception {
        OperatorID operatorId = operatorCoordinatorContext.getOperatorId();
        LOG.info("PWC starting, operatorId={}", operatorId);

        this.started = true;
        this.coordinatorExecutor = Executors.newScheduledThreadPool(1, coordinatorThreadFactory);
        this.coordinator.initStateManager(operatorId, resolveBaseDir());
        this.coordinator.init(operatorCoordinatorContext.currentParallelism());
    }

    /**
     * Save subtask index and attempt number in coordinator to filter file info from writer task.
     */
    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        runInEventLoop(
                () -> pendingSubtask.registerSubtask(subtask, attemptNumber, gateway),
                "making event gateway to subtask %d (#%d) available",
                subtask,
                attemptNumber);
    }

    /** Failed specify subtask in coordinator. */
    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable throwable) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Removing registered reader after failure for subtask {} (#{}) of Committer.",
                            subtask,
                            attemptNumber);
                    pendingSubtask.unregisterSubtask(subtask, attemptNumber, throwable);
                },
                "handling subtask %d (#%d) failure",
                subtask,
                attemptNumber);
    }

    /**
     * Receive file info from writer, check whether all tasks have been reported and then flush
     * state to HDFS.
     */
    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        this.isFreshInstance = false;
        runInEventLoop(
                () -> {
                    if (!pendingSubtask.isValid(subtask, attemptNumber)) {
                        return;
                    }
                    if (event instanceof FileInfoEvent) {
                        pendingSubtask.receive(subtask, (FileInfoEvent) event);
                    } else if (event instanceof WatermarkEvent) {
                        long watermark = ((WatermarkEvent) event).getWatermark();
                        coordinator.processWaterMark(watermark);
                    }
                },
                "handling operator event %s from subtask %d (#%d)",
                event,
                subtask,
                attemptNumber);
    }

    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        this.isFreshInstance = false;
        if (request instanceof FileInfoConfirmRequest) {
            long checkpointId = ((FileInfoConfirmRequest) request).getCheckpointId();
            return pendingSubtask
                    .waitFor(checkpointId)
                    .thenApply(v -> new FileInfoConfirmResponse(checkpointId));
        }
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        future.completeExceptionally(
                new IllegalArgumentException("Unsupported request type: " + request.getClass()));
        return future;
    }

    @Override
    public void checkpointCoordinator(long l, CompletableFuture<byte[]> completableFuture) {
        this.isFreshInstance = false;
        completableFuture.complete(new byte[0]);
    }

    @Override
    public void close() throws Exception {
        coordinator.close();
        pendingSubtask.close();
        if (coordinatorExecutor != null) {
            coordinatorExecutor.shutdownNow();
        }
    }

    /** Notify coordinator to perform table commit for given checkpoint id. */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        this.isFreshInstance = false;
        runInEventLoop(
                () -> {
                    try {
                        this.coordinator.notifyCheckpointComplete(checkpointId);
                        if (coordinator.isEndInput()) {
                            finalCheckpointCompleted.complete(null);
                        }
                        pendingSubtask.remove(checkpointId);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Fail to do checkpointComplete for checkpoint: " + checkpointId, e);
                    }
                },
                "notifying the enumerator of completion of checkpoint %d",
                checkpointId);
        if (coordinator.isEndInput()) {
            try {
                finalCheckpointCompleted.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        runInEventLoop(
                () -> {
                    LOG.info("Checkpoint-{} aborted, cleaning up", checkpointId);
                    pendingSubtask.abortCheckpoint(checkpointId);
                    coordinator.notifyCheckpointAborted(checkpointId);
                },
                "aborting checkpoint-%d",
                checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] bytes) throws Exception {
        LOG.info(
                "resetToCheckpoint: checkpointId={}, isFreshInstance={}",
                checkpointId,
                isFreshInstance);
        if (isFreshInstance && checkpointId >= 0) {
            // JM HA 恢复
            coordinator.restore(checkpointId);
        }

        isFreshInstance = false;
    }

    private String resolveBaseDir() {
        if (configuredStateDir != null && !configuredStateDir.isEmpty()) {
            return configuredStateDir;
        }

        throw new IllegalStateException(
                "PWC state directory not configured. "
                        + "Please set 'paimon.write-coordinator.state-dir'.");
    }

    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {

        ensureStarted();

        if (coordinator == null) {
            return;
        }

        runInCoordinatorThread(
                () -> {
                    try {
                        action.run();
                    } catch (Throwable t) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        final String actionString =
                                String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the CommitCoordinator while {}. Triggering job failover.",
                                actionString,
                                t);
                        operatorCoordinatorContext.failJob(t);
                    }
                });
    }

    public void runInCoordinatorThread(Runnable runnable) {
        // when using a ScheduledThreadPool, uncaught exception handler catches only
        // exceptions thrown by the threadPool, so manually call it when the exception is
        // thrown by the runnable
        coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                coordinatorThreadFactory.uncaughtException(
                                        Thread.currentThread(), throwable),
                        runnable));
    }

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }

    @Override
    public void subtaskReset(int i, long l) {}

    /** sender for coordinator. */
    public static class WriterCoordinatorProvider implements OperatorCoordinator.Provider {
        private static final long serialVersionUID = 1L;

        protected final boolean streamingCheckpointEnabled;

        private final String operatorName;

        private final String configuredStateDir;

        private final OperatorID operatorId;

        private final String initialCommitUser;

        private final Committer.Factory committerFactory;

        protected final Long endInputWatermark;

        public WriterCoordinatorProvider(
                boolean streamingCheckpointEnabled,
                String operatorName,
                String configuredStateDir,
                OperatorID operatorID,
                String initialCommitUser,
                Committer.Factory committerFactory,
                Long endInputWatermark) {
            this.streamingCheckpointEnabled = streamingCheckpointEnabled;
            this.operatorName = operatorName;
            this.configuredStateDir = configuredStateDir;
            this.operatorId = operatorID;
            this.initialCommitUser = initialCommitUser;
            this.committerFactory = committerFactory;
            this.endInputWatermark = endInputWatermark;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
            final String coordinatorThreadName = " PaimonWriterCoordinator-" + operatorName;
            PaimonWriterCoordinator.CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                    new PaimonWriterCoordinator.CoordinatorExecutorThreadFactory(
                            coordinatorThreadName, context);
            return new PaimonWriterCoordinator(
                    configuredStateDir,
                    streamingCheckpointEnabled,
                    initialCommitUser,
                    committerFactory,
                    ManifestCommittableSerializer::new,
                    context,
                    coordinatorThreadFactory,
                    endInputWatermark);
        }
    }

    /**
     * A thread factory class that provides some helper methods. Because it is used to check the
     * current thread, it is a one-off, do not use this ThreadFactory to create multiple threads.
     */
    public static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private final String coordinatorThreadName;
        private final ClassLoader cl;
        private final Thread.UncaughtExceptionHandler errorHandler;

        @Nullable private Thread t;

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final OperatorCoordinator.Context context) {
            this(
                    coordinatorThreadName,
                    context.getUserCodeClassloader(),
                    (t, e) -> {
                        LOG.error(
                                "Thread '{}' produced an uncaught exception. Failing the job.",
                                t.getName(),
                                e);
                        context.failJob(e);
                    });
        }

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.cl = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(Runnable r) {
            checkState(
                    t == null,
                    "Please using the new CoordinatorExecutorThreadFactory,"
                            + " this factory cannot new multiple threads.");
            t = new Thread(r, coordinatorThreadName);
            t.setContextClassLoader(cl);
            t.setUncaughtExceptionHandler(this);
            return t;
        }

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e);
        }

        String getCoordinatorThreadName() {
            return coordinatorThreadName;
        }

        boolean isCurrentThreadCoordinatorThread() {
            return Thread.currentThread() == t;
        }
    }
}
