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

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.utils.ExceptionUtils;

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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link OperatorCoordinator} for {@link TableWriteOperator}. It receives writer file information
 * and performs global commits in JobManager.
 */
public class PaimonWriterCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonWriterCoordinator.class);

    private final PendingSubtask pendingSubtask;
    private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;
    private final CompletableFuture<Void> finalCheckpointCompleted = new CompletableFuture<>();

    private final OperatorCoordinator.Context context;
    private final CommitterCoordinator<Committable, ?> coordinator;
    private final String initialCommitUser;

    private @Nullable String commitUser;
    private ScheduledExecutorService coordinatorExecutor;
    private boolean started;
    private boolean freshInstance = true;

    public PaimonWriterCoordinator(
            boolean streamingCheckpointEnabled,
            String initialCommitUser,
            Committer.Factory<Committable, ?> committerFactory,
            OperatorCoordinator.Context context,
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            Long endInputWatermark) {
        this.context = context;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.initialCommitUser = initialCommitUser;
        this.coordinator =
                new CommitterCoordinator<>(
                        streamingCheckpointEnabled, committerFactory, endInputWatermark);
        this.pendingSubtask = new PendingSubtask(this.coordinator);
    }

    @Override
    public void start() throws Exception {
        OperatorID operatorId = context.getOperatorId();
        LOG.info("Paimon writer coordinator starting, operatorId={}", operatorId);
        if (commitUser == null) {
            commitUser = initialCommitUser;
        }
        started = true;
        coordinatorExecutor = Executors.newScheduledThreadPool(1, coordinatorThreadFactory);
        int parallelism = context.currentParallelism();
        coordinator.init(parallelism, commitUser);
        pendingSubtask.init(parallelism);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        runInEventLoop(
                () -> pendingSubtask.registerSubtask(subtask, attemptNumber, gateway),
                "registering subtask %d attempt %d",
                subtask,
                attemptNumber);
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable throwable) {
        runInEventLoop(
                () -> pendingSubtask.unregisterSubtask(subtask, attemptNumber, throwable),
                "unregistering subtask %d attempt %d",
                subtask,
                attemptNumber);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
        freshInstance = false;
        throw new UnsupportedOperationException(
                "PWC only accepts file info through coordination requests.");
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        freshInstance = false;
        if (request instanceof FileInfoRequest) {
            return handleFileInfoRequest((FileInfoRequest) request);
        }
        CompletableFuture<CoordinationResponse> result = new CompletableFuture<>();
        result.completeExceptionally(
                new IllegalArgumentException("Unsupported request type: " + request.getClass()));
        return result;
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        freshInstance = false;
        LOG.info("PWC snapshot commitUser={}, checkpointId={}", commitUser, checkpointId);
        checkState(commitUser != null, "PWC has not been started.");
        result.complete(serializeCoordinatorState(commitUser));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        freshInstance = false;
        runInEventLoop(
                () -> {
                    handleCommitResult(pendingSubtask.notifyCheckpointComplete(checkpointId));
                    if (coordinator.isEndInput()) {
                        finalCheckpointCompleted.complete(null);
                    }
                },
                "notifying checkpoint %d complete",
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
                () -> pendingSubtask.notifyCheckpointAborted(checkpointId),
                "notifying checkpoint %d aborted",
                checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] bytes) throws Exception {
        LOG.info("PWC resetToCheckpoint: checkpointId={}, fresh={}", checkpointId, freshInstance);
        if (freshInstance && checkpointId >= 0) {
            checkState(!started, "PWC can only be restored before it is started.");
            commitUser = deserializeCoordinatorState(bytes);
            pendingSubtask.restoreCheckpoint(checkpointId);
        }
        freshInstance = false;
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {}

    @Override
    public void close() throws Exception {
        pendingSubtask.close();
        coordinator.close();
        if (coordinatorExecutor != null) {
            coordinatorExecutor.shutdownNow();
        }
    }

    private void handleCommitResult(CommitResult result) {
        if (!result.committed()) {
            return;
        }
        if (result.restoredCommit() && result.committedCount() > 0) {
            context.failJob(
                    new RecommitRequiredException(result.checkpointId(), result.committedCount()));
            return;
        }
        sendCommitCompleteEvent(result.checkpointId());
    }

    private void sendCommitCompleteEvent(long checkpointId) {
        CommitCompleteEvent event = new CommitCompleteEvent(checkpointId);
        for (SubtaskGateway gateway : pendingSubtask.activeGateways()) {
            gateway.sendEvent(event)
                    .whenComplete(
                            (ignored, error) -> {
                                if (error != null) {
                                    context.failJob(error);
                                }
                            });
        }
    }

    private CompletableFuture<CoordinationResponse> handleFileInfoRequest(FileInfoRequest request) {
        ensureStarted();
        CompletableFuture<CoordinationResponse> result = new CompletableFuture<>();
        runInEventLoop(
                () -> {
                    try {
                        if (!pendingSubtask.isValid(request.subtaskId(), request.attemptNumber())) {
                            result.completeExceptionally(
                                    new IllegalStateException(
                                            String.format(
                                                    "Received file info request from invalid subtask %d attempt %d.",
                                                    request.subtaskId(), request.attemptNumber())));
                            return;
                        }
                        if (request.recovered()) {
                            validateCommitUser(request.commitUser());
                        }
                        handleCommitResult(pendingSubtask.receive(request.subtaskId(), request));
                        result.complete(
                                CoordinationResponseUtils.wrap(
                                        new FileInfoReceivedResponse(
                                                request.checkpointId(), request.subtaskId())));
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                        throw t;
                    }
                },
                "handling file info request %s",
                request);
        return result;
    }

    private void validateCommitUser(@Nullable String recoveredCommitUser) {
        checkState(commitUser != null, "PWC has not been started.");
        checkState(recoveredCommitUser != null, "Recovered writer commit user is null.");
        checkState(
                commitUser.equals(recoveredCommitUser),
                "Writer commit user %s does not match PWC commit user %s.",
                recoveredCommitUser,
                commitUser);
    }

    private static byte[] serializeCoordinatorState(String commitUser) {
        byte[] commitUserBytes = commitUser.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(Integer.BYTES + commitUserBytes.length)
                .putInt(commitUserBytes.length)
                .put(commitUserBytes)
                .array();
    }

    private static String deserializeCoordinatorState(byte[] bytes) {
        if (bytes.length < Integer.BYTES) {
            throw new IllegalArgumentException("Corrupted PWC coordinator state.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int commitUserLength = buffer.getInt();
        if (commitUserLength < 0 || commitUserLength != buffer.remaining()) {
            throw new IllegalArgumentException("Corrupted commit user in PWC coordinator state.");
        }
        byte[] commitUserBytes = new byte[commitUserLength];
        buffer.get(commitUserBytes);
        return new String(commitUserBytes, StandardCharsets.UTF_8);
    }

    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... parameters) {
        ensureStarted();
        coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                coordinatorThreadFactory.uncaughtException(
                                        Thread.currentThread(), throwable),
                        () -> {
                            try {
                                action.run();
                            } catch (Throwable t) {
                                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                                LOG.error(
                                        "Uncaught exception in PWC while {}.",
                                        String.format(actionName, parameters),
                                        t);
                                context.failJob(t);
                            }
                        }));
    }

    public void runInCoordinatorThread(Runnable runnable) {
        ensureStarted();
        coordinatorExecutor.execute(runnable);
    }

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }

    /** Provider for {@link PaimonWriterCoordinator}. */
    public static class WriterCoordinatorProvider implements OperatorCoordinator.Provider {

        private static final long serialVersionUID = 1L;

        private final boolean streamingCheckpointEnabled;
        private final String operatorName;
        private final OperatorID operatorId;
        private final String initialCommitUser;
        private final Committer.Factory<Committable, ?> committerFactory;
        private final Long endInputWatermark;

        public WriterCoordinatorProvider(
                boolean streamingCheckpointEnabled,
                String operatorName,
                OperatorID operatorId,
                String initialCommitUser,
                Committer.Factory<Committable, ?> committerFactory,
                Long endInputWatermark) {
            this.streamingCheckpointEnabled = streamingCheckpointEnabled;
            this.operatorName = operatorName;
            this.operatorId = operatorId;
            this.initialCommitUser = initialCommitUser;
            this.committerFactory = committerFactory;
            this.endInputWatermark = endInputWatermark;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorId;
        }

        @Override
        public OperatorCoordinator create(OperatorCoordinator.Context context) {
            CoordinatorExecutorThreadFactory threadFactory =
                    new CoordinatorExecutorThreadFactory(
                            "PaimonWriterCoordinator-" + operatorName, context);
            return new PaimonWriterCoordinator(
                    streamingCheckpointEnabled,
                    initialCommitUser,
                    committerFactory,
                    context,
                    threadFactory,
                    endInputWatermark);
        }
    }

    /** Thread factory for the single coordinator event loop. */
    public static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        private final String coordinatorThreadName;
        private final ClassLoader classLoader;
        private final Thread.UncaughtExceptionHandler errorHandler;

        @Nullable private Thread thread;

        public CoordinatorExecutorThreadFactory(
                String coordinatorThreadName, OperatorCoordinator.Context context) {
            this(
                    coordinatorThreadName,
                    context.getUserCodeClassloader(),
                    (thread, error) -> context.failJob(error));
        }

        CoordinatorExecutorThreadFactory(
                String coordinatorThreadName,
                ClassLoader classLoader,
                Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.classLoader = classLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(Runnable runnable) {
            checkState(thread == null, "CoordinatorExecutorThreadFactory can create one thread.");
            thread = new Thread(runnable, coordinatorThreadName);
            thread.setContextClassLoader(classLoader);
            thread.setUncaughtExceptionHandler(this);
            return thread;
        }

        @Override
        public synchronized void uncaughtException(Thread thread, Throwable error) {
            errorHandler.uncaughtException(thread, error);
        }
    }

    private static class RecommitRequiredException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private RecommitRequiredException(long checkpointId, int committedCount) {
            super(
                    String.format(
                            "PWC committed %d restored committable(s) up to checkpoint %d. "
                                    + "Triggering global recovery so writers continue from the "
                                    + "latest Paimon snapshot.",
                            committedCount, checkpointId));
        }
    }
}
