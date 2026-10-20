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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTaskSerializer;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactionCommitPreparation;
import org.apache.paimon.operation.commit.DataEvolutionRowRangeConflictException;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.RetryWaiter;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Executes planned data-evolution rewrite tasks in bounded Spark batches. */
final class DataEvolutionRewriteExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionRewriteExecutor.class);

    private DataEvolutionRewriteExecutor() {}

    static void execute(
            FileStoreTable table,
            Snapshot initialSnapshot,
            Function<Snapshot, List<DataEvolutionCompactTask>> taskPlanner,
            JavaSparkContext javaSparkContext,
            SparkSession sparkSession,
            CommitConfigurer commitConfigurer) {
        execute(
                table,
                initialSnapshot,
                taskPlanner,
                javaSparkContext,
                sparkSession,
                commitConfigurer,
                null);
    }

    static void execute(
            FileStoreTable table,
            Snapshot initialSnapshot,
            Function<Snapshot, List<DataEvolutionCompactTask>> taskPlanner,
            JavaSparkContext javaSparkContext,
            SparkSession sparkSession,
            CommitConfigurer commitConfigurer,
            @Nullable CommitMessageRewriter commitMessageRewriter) {
        execute(
                table,
                initialSnapshot,
                taskPlanner,
                javaSparkContext,
                sparkSession,
                commitConfigurer,
                commitMessageRewriter,
                null);
    }

    static void execute(
            FileStoreTable table,
            Snapshot initialSnapshot,
            Function<Snapshot, List<DataEvolutionCompactTask>> taskPlanner,
            JavaSparkContext javaSparkContext,
            SparkSession sparkSession,
            CommitConfigurer commitConfigurer,
            @Nullable CommitMessageRewriter commitMessageRewriter,
            @Nullable Consumer<List<CommitMessage>> commitObserver) {
        String commitUser = createCommitUser(table.coreOptions().toConfiguration());
        Snapshot preparationSnapshot = initialSnapshot;
        int round = 0;
        try {
            while (true) {
                List<DataEvolutionCompactTask> compactionTasks =
                        taskPlanner.apply(preparationSnapshot);
                round++;
                if (compactionTasks.isEmpty()) {
                    LOG.info(
                            "No data evolution rewrite task planned in round {} for table {}, "
                                    + "continue to scan the next batch.",
                            round,
                            table.fullName());
                    continue;
                }
                try {
                    List<CommitMessage> compactMessages =
                            executeTasks(
                                    table,
                                    compactionTasks,
                                    javaSparkContext,
                                    sparkSession,
                                    commitUser);
                    Snapshot committedSnapshot =
                            commitWithMergeConflictRetry(
                                    table,
                                    preparationSnapshot,
                                    compactMessages,
                                    commitUser,
                                    sparkSession,
                                    commitConfigurer,
                                    commitMessageRewriter,
                                    commitObserver);
                    checkArgument(
                            committedSnapshot.id() > preparationSnapshot.id(),
                            "Committed data evolution rewrite snapshot %s must be newer than preparation snapshot %s.",
                            committedSnapshot.id(),
                            preparationSnapshot.id());
                    preparationSnapshot = committedSnapshot;
                } catch (Exception e) {
                    throw new RuntimeException("Execute data evolution rewrite failed.", e);
                }
            }
        } catch (EndOfScanException e) {
            LOG.info(
                    "Catching EndOfScanException, the data evolution rewrite job of table {} is "
                            + "finishing after {} plan rounds.",
                    table.fullName(),
                    round);
        }
    }

    /** Executes one task batch without publishing it, so callers can share a commit. */
    static List<CommitMessage> executeTasks(
            FileStoreTable table,
            List<DataEvolutionCompactTask> tasks,
            JavaSparkContext javaSparkContext,
            SparkSession sparkSession,
            String commitUser)
            throws IOException {
        if (tasks.isEmpty()) {
            return Collections.emptyList();
        }
        DataEvolutionCompactTaskSerializer serializer = new DataEvolutionCompactTaskSerializer();
        List<byte[]> serializedTasks = new ArrayList<>(tasks.size());
        for (DataEvolutionCompactTask task : tasks) {
            serializedTasks.add(serializer.serialize(task));
        }
        int parallelism = readParallelism(serializedTasks, sparkSession);
        LOG.info(
                "Executing {} data evolution rewrite tasks for table {} with parallelism {}.",
                tasks.size(),
                table.fullName(),
                parallelism);
        JavaRDD<byte[]> serializedMessages =
                javaSparkContext
                        .parallelize(serializedTasks, parallelism)
                        .mapPartitions(
                                (FlatMapFunction<Iterator<byte[]>, byte[]>)
                                        taskIterator -> {
                                            DataEvolutionCompactTaskSerializer taskSerializer =
                                                    new DataEvolutionCompactTaskSerializer();
                                            CommitMessageSerializer messageSerializer =
                                                    new CommitMessageSerializer();
                                            List<byte[]> messages = new ArrayList<>();
                                            while (taskIterator.hasNext()) {
                                                DataEvolutionCompactTask task =
                                                        taskSerializer.deserialize(
                                                                taskSerializer.getVersion(),
                                                                taskIterator.next());
                                                messages.add(
                                                        messageSerializer.serialize(
                                                                task.doCompact(table, commitUser)));
                                            }
                                            return messages.iterator();
                                        });
        return deserializeCommitMessagesAndReleaseSerializedBytes(
                new CommitMessageSerializer(), new ArrayList<>(serializedMessages.collect()));
    }

    private static Snapshot commitWithMergeConflictRetry(
            FileStoreTable table,
            Snapshot taskSnapshot,
            List<CommitMessage> compactMessages,
            String commitUser,
            SparkSession sparkSession,
            CommitConfigurer commitConfigurer,
            @Nullable CommitMessageRewriter commitMessageRewriter,
            @Nullable Consumer<List<CommitMessage>> commitObserver) {
        int retryCount = 0;
        long startMillis = System.currentTimeMillis();
        RetryWaiter retryWaiter =
                new RetryWaiter(
                        table.coreOptions().commitMinRetryWait(),
                        table.coreOptions().commitMaxRetryWait());
        RuntimeException lastConflict = null;

        while (true) {
            if (lastConflict != null
                    && System.currentTimeMillis() - startMillis
                            > table.coreOptions().commitTimeout()) {
                throw lastConflict;
            }

            Snapshot attemptSnapshot = taskSnapshot;
            List<CommitMessage> attemptMessages = compactMessages;
            List<CommitMessage> retryArtifacts = Collections.emptyList();
            Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
            if (commitMessageRewriter != null
                    && latestSnapshot != null
                    && latestSnapshot.id() > taskSnapshot.id()) {
                Optional<List<CommitMessage>> rewritten;
                try {
                    rewritten =
                            commitMessageRewriter.rewrite(
                                    sparkSession, taskSnapshot, latestSnapshot, compactMessages);
                } catch (RuntimeException rewriteError) {
                    if (lastConflict == null) {
                        throw rewriteError;
                    }
                    RuntimeException failure =
                            new RuntimeException(
                                    lastConflict.getMessage() + " " + rewriteError.getMessage(),
                                    rewriteError);
                    failure.addSuppressed(lastConflict);
                    throw failure;
                }
                if (rewritten.isPresent()) {
                    attemptSnapshot = latestSnapshot;
                    attemptMessages = rewritten.get();
                    retryArtifacts = retryArtifacts(compactMessages, attemptMessages);
                    LOG.info(
                            "Rebased staged data evolution compact files against compatible "
                                    + "concurrent partial-column files "
                                    + "through snapshot {} for table {}.",
                            latestSnapshot.id(),
                            table.fullName());
                } else if (lastConflict != null) {
                    throw lastConflict;
                }
                if (lastConflict != null
                        && System.currentTimeMillis() - startMillis
                                > table.coreOptions().commitTimeout()) {
                    abortRetryArtifacts(table, commitUser, retryArtifacts, lastConflict);
                    throw lastConflict;
                }
            }

            List<CommitMessage> preparedMessages = new ArrayList<>(attemptMessages);
            List<CommitMessage> preparationArtifacts =
                    new DataEvolutionCompactionCommitPreparation(table, attemptSnapshot)
                            .prepare(preparedMessages);
            preparedMessages.addAll(preparationArtifacts);
            List<CommitMessage> abortMessages = new ArrayList<>(retryArtifacts);
            abortMessages.addAll(preparationArtifacts);
            try (TableCommitImpl commit = table.newCommit(commitUser)) {
                commitConfigurer.configure(commit);
                commitConfigurer.prepareMessages(attemptSnapshot, preparedMessages);
                try {
                    commit.commit(preparedMessages);
                } catch (RuntimeException conflict) {
                    if (isMergeConflict(conflict)) {
                        abortRetryArtifacts(commit, abortMessages, conflict, table);
                    }
                    throw conflict;
                }
                Snapshot committedSnapshot =
                        table.snapshotManager()
                                .latestSnapshotOfUser(commitUser)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Cannot find the committed data evolution rewrite snapshot."));
                if (commitObserver != null) {
                    // Include successful retry artifacts, not just the original staged outputs.
                    commitObserver.accept(preparedMessages);
                }
                return committedSnapshot;
            } catch (RuntimeException conflict) {
                if (commitMessageRewriter == null
                        || !isMergeConflict(conflict)
                        || System.currentTimeMillis() - startMillis
                                > table.coreOptions().commitTimeout()
                        || retryCount >= table.coreOptions().commitMaxRetries()) {
                    throw conflict;
                }
                lastConflict = conflict;
                retryWaiter.retryWait(retryCount);
                retryCount++;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static List<CommitMessage> retryArtifacts(
            List<CommitMessage> compactMessages, List<CommitMessage> rewrittenMessages) {
        checkArgument(
                rewrittenMessages.size() >= compactMessages.size(),
                "Rewritten commit messages must retain all staged compact messages.");
        for (int i = 0; i < compactMessages.size(); i++) {
            checkArgument(
                    rewrittenMessages.get(i) == compactMessages.get(i),
                    "Rewritten commit messages must retain staged compact message %s.",
                    i);
        }
        return new ArrayList<>(
                rewrittenMessages.subList(compactMessages.size(), rewrittenMessages.size()));
    }

    private static boolean isMergeConflict(RuntimeException conflict) {
        return ExceptionUtils.findThrowable(conflict, DataEvolutionRowRangeConflictException.class)
                .isPresent();
    }

    private static void abortRetryArtifacts(
            TableCommitImpl commit,
            List<CommitMessage> abortMessages,
            RuntimeException conflict,
            FileStoreTable table) {
        if (abortMessages.isEmpty()) {
            return;
        }
        try {
            commit.abort(abortMessages);
        } catch (RuntimeException abortFailure) {
            conflict.addSuppressed(abortFailure);
            LOG.warn(
                    "Failed to abort {} staged compact retry artifacts for table {}.",
                    abortMessages.size(),
                    table.fullName(),
                    abortFailure);
        }
    }

    private static void abortRetryArtifacts(
            FileStoreTable table,
            String commitUser,
            List<CommitMessage> abortMessages,
            RuntimeException conflict) {
        if (abortMessages.isEmpty()) {
            return;
        }
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            abortRetryArtifacts(commit, abortMessages, conflict, table);
        } catch (Exception abortFailure) {
            conflict.addSuppressed(abortFailure);
            LOG.warn(
                    "Failed to close the commit after aborting staged compact retry artifacts "
                            + "for table {}.",
                    table.fullName(),
                    abortFailure);
        }
    }

    private static List<CommitMessage> deserializeCommitMessagesAndReleaseSerializedBytes(
            CommitMessageSerializer serializer, List<byte[]> serializedMessages)
            throws IOException {
        List<CommitMessage> messages = new ArrayList<>(serializedMessages.size());
        for (int i = 0; i < serializedMessages.size(); i++) {
            byte[] serializedMessage = serializedMessages.set(i, null);
            messages.add(serializer.deserialize(serializer.getVersion(), serializedMessage));
        }
        return messages;
    }

    @FunctionalInterface
    interface CommitConfigurer {

        void configure(TableCommitImpl commit);

        default void prepareMessages(Snapshot snapshot, List<CommitMessage> commitMessages) {}
    }

    @FunctionalInterface
    interface CommitMessageRewriter {

        /**
         * Returns the original compact messages followed by any newly staged retry artifacts. Retry
         * artifacts are aborted if the rebased commit still conflicts.
         */
        Optional<List<CommitMessage>> rewrite(
                SparkSession sparkSession,
                Snapshot taskSnapshot,
                Snapshot latestSnapshot,
                List<CommitMessage> compactMessages);
    }
}
