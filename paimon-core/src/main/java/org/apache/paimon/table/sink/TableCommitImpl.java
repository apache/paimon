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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.ExpireExecutionMode;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;
import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecuteSequentialReturn;

/** An abstraction layer above {@link FileStoreCommit} to provide snapshot commit and expiration. */
public class TableCommitImpl implements InnerTableCommit {
    private static final Logger LOG = LoggerFactory.getLogger(TableCommitImpl.class);

    private final FileStoreCommit commit;
    @Nullable private final Runnable expireSnapshots;
    @Nullable private final PartitionExpire partitionExpire;
    @Nullable private final TagAutoManager tagAutoManager;
    private final Lock lock;

    @Nullable private final Duration consumerExpireTime;
    private final ConsumerManager consumerManager;

    private final ExecutorService expireMainExecutor;
    private final AtomicReference<Throwable> expireError;

    private final String tableName;

    @Nullable private Map<String, String> overwritePartition = null;
    private boolean batchCommitted = false;
    private final boolean forceCreatingSnapshot;

    public TableCommitImpl(
            FileStoreCommit commit,
            @Nullable Runnable expireSnapshots,
            @Nullable PartitionExpire partitionExpire,
            @Nullable TagAutoManager tagAutoManager,
            Lock lock,
            @Nullable Duration consumerExpireTime,
            ConsumerManager consumerManager,
            ExpireExecutionMode expireExecutionMode,
            String tableName,
            boolean forceCreatingSnapshot) {
        commit.withLock(lock);
        if (partitionExpire != null) {
            partitionExpire.withLock(lock);
            commit.withPartitionExpire(partitionExpire);
        }

        this.commit = commit;
        this.expireSnapshots = expireSnapshots;
        this.partitionExpire = partitionExpire;
        this.tagAutoManager = tagAutoManager;
        this.lock = lock;

        this.consumerExpireTime = consumerExpireTime;
        this.consumerManager = consumerManager;

        this.expireMainExecutor =
                expireExecutionMode == ExpireExecutionMode.SYNC
                        ? MoreExecutors.newDirectExecutorService()
                        : Executors.newSingleThreadExecutor(
                                new ExecutorThreadFactory(
                                        Thread.currentThread().getName() + "expire-main-thread"));
        this.expireError = new AtomicReference<>(null);

        this.tableName = tableName;
        this.forceCreatingSnapshot = forceCreatingSnapshot;
    }

    public boolean forceCreatingSnapshot() {
        if (this.forceCreatingSnapshot) {
            return true;
        }
        if (overwritePartition != null) {
            return true;
        }
        return tagAutoManager != null
                && tagAutoManager.getTagAutoCreation() != null
                && tagAutoManager.getTagAutoCreation().forceCreatingSnapshot();
    }

    @Override
    public TableCommitImpl withOverwrite(@Nullable Map<String, String> overwritePartitions) {
        this.overwritePartition = overwritePartitions;
        return this;
    }

    @Override
    public TableCommitImpl ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        commit.ignoreEmptyCommit(ignoreEmptyCommit);
        return this;
    }

    @Override
    public InnerTableCommit withMetricRegistry(MetricRegistry registry) {
        commit.withMetrics(new CommitMetrics(registry, tableName));
        return this;
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        checkCommitted();
        commit(COMMIT_IDENTIFIER, commitMessages);
    }

    @Override
    public void truncateTable() {
        checkCommitted();
        commit.truncateTable(COMMIT_IDENTIFIER);
    }

    private void checkCommitted() {
        checkState(!batchCommitted, "BatchTableCommit only support one-time committing.");
        batchCommitted = true;
    }

    @Override
    public void commit(long identifier, List<CommitMessage> commitMessages) {
        commit(createManifestCommittable(identifier, commitMessages));
    }

    @Override
    public int filterAndCommit(Map<Long, List<CommitMessage>> commitIdentifiersAndMessages) {
        return filterAndCommitMultiple(
                commitIdentifiersAndMessages.entrySet().stream()
                        .map(e -> createManifestCommittable(e.getKey(), e.getValue()))
                        .collect(Collectors.toList()));
    }

    private ManifestCommittable createManifestCommittable(
            long identifier, List<CommitMessage> commitMessages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        for (CommitMessage commitMessage : commitMessages) {
            committable.addFileCommittable(commitMessage);
        }
        return committable;
    }

    public void commit(ManifestCommittable committable) {
        commitMultiple(singletonList(committable), false);
    }

    public void commitMultiple(List<ManifestCommittable> committables, boolean checkAppendFiles) {
        if (overwritePartition == null) {
            for (ManifestCommittable committable : committables) {
                commit.commit(committable, new HashMap<>(), checkAppendFiles);
            }
            if (!committables.isEmpty()) {
                expire(committables.get(committables.size() - 1).identifier(), expireMainExecutor);
            }
        } else {
            ManifestCommittable committable;
            if (committables.size() > 1) {
                throw new RuntimeException(
                        "Multiple committables appear in overwrite mode, this may be a bug, please report it: "
                                + committables);
            } else if (committables.size() == 1) {
                committable = committables.get(0);
            } else {
                // create an empty committable
                // identifier is Long.MAX_VALUE, come from batch job
                // TODO maybe it can be produced by CommitterOperator
                committable = new ManifestCommittable(Long.MAX_VALUE);
            }
            commit.overwrite(overwritePartition, committable, Collections.emptyMap());
            expire(committable.identifier(), expireMainExecutor);
        }
    }

    public int filterAndCommitMultiple(List<ManifestCommittable> committables) {
        return filterAndCommitMultiple(committables, true);
    }

    public int filterAndCommitMultiple(
            List<ManifestCommittable> committables, boolean checkAppendFiles) {
        List<ManifestCommittable> sortedCommittables =
                committables.stream()
                        // identifier must be in increasing order
                        .sorted(Comparator.comparingLong(ManifestCommittable::identifier))
                        .collect(Collectors.toList());
        List<ManifestCommittable> retryCommittables = commit.filterCommitted(sortedCommittables);

        if (!retryCommittables.isEmpty()) {
            checkFilesExistence(retryCommittables);
            commitMultiple(retryCommittables, checkAppendFiles);
        }
        return retryCommittables.size();
    }

    private void checkFilesExistence(List<ManifestCommittable> committables) {
        List<Path> files = new ArrayList<>();
        Map<Pair<BinaryRow, Integer>, DataFilePathFactory> factoryMap = new HashMap<>();
        PathFactory indexFileFactory = commit.pathFactory().indexFileFactory();
        for (ManifestCommittable committable : committables) {
            for (CommitMessage message : committable.fileCommittables()) {
                CommitMessageImpl msg = (CommitMessageImpl) message;
                DataFilePathFactory pathFactory =
                        factoryMap.computeIfAbsent(
                                Pair.of(message.partition(), message.bucket()),
                                k ->
                                        commit.pathFactory()
                                                .createDataFilePathFactory(
                                                        k.getKey(), k.getValue()));

                Consumer<DataFileMeta> collector = f -> files.addAll(f.collectFiles(pathFactory));
                msg.newFilesIncrement().newFiles().forEach(collector);
                msg.newFilesIncrement().changelogFiles().forEach(collector);
                msg.compactIncrement().compactBefore().forEach(collector);
                msg.compactIncrement().compactAfter().forEach(collector);
                msg.indexIncrement().newIndexFiles().stream()
                        .map(IndexFileMeta::fileName)
                        .map(indexFileFactory::toPath)
                        .forEach(files::add);
                msg.indexIncrement().deletedIndexFiles().stream()
                        .map(IndexFileMeta::fileName)
                        .map(indexFileFactory::toPath)
                        .forEach(files::add);
            }
        }

        Predicate<Path> nonExists =
                p -> {
                    try {
                        return !commit.fileIO().exists(p);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                };

        List<Path> nonExistFiles =
                Lists.newArrayList(
                        randomlyExecuteSequentialReturn(
                                getExecutorService(null),
                                f -> nonExists.test(f) ? singletonList(f) : emptyList(),
                                files));

        if (nonExistFiles.size() > 0) {
            String message =
                    String.join(
                            "\n",
                            "Cannot recover from this checkpoint because some files in the snapshot that"
                                    + " need to be resubmitted have been deleted:",
                            "    "
                                    + nonExistFiles.stream()
                                            .map(Object::toString)
                                            .collect(Collectors.joining(",")),
                            "    The most likely reason is because you are recovering from a very old savepoint that"
                                    + " contains some uncommitted files that have already been deleted.");
            throw new RuntimeException(message);
        }
    }

    private void expire(long partitionExpireIdentifier, ExecutorService executor) {
        if (expireError.get() != null) {
            throw new RuntimeException(expireError.get());
        }

        executor.execute(
                () -> {
                    try {
                        expire(partitionExpireIdentifier);
                    } catch (Throwable t) {
                        LOG.error("Executing expire encountered an error.", t);
                        expireError.compareAndSet(null, t);
                    }
                });
    }

    private void expire(long partitionExpireIdentifier) {
        // expire consumer first to avoid preventing snapshot expiration
        if (consumerExpireTime != null) {
            consumerManager.expire(LocalDateTime.now().minus(consumerExpireTime));
        }

        expireSnapshots();

        if (partitionExpire != null) {
            partitionExpire.expire(partitionExpireIdentifier);
        }

        if (tagAutoManager != null) {
            tagAutoManager.run();
        }
    }

    public void expireSnapshots() {
        if (expireSnapshots != null) {
            expireSnapshots.run();
        }
    }

    @Override
    public void close() throws Exception {
        commit.close();
        IOUtils.closeQuietly(lock);
        expireMainExecutor.shutdownNow();
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        commit.abort(commitMessages);
    }

    @VisibleForTesting
    public ExecutorService getExpireMainExecutor() {
        return expireMainExecutor;
    }
}
