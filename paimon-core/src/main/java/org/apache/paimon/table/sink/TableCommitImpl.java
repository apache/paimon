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
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.metrics.commit.CommitMetrics;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreExpire;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.tag.TagAutoCreation;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.IOUtils;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.ExpireExecutionMode;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * An abstraction layer above {@link FileStoreCommit} and {@link FileStoreExpire} to provide
 * snapshot commit and expiration.
 */
public class TableCommitImpl implements InnerTableCommit {
    private static final Logger LOG = LoggerFactory.getLogger(TableCommitImpl.class);

    private final FileStoreCommit commit;
    private final List<CommitCallback> commitCallbacks;
    @Nullable private final FileStoreExpire expire;
    @Nullable private final PartitionExpire partitionExpire;
    @Nullable private final TagAutoCreation tagAutoCreation;
    private final Lock lock;

    @Nullable private final Duration consumerExpireTime;
    private final ConsumerManager consumerManager;

    @Nullable private Map<String, String> overwritePartition = null;

    private boolean batchCommitted = false;

    private ExecutorService expireMainExecutor;
    private AtomicReference<Throwable> expireError;
    private final CommitMetrics commitMetrics;

    public TableCommitImpl(
            FileStoreCommit commit,
            List<CommitCallback> commitCallbacks,
            @Nullable FileStoreExpire expire,
            @Nullable PartitionExpire partitionExpire,
            @Nullable TagAutoCreation tagAutoCreation,
            Lock lock,
            @Nullable Duration consumerExpireTime,
            ConsumerManager consumerManager,
            ExpireExecutionMode expireExecutionMode,
            String tableName) {
        this.commitMetrics = new CommitMetrics(tableName);
        commit.withLock(lock).withMetrics(commitMetrics);
        if (expire != null) {
            expire.withLock(lock);
        }
        if (partitionExpire != null) {
            partitionExpire.withLock(lock);
        }

        this.commit = commit;
        this.commitCallbacks = commitCallbacks;
        this.expire = expire;
        this.partitionExpire = partitionExpire;
        this.tagAutoCreation = tagAutoCreation;
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
    public Set<Long> filterCommitted(Set<Long> commitIdentifiers) {
        return commit.filterCommitted(commitIdentifiers);
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        checkState(!batchCommitted, "BatchTableCommit only support one-time committing.");
        batchCommitted = true;
        commit(BatchWriteBuilder.COMMIT_IDENTIFIER, commitMessages);
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
        commitMultiple(Collections.singletonList(committable));
    }

    public void commitMultiple(List<ManifestCommittable> committables) {
        if (overwritePartition == null) {
            for (ManifestCommittable committable : committables) {
                commit.commit(committable, new HashMap<>());
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

        commitCallbacks.forEach(c -> c.call(committables));
    }

    public int filterAndCommitMultiple(List<ManifestCommittable> committables) {
        Set<Long> retryIdentifiers =
                commit.filterCommitted(
                        committables.stream()
                                .map(ManifestCommittable::identifier)
                                .collect(Collectors.toSet()));

        // commitCallback may fail after the snapshot file is successfully created,
        // so we have to try all of them again
        List<ManifestCommittable> succeededCommittables =
                committables.stream()
                        .filter(c -> !retryIdentifiers.contains(c.identifier()))
                        .collect(Collectors.toList());
        commitCallbacks.forEach(c -> c.call(succeededCommittables));

        List<ManifestCommittable> retryCommittables =
                committables.stream()
                        .filter(c -> retryIdentifiers.contains(c.identifier()))
                        // identifier must be in increasing order
                        .sorted(Comparator.comparingLong(ManifestCommittable::identifier))
                        .collect(Collectors.toList());
        if (retryCommittables.size() > 0) {
            commitMultiple(retryCommittables);
        }
        return retryCommittables.size();
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

        if (expire != null) {
            expire.expire();
        }

        if (partitionExpire != null) {
            partitionExpire.expire(partitionExpireIdentifier);
        }

        if (tagAutoCreation != null) {
            tagAutoCreation.run();
        }
    }

    @Override
    public void close() throws Exception {
        for (CommitCallback commitCallback : commitCallbacks) {
            IOUtils.closeQuietly(commitCallback);
        }
        IOUtils.closeQuietly(lock);
        expireMainExecutor.shutdownNow();
        commitMetrics.close();
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
