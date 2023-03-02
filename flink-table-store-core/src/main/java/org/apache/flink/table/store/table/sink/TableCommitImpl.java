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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.FileStoreExpire;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.file.operation.PartitionExpire;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.store.utils.Preconditions.checkState;

/**
 * An abstraction layer above {@link FileStoreCommit} and {@link FileStoreExpire} to provide
 * snapshot commit and expiration.
 */
public class TableCommitImpl implements InnerTableCommit {

    private final FileStoreCommit commit;
    @Nullable private final FileStoreExpire expire;
    @Nullable private final PartitionExpire partitionExpire;

    @Nullable private List<Map<String, String>> overwritePartitions = null;
    @Nullable private Lock lock;

    private boolean batchCommitted = false;

    public TableCommitImpl(
            FileStoreCommit commit,
            @Nullable FileStoreExpire expire,
            @Nullable PartitionExpire partitionExpire) {
        this.commit = commit;
        this.expire = expire;
        this.partitionExpire = partitionExpire;
    }

    @Override
    public TableCommitImpl withOverwrite(@Nullable List<Map<String, String>> overwritePartitions) {
        this.overwritePartitions = overwritePartitions;
        return this;
    }

    @Override
    public TableCommitImpl withLock(Lock lock) {
        commit.withLock(lock);

        if (expire != null) {
            expire.withLock(lock);
        }

        if (partitionExpire != null) {
            partitionExpire.withLock(lock);
        }

        this.lock = lock;
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
    public void commit(long identifier, List<CommitMessage> commitMessages) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        for (CommitMessage commitMessage : commitMessages) {
            committable.addFileCommittable(commitMessage);
        }
        if (overwritePartitions == null) {
            commit.commit(committable, new HashMap<>());
        } else {
            commit.overwrite(overwritePartitions, committable, new HashMap<>());
        }
        expire();
    }

    public void commitMultiple(List<ManifestCommittable> committables) {
        if (overwritePartitions == null) {
            for (ManifestCommittable committable : committables) {
                commit.commit(committable, new HashMap<>());
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
            commit.overwrite(overwritePartitions, committable, new HashMap<>());
        }

        expire();
    }

    private void expire() {
        if (expire != null) {
            expire.expire();
        }

        if (partitionExpire != null) {
            partitionExpire.expire();
        }
    }

    @Override
    public void close() throws Exception {
        if (lock != null) {
            lock.close();
        }
    }

    @Override
    public void commit(List<CommitMessage> commitMessages) {
        checkState(!batchCommitted, "BatchTableCommit only support one-time committing.");
        batchCommitted = true;
        commit(BatchWriteBuilder.COMMIT_IDENTIFIER, commitMessages);
    }
}
