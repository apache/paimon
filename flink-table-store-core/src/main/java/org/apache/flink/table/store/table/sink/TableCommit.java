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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstraction layer above {@link FileStoreCommit} and {@link FileStoreExpire} to provide
 * snapshot commit and expiration.
 */
public class TableCommit implements AutoCloseable {

    private final FileStoreCommit commit;
    @Nullable private final FileStoreExpire expire;

    @Nullable private Map<String, String> overwritePartition = null;
    @Nullable private Lock lock;

    public TableCommit(FileStoreCommit commit, @Nullable FileStoreExpire expire) {
        this.commit = commit;
        this.expire = expire;
    }

    public TableCommit withOverwritePartition(@Nullable Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    public TableCommit withLock(Lock lock) {
        commit.withLock(lock);

        if (expire != null) {
            expire.withLock(lock);
        }

        this.lock = lock;
        return this;
    }

    public TableCommit withCreateEmptyCommit(boolean createEmptyCommit) {
        commit.withCreateEmptyCommit(createEmptyCommit);
        return this;
    }

    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables) {
        return commit.filterCommitted(committables);
    }

    public void commit(long identifier, List<FileCommittable> fileCommittables) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        for (FileCommittable fileCommittable : fileCommittables) {
            committable.addFileCommittable(fileCommittable);
        }
        commit(Collections.singletonList(committable));
    }

    public void commit(List<ManifestCommittable> committables) {
        if (overwritePartition == null) {
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
            commit.overwrite(overwritePartition, committable, new HashMap<>());
        }

        if (expire != null) {
            expire.expire();
        }
    }

    @Override
    public void close() throws Exception {
        if (lock != null) {
            lock.close();
        }
    }
}
