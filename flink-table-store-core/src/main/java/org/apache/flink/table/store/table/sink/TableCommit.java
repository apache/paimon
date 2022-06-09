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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An abstraction layer above {@link FileStoreCommit} and {@link FileStoreExpire} to provide
 * snapshot commit and expiration.
 */
public class TableCommit {

    private final FileStoreCommit commit;
    private final FileStoreExpire expire;
    @Nullable private final Map<String, String> overwritePartition;

    public TableCommit(
            FileStoreCommit commit,
            FileStoreExpire expire,
            @Nullable Map<String, String> overwritePartition) {
        this.commit = commit;
        this.expire = expire;
        this.overwritePartition = overwritePartition;
    }

    public List<ManifestCommittable> filterRecoveredCommittables(
            List<ManifestCommittable> committables) {
        return commit.filterCommitted(committables);
    }

    public void commit(String identifier, List<FileCommittable> fileCommittables) {
        ManifestCommittable committable = new ManifestCommittable(identifier);
        for (FileCommittable fileCommittable : fileCommittables) {
            committable.addFileCommittable(
                    fileCommittable.partition(),
                    fileCommittable.bucket(),
                    fileCommittable.increment());
        }
        commit(Collections.singletonList(committable));
    }

    public void commit(List<ManifestCommittable> committables) {
        if (overwritePartition == null) {
            for (ManifestCommittable committable : committables) {
                commit.commit(committable, new HashMap<>());
            }
        } else {
            for (ManifestCommittable committable : committables) {
                commit.overwrite(overwritePartition, committable, new HashMap<>());
            }
        }
        expire.expire();
    }
}
