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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.FileStoreExpire;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link GlobalCommitter} for dynamic store. */
public class StoreGlobalCommitter
        implements GlobalCommitter<LocalCommittable, ManifestCommittable> {

    private final FileStoreCommit fileStoreCommit;

    private final FileStoreExpire fileStoreExpire;

    @Nullable private final CatalogLock lock;

    @Nullable private final Map<String, String> overwritePartition;

    public StoreGlobalCommitter(
            FileStoreCommit fileStoreCommit,
            FileStoreExpire fileStoreExpire,
            @Nullable CatalogLock lock,
            @Nullable Map<String, String> overwritePartition) {
        this.fileStoreCommit = fileStoreCommit;
        this.fileStoreExpire = fileStoreExpire;
        this.lock = lock;
        this.overwritePartition = overwritePartition;
    }

    @Override
    public List<ManifestCommittable> filterRecoveredCommittables(
            List<ManifestCommittable> globalCommittables) {
        return fileStoreCommit.filterCommitted(globalCommittables);
    }

    @Override
    public ManifestCommittable combine(List<LocalCommittable> committables) {
        ManifestCommittable globalCommittable = new ManifestCommittable();
        committables.forEach(
                committable ->
                        globalCommittable.add(
                                committable.partition(),
                                committable.bucket(),
                                committable.increment()));
        return globalCommittable;
    }

    @Override
    public List<ManifestCommittable> commit(List<ManifestCommittable> globalCommittables) {
        Map<String, String> properties = new HashMap<>();
        if (overwritePartition == null) {
            for (ManifestCommittable committable : globalCommittables) {
                fileStoreCommit.commit(committable, properties);
            }
        } else {
            checkArgument(
                    globalCommittables.size() == 1, "overwrite is only supported in batch mode.");
            fileStoreCommit.overwrite(overwritePartition, globalCommittables.get(0), properties);
        }
        fileStoreExpire.expire();
        return Collections.emptyList();
    }

    @Override
    public void endOfInput() {}

    @Override
    public void close() throws Exception {
        if (lock != null) {
            lock.close();
        }
    }
}
