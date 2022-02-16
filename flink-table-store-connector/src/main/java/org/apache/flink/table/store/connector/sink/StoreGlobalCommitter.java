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

import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.store.connector.sink.global.GlobalCommitter;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.FileStoreExpire;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link GlobalCommitter} for dynamic store. */
public class StoreGlobalCommitter<LogCommT>
        implements GlobalCommitter<Committable, GlobalCommittable<LogCommT>> {

    private final FileStoreCommit fileStoreCommit;

    private final FileStoreExpire fileStoreExpire;

    private final FileCommittableSerializer fileCommitSerializer;

    @Nullable private final CatalogLock lock;

    @Nullable private final Map<String, String> overwritePartition;

    public StoreGlobalCommitter(
            FileStoreCommit fileStoreCommit,
            FileStoreExpire fileStoreExpire,
            FileCommittableSerializer fileCommitSerializer,
            @Nullable CatalogLock lock,
            @Nullable Map<String, String> overwritePartition) {
        this.fileStoreCommit = fileStoreCommit;
        this.fileStoreExpire = fileStoreExpire;
        this.fileCommitSerializer = fileCommitSerializer;
        this.lock = lock;
        this.overwritePartition = overwritePartition;
    }

    @Override
    public void close() throws Exception {
        if (lock != null) {
            lock.close();
        }
    }

    @Override
    public List<GlobalCommittable<LogCommT>> filterRecoveredCommittables(
            List<GlobalCommittable<LogCommT>> globalCommittables) {
        List<ManifestCommittable> filtered =
                fileStoreCommit.filterCommitted(
                        globalCommittables.stream()
                                .map(GlobalCommittable::fileCommittable)
                                .collect(Collectors.toList()));
        return globalCommittables.stream()
                .filter(c -> filtered.contains(c.fileCommittable()))
                .collect(Collectors.toList());
    }

    @Override
    public GlobalCommittable<LogCommT> combine(long checkpointId, List<Committable> committables)
            throws IOException {
        List<LogCommT> logCommittables = new ArrayList<>();
        ManifestCommittable fileCommittable = new ManifestCommittable(String.valueOf(checkpointId));
        for (Committable committable : committables) {
            switch (committable.kind()) {
                case FILE:
                    FileCommittable file =
                            fileCommitSerializer.deserialize(
                                    committable.serializerVersion(),
                                    committable.wrappedCommittable());
                    fileCommittable.addFileCommittable(
                            file.partition(), file.bucket(), file.increment());
                    break;
                case LOG_OFFSET:
                    LogOffsetCommittable offset =
                            LogOffsetCommittable.fromBytes(committable.wrappedCommittable());
                    fileCommittable.addLogOffset(offset.bucket(), offset.offset());
                    break;
                case LOG:
                    throw new UnsupportedOperationException();
            }
        }
        return new GlobalCommittable<>(logCommittables, fileCommittable);
    }

    @Override
    public void commit(List<GlobalCommittable<LogCommT>> committables) {
        if (overwritePartition == null) {
            for (GlobalCommittable<LogCommT> committable : committables) {
                fileStoreCommit.commit(committable.fileCommittable(), new HashMap<>());
            }
        } else {
            for (GlobalCommittable<LogCommT> committable : committables) {
                fileStoreCommit.overwrite(
                        overwritePartition, committable.fileCommittable(), new HashMap<>());
            }
        }

        // TODO introduce check interval
        fileStoreExpire.expire();
    }
}
