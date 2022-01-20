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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.flink.table.store.connector.utils.ProjectionUtils.project;

/** {@link Sink} of dynamic store. */
public class StoreSink implements Sink<RowData, LocalCommittable, Void, ManifestCommittable> {

    private static final long serialVersionUID = 1L;

    private final ObjectIdentifier tableIdentifier;

    private final FileStore fileStore;

    private final RowType rowType;

    private final int[] partitions;

    private final int[] keys;

    private final int numBucket;

    @Nullable private final CatalogLock.Factory lockFactory;

    @Nullable private final Map<String, String> overwritePartition;

    public StoreSink(
            ObjectIdentifier tableIdentifier,
            FileStore fileStore,
            RowType rowType,
            int[] partitions,
            int[] keys,
            int numBucket,
            @Nullable CatalogLock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition) {
        this.tableIdentifier = tableIdentifier;
        this.fileStore = fileStore;
        this.rowType = rowType;
        this.partitions = partitions;
        this.keys = keys;
        this.numBucket = numBucket;
        this.lockFactory = lockFactory;
        this.overwritePartition = overwritePartition;
    }

    @Override
    public StoreSinkWriter createWriter(InitContext initContext, List<Void> list) {
        SinkRecordConverter recordConverter =
                new SinkRecordConverter(numBucket, rowType, partitions, keys);
        return new StoreSinkWriter(
                fileStore.newWrite(), recordConverter, overwritePartition != null);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<Committer<LocalCommittable>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<LocalCommittable, ManifestCommittable>>
            createGlobalCommitter() {
        FileStoreCommit commit = fileStore.newCommit();
        CatalogLock lock;
        if (lockFactory == null) {
            lock = null;
        } else {
            lock = lockFactory.create();
            commit.withLock(
                    new Lock() {
                        @Override
                        public <T> T runWithLock(Callable<T> callable) throws Exception {
                            return lock.runWithLock(
                                    tableIdentifier.getDatabaseName(),
                                    tableIdentifier.getObjectName(),
                                    callable);
                        }
                    });
        }
        return Optional.of(
                new StoreGlobalCommitter(commit, fileStore.newExpire(), lock, overwritePartition));
    }

    @Override
    public Optional<SimpleVersionedSerializer<LocalCommittable>> getCommittableSerializer() {
        return Optional.of(
                new LocalCommittableSerializer(
                        project(rowType, partitions), project(rowType, keys), rowType));
    }

    @Override
    public Optional<SimpleVersionedSerializer<ManifestCommittable>>
            getGlobalCommittableSerializer() {
        return Optional.of(
                new ManifestCommittableSerializer(
                        project(rowType, partitions), project(rowType, keys), rowType));
    }
}
