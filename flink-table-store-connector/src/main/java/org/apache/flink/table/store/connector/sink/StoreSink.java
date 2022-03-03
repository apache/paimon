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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSink;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.sink.SinkRecordConverter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

/** {@link Sink} of dynamic store. */
public class StoreSink<WriterStateT, LogCommT>
        implements StatefulSink<RowData, WriterStateT>,
                GlobalCommittingSink<RowData, Committable, GlobalCommittable<LogCommT>> {

    private static final long serialVersionUID = 1L;

    private final ObjectIdentifier tableIdentifier;

    private final FileStore fileStore;

    private final int[] partitions;

    private final int[] primaryKeys;

    private final int numBucket;

    @Nullable private final CatalogLock.Factory lockFactory;

    @Nullable private final Map<String, String> overwritePartition;

    public StoreSink(
            ObjectIdentifier tableIdentifier,
            FileStore fileStore,
            int[] partitions,
            int[] primaryKeys,
            int numBucket,
            @Nullable CatalogLock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition) {
        this.tableIdentifier = tableIdentifier;
        this.fileStore = fileStore;
        this.partitions = partitions;
        this.primaryKeys = primaryKeys;
        this.numBucket = numBucket;
        this.lockFactory = lockFactory;
        this.overwritePartition = overwritePartition;
    }

    @Override
    public StoreSinkWriter<WriterStateT> createWriter(InitContext initContext) throws IOException {
        return restoreWriter(initContext, null);
    }

    @Override
    public StoreSinkWriter<WriterStateT> restoreWriter(
            InitContext initContext, Collection<WriterStateT> states) throws IOException {
        return new StoreSinkWriter<>(
                fileStore.newWrite(),
                new SinkRecordConverter(
                        numBucket,
                        primaryKeys.length > 0 ? fileStore.valueType() : fileStore.keyType(),
                        partitions,
                        primaryKeys),
                fileCommitSerializer(),
                overwritePartition != null);
    }

    @Override
    public SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer() {
        return new NoOutputSerializer<>();
    }

    @Override
    public StoreGlobalCommitter<LogCommT> createGlobalCommitter() {
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

        return new StoreGlobalCommitter<>(
                commit, fileStore.newExpire(), fileCommitSerializer(), lock, overwritePartition);
    }

    @Override
    public SimpleVersionedSerializer<Committable> getCommittableSerializer() {
        return CommittableSerializer.INSTANCE;
    }

    @Override
    public GlobalCommittableSerializer<LogCommT> getGlobalCommittableSerializer() {
        ManifestCommittableSerializer fileCommSerializer =
                new ManifestCommittableSerializer(
                        fileStore.partitionType(), fileStore.keyType(), fileStore.valueType());
        SimpleVersionedSerializer<LogCommT> logCommitSerializer = new NoOutputSerializer<>();
        return new GlobalCommittableSerializer<>(logCommitSerializer, fileCommSerializer);
    }

    private FileCommittableSerializer fileCommitSerializer() {
        return new FileCommittableSerializer(
                fileStore.partitionType(), fileStore.keyType(), fileStore.valueType());
    }

    private static class NoOutputSerializer<T> implements SimpleVersionedSerializer<T> {
        private NoOutputSerializer() {}

        public int getVersion() {
            return 1;
        }

        public byte[] serialize(T obj) {
            throw new IllegalStateException("Should not serialize anything");
        }

        public T deserialize(int version, byte[] serialized) {
            throw new IllegalStateException("Should not deserialize anything");
        }
    }
}
