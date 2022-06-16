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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.connector.StatefulPrecommittingSinkWriter;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSink;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.log.LogInitContext;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.log.LogWriteCallback;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.TableCompact;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/** {@link Sink} of dynamic store. */
public class StoreSink<WriterStateT, LogCommT>
        implements StatefulSink<RowData, WriterStateT>,
                GlobalCommittingSink<RowData, Committable, ManifestCommittable> {

    private static final long serialVersionUID = 1L;

    private final ObjectIdentifier tableIdentifier;

    private final FileStoreTable table;

    private final boolean compactionTask;

    @Nullable private final Map<String, String> compactPartitionSpec;

    @Nullable private final CatalogLock.Factory lockFactory;

    @Nullable private final Map<String, String> overwritePartition;

    @Nullable private final LogSinkProvider logSinkProvider;

    public StoreSink(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            boolean compactionTask,
            @Nullable Map<String, String> compactPartitionSpec,
            @Nullable CatalogLock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition,
            @Nullable LogSinkProvider logSinkProvider) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.compactionTask = compactionTask;
        this.compactPartitionSpec = compactPartitionSpec;
        this.lockFactory = lockFactory;
        this.overwritePartition = overwritePartition;
        this.logSinkProvider = logSinkProvider;
    }

    @Override
    public StatefulPrecommittingSinkWriter<WriterStateT> createWriter(InitContext initContext)
            throws IOException {
        return restoreWriter(initContext, null);
    }

    @Override
    public StatefulPrecommittingSinkWriter<WriterStateT> restoreWriter(
            InitContext initContext, Collection<WriterStateT> states) throws IOException {
        if (compactionTask) {
            return createCompactWriter(initContext);
        }
        SinkWriter<SinkRecord> logWriter = null;
        LogWriteCallback logCallback = null;
        if (logSinkProvider != null) {
            logCallback = new LogWriteCallback();
            Consumer<?> metadataConsumer = logSinkProvider.createMetadataConsumer(logCallback);
            LogInitContext logInitContext = new LogInitContext(initContext, metadataConsumer);
            Sink<SinkRecord> logSink = logSinkProvider.createSink();
            logWriter =
                    states == null
                            ? logSink.createWriter(logInitContext)
                            : ((StatefulSink<SinkRecord, WriterStateT>) logSink)
                                    .restoreWriter(logInitContext, states);
        }
        return new StoreSinkWriter<>(
                table.newWrite().withOverwrite(overwritePartition != null), logWriter, logCallback);
    }

    private StoreSinkCompactor<WriterStateT> createCompactWriter(InitContext initContext) {
        int task = initContext.getSubtaskId();
        int numTask = initContext.getNumberOfParallelSubtasks();
        TableCompact tableCompact = table.newCompact();
        tableCompact.withPartitions(
                compactPartitionSpec == null ? Collections.emptyMap() : compactPartitionSpec);
        tableCompact.withFilter(
                (partition, bucket) -> task == Math.abs(Objects.hash(partition, bucket) % numTask));
        return new StoreSinkCompactor<>(tableCompact);
    }

    @Override
    public SimpleVersionedSerializer<WriterStateT> getWriterStateSerializer() {
        return logSinkProvider == null
                ? new NoOutputSerializer<>()
                : ((StatefulSink<SinkRecord, WriterStateT>) logSinkProvider.createSink())
                        .getWriterStateSerializer();
    }

    @Nullable
    private Committer<LogCommT> logCommitter() {
        if (logSinkProvider != null) {
            Sink<SinkRecord> sink = logSinkProvider.createSink();
            if (sink instanceof TwoPhaseCommittingSink) {
                try {
                    return ((TwoPhaseCommittingSink<SinkRecord, LogCommT>) sink).createCommitter();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        return null;
    }

    @Nullable
    private SimpleVersionedSerializer<LogCommT> logCommitSerializer() {
        if (logSinkProvider != null) {
            Sink<SinkRecord> sink = logSinkProvider.createSink();
            if (sink instanceof TwoPhaseCommittingSink) {
                return ((TwoPhaseCommittingSink<SinkRecord, LogCommT>) sink)
                        .getCommittableSerializer();
            }
        }

        return null;
    }

    @Override
    public Committer<Committable> createCommitter() {
        return new StoreLocalCommitter<>(logCommitter());
    }

    @Override
    public StoreGlobalCommitter createGlobalCommitter() {
        CatalogLock catalogLock;
        Lock lock;
        if (lockFactory == null) {
            catalogLock = null;
            lock = null;
        } else {
            catalogLock = lockFactory.create();
            lock =
                    new Lock() {
                        @Override
                        public <T> T runWithLock(Callable<T> callable) throws Exception {
                            return catalogLock.runWithLock(
                                    tableIdentifier.getDatabaseName(),
                                    tableIdentifier.getObjectName(),
                                    callable);
                        }
                    };
        }

        return new StoreGlobalCommitter(
                table.newCommit().withOverwritePartition(overwritePartition).withLock(lock),
                catalogLock);
    }

    @SuppressWarnings("unchecked")
    @Override
    public SimpleVersionedSerializer<Committable> getCommittableSerializer() {
        return new CommittableSerializer(
                fileCommitSerializer(), (SimpleVersionedSerializer<Object>) logCommitSerializer());
    }

    @Override
    public ManifestCommittableSerializer getGlobalCommittableSerializer() {
        return new ManifestCommittableSerializer();
    }

    private FileCommittableSerializer fileCommitSerializer() {
        return new FileCommittableSerializer();
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
