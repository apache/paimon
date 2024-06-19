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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteApi;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Default implementation of {@link StoreSinkWrite}. This writer does not have states. */
public class StoreSinkWriteImpl implements StoreSinkWrite {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkWriteImpl.class);

    protected final String commitUser;
    protected final StoreSinkWriteState state;
    private final IOManagerImpl paimonIOManager;
    private final boolean ignorePreviousFiles;
    private final boolean waitCompaction;
    private final boolean isStreamingMode;
    @Nullable private final MemorySegmentPool memoryPool;
    @Nullable private final MemoryPoolFactory memoryPoolFactory;

    protected TableWriteApi<?> write;

    @Nullable private final MetricGroup metricGroup;

    public StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            @Nullable MemorySegmentPool memoryPool,
            @Nullable MetricGroup metricGroup) {
        this(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreamingMode,
                memoryPool,
                null,
                metricGroup);
    }

    public StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        this(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreamingMode,
                null,
                memoryPoolFactory,
                metricGroup);
    }

    private StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            @Nullable MemorySegmentPool memoryPool,
            @Nullable MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        this.commitUser = commitUser;
        this.state = state;
        this.paimonIOManager = new IOManagerImpl(ioManager.getSpillingDirectoriesPaths());
        this.ignorePreviousFiles = ignorePreviousFiles;
        this.waitCompaction = waitCompaction;
        this.isStreamingMode = isStreamingMode;
        this.memoryPool = memoryPool;
        this.memoryPoolFactory = memoryPoolFactory;
        this.metricGroup = metricGroup;
        this.write = newTableWrite(table);
    }

    private TableWriteApi<?> newTableWrite(FileStoreTable table) {
        checkArgument(
                !(memoryPool != null && memoryPoolFactory != null),
                "memoryPool and memoryPoolFactory cannot be set at the same time.");

        TableWriteApi<?> tableWrite =
                table.newWrite(
                                commitUser,
                                (part, bucket) ->
                                        state.stateValueFilter().filter(table.name(), part, bucket))
                        .withIOManager(paimonIOManager)
                        .asTableWriteApi()
                        .withIgnorePreviousFiles(ignorePreviousFiles)
                        .withExecutionMode(isStreamingMode)
                        .asTableWriteApi()
                        .withBucketMode(table.bucketMode());

        if (metricGroup != null) {
            tableWrite.withMetricRegistry(new FlinkMetricRegistry(metricGroup));
        }

        if (memoryPoolFactory != null) {
            return tableWrite.withMemoryPoolFactory(memoryPoolFactory);
        } else {
            return tableWrite
                    .withMemoryPool(
                            memoryPool != null
                                    ? memoryPool
                                    : new HeapMemorySegmentPool(
                                            table.coreOptions().writeBufferSize(),
                                            table.coreOptions().pageSize()))
                    .asTableWriteApi();
        }
    }

    public void withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
    }

    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData) throws Exception {
        return write.writeAndReturn(rowData);
    }

    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData, int bucket) throws Exception {
        return write.writeAndReturn(rowData, bucket);
    }

    @Override
    public SinkRecord toLogRecord(SinkRecord record) {
        return write.toLogRecord(record);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        write.compact(partition, bucket, fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Receive {} new files from snapshot {}, partition {}, bucket {}",
                    files.size(),
                    snapshotId,
                    partition,
                    bucket);
        }
        write.notifyNewFiles(snapshotId, partition, bucket, files);
    }

    @Override
    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = new ArrayList<>();
        if (write != null) {
            try {
                for (CommitMessage committable :
                        write.prepareCommit(this.waitCompaction || waitCompaction, checkpointId)) {
                    committables.add(
                            new Committable(checkpointId, Committable.Kind.FILE, committable));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return committables;
    }

    @Override
    public void snapshotState() throws Exception {
        // do nothing
    }

    @Override
    public boolean streamingMode() {
        return isStreamingMode;
    }

    @Override
    public void close() throws Exception {
        if (write != null) {
            write.close();
        }

        paimonIOManager.close();
    }

    @Override
    public void replace(FileStoreTable newTable) throws Exception {
        if (commitUser == null) {
            return;
        }

        List<? extends FileStoreWrite.State<?>> states = write.checkpoint();
        write.close();
        write = newTableWrite(newTable);
        write.restore((List) states);
    }

    @VisibleForTesting
    public TableWriteApi<?> getWrite() {
        return write;
    }
}
