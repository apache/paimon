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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Default implementation of {@link StoreSinkWrite}. This writer does not have states. */
public class StoreSinkWriteImpl implements StoreSinkWrite {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkWriteImpl.class);

    /** Per-(table, subtask) write throughput, unlike the commit-gated CommitMetrics counters. */
    private static final String WRITER_METRIC_GROUP = "writer";

    private static final String ROWS_WRITTEN_METRIC = "rowsWritten";

    protected final String commitUser;
    protected final StoreSinkWriteState state;
    private final IOManagerImpl paimonIOManager;
    private final boolean ignorePreviousFiles;
    private final boolean waitCompaction;
    private final boolean isStreamingMode;
    private final MemoryPoolFactory memoryPoolFactory;
    @Nullable private final MetricGroup metricGroup;
    private final TableWriteFactory tableWriteFactory;
    @Nullable private final Counter rowsWritten;

    @Nullable private UriReaderFactory blobDescriptorReaderFactory;

    protected TableWriteImpl<?> write;

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
                memoryPoolFactory,
                metricGroup,
                FileStoreTable::newWrite);
    }

    StoreSinkWriteImpl(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup,
            TableWriteFactory tableWriteFactory) {
        this.commitUser = commitUser;
        this.state = state;
        this.paimonIOManager = new IOManagerImpl(ioManager.getSpillingDirectoriesPaths());
        this.ignorePreviousFiles = ignorePreviousFiles;
        this.waitCompaction = waitCompaction;
        this.isStreamingMode = isStreamingMode;
        this.memoryPoolFactory = memoryPoolFactory;
        this.metricGroup = metricGroup;
        this.tableWriteFactory = tableWriteFactory;
        // Not derived from newTableWrite: replace() rebuilds the write on schema evolution,
        // and the counter must survive that, not reset.
        this.rowsWritten =
                metricGroup == null
                        ? null
                        : new FlinkMetricRegistry(metricGroup)
                                .createTableMetricGroup(WRITER_METRIC_GROUP, table.name())
                                .counter(ROWS_WRITTEN_METRIC);
        this.write = newTableWrite(table);
    }

    private TableWriteImpl<?> newTableWrite(FileStoreTable table) {
        TableWriteImpl<?> tableWrite =
                tableWriteFactory
                        .create(table, commitUser, state.getSubtaskId())
                        .withIOManager(paimonIOManager)
                        .withIgnorePreviousFiles(ignorePreviousFiles)
                        .withMemoryPoolFactory(memoryPoolFactory);

        if (metricGroup != null) {
            tableWrite.withMetricRegistry(new FlinkMetricRegistry(metricGroup));
        }
        return tableWrite;
    }

    public void withCompactExecutor(ExecutorService compactExecutor) {
        write.withCompactExecutor(compactExecutor);
    }

    @Override
    public void setWriteRestore(WriteRestore writeRestore) {
        write.withWriteRestore(writeRestore);
    }

    @Override
    public void setBlobDescriptorReaderFactory(UriReaderFactory uriReaderFactory) {
        this.blobDescriptorReaderFactory = uriReaderFactory;
    }

    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData) throws Exception {
        return countRow(write.writeAndReturn(withBlobDescriptorReader(rowData)));
    }

    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData, int bucket) throws Exception {
        return countRow(write.writeAndReturn(withBlobDescriptorReader(rowData), bucket));
    }

    @Override
    @Nullable
    public SinkRecord write(InternalRow rowData, int bucket, int totalBuckets) throws Exception {
        return countRow(
                write.writeAndReturn(withBlobDescriptorReader(rowData), bucket, totalBuckets));
    }

    private InternalRow withBlobDescriptorReader(InternalRow rowData) {
        return blobDescriptorReaderFactory == null
                ? rowData
                : new BlobDescriptorResolvingRow(rowData, blobDescriptorReaderFactory);
    }

    /**
     * Counts accepted rows. On the result, not on entry: writeAndReturn returns null for rows the
     * row-kind filter drops. In all three overloads because they do not delegate to each other.
     *
     * <p>Rows entering the write buffer, so an upper bound on LAST_DELTA_RECORDS_APPENDED --
     * MergeTreeWriter merges same-key rows on flush.
     */
    @Nullable
    private SinkRecord countRow(@Nullable SinkRecord record) {
        if (rowsWritten != null && record != null) {
            rowsWritten.inc();
        }
        return record;
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
                    committables.add(new Committable(checkpointId, committable));
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

    public TableWriteImpl<?> getWrite() {
        return write;
    }

    @FunctionalInterface
    interface TableWriteFactory {

        TableWriteImpl<?> create(
                FileStoreTable table, String commitUser, @Nullable Integer writeId);
    }
}
