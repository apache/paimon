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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.format.FileFormat.fileFormat;

/** File write for format table. */
public class FormatTableFileWrite implements FileStoreWrite<InternalRow> {

    private final FileIO fileIO;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private boolean forceBufferSpill = false;
    protected final Map<BinaryRow, RecordWriter<InternalRow>> writers;
    protected final CoreOptions options;
    @Nullable protected IOManager ioManager;

    public FormatTableFileWrite(
            FileIO fileIO, RowType rowType, FileStorePathFactory pathFactory, CoreOptions options) {
        this.fileIO = fileIO;
        this.rowType = rowType;
        this.fileFormat = fileFormat(options);
        this.pathFactory = pathFactory;
        this.writers = new HashMap<>();
        this.options = options;
    }

    @Override
    public FileStoreWrite<InternalRow> withWriteRestore(WriteRestore writeRestore) {
        return this;
    }

    @Override
    public FileStoreWrite<InternalRow> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FileStoreWrite<InternalRow> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        return this;
    }

    @Override
    public FileStoreWrite<InternalRow> withMetricRegistry(MetricRegistry metricRegistry) {
        return this;
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        for (RecordWriter<InternalRow> writer : writers.values()) {
            writer.prepareCommit(false);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        writers.clear();
    }

    public void write(BinaryRow partition, InternalRow data) throws Exception {
        RecordWriter<InternalRow> writer = writers.get(partition);
        if (writer == null) {
            writer = createWriter(partition.copy());
            writers.put(partition.copy(), writer);
        }
        writer.write(data);
    }

    protected RecordWriter<InternalRow> createWriter(BinaryRow partition) {
        return new FormatTableRecordWriter(
                fileIO,
                ioManager,
                fileFormat,
                options.targetFileSize(false),
                pathFactory.createFormatTableDataFilePathFactory(partition),
                options.spillCompressOptions(),
                options.writeBufferSpillDiskSize(),
                options.useWriteBufferForAppend() || forceBufferSpill,
                options.writeBufferSpillable() || forceBufferSpill,
                rowType,
                options.fileCompression());
    }

    public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws Exception {
        List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
        for (RecordWriter<InternalRow> writer : writers.values()) {
            if (writer instanceof FormatTableRecordWriter) {
                FormatTableRecordWriter formatWriter = (FormatTableRecordWriter) writer;
                committers.addAll(formatWriter.closeAndGetCommitters());
            }
        }
        return committers;
    }

    @Override
    public List<State<InternalRow>> checkpoint() {
        return Collections.emptyList();
    }

    @Override
    public void restore(List<State<InternalRow>> state) {}

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {}

    @Override
    public void withIgnoreNumBucketCheck(boolean ignoreNumBucketCheck) {}

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void withCompactExecutor(ExecutorService compactExecutor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(BinaryRow partition, int bucket, InternalRow data) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        throw new UnsupportedOperationException();
    }
}
