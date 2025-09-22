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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BatchRecordWriter;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SinkWriter;
import org.apache.paimon.utils.SinkWriter.BufferedSinkWriter;
import org.apache.paimon.utils.SinkWriter.DirectSinkWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** {@link RecordWriter} for format table. */
public class FormatTableRecordWriter implements BatchRecordWriter, MemoryOwner {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final List<DataFileMeta> files;
    private final @Nullable IOManager ioManager;
    private final CompressOptions spillCompression;
    private final MemorySize maxDiskSize;
    private final RowType writeSchema;
    private final String fileCompression;
    private final FileFormat fileFormat;
    private final long targetFileSize;
    private MemorySegmentPool memorySegmentPool;
    private final LongCounter seqNumCounter;

    private SinkWriter<InternalRow> sinkWriter;

    public FormatTableRecordWriter(
            FileIO fileIO,
            @Nullable IOManager ioManager,
            FileFormat fileFormat,
            long targetFileSize,
            DataFilePathFactory pathFactory,
            CompressOptions spillCompression,
            MemorySize maxDiskSize,
            boolean useWriteBuffer,
            boolean spillable,
            RowType writeSchema,
            String fileCompression) {
        this.ioManager = ioManager;
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.spillCompression = spillCompression;
        this.fileCompression = fileCompression;
        this.maxDiskSize = maxDiskSize;
        this.files = new ArrayList<>();
        this.writeSchema = writeSchema;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
        this.seqNumCounter = new LongCounter(1);
        this.sinkWriter =
                useWriteBuffer
                        ? createBufferedSinkWriter(spillable)
                        : new DirectSinkWriter<>(this::createRollingRowWriter);
    }

    @Override
    public void write(InternalRow rowData) throws Exception {
        Preconditions.checkArgument(
                rowData.getRowKind().isAdd(),
                "Append-only writer can only accept insert or update_after row kind, but current row kind is: %s. "
                        + "You can configure 'ignore-delete' to ignore retract records.",
                rowData.getRowKind());
        boolean success = sinkWriter.write(rowData);
        if (!success) {
            closeAndGetCommitters();
            success = sinkWriter.write(rowData);
            if (!success) {
                // Should not get here, because writeBuffer will throw too big exception out.
                // But we throw again in case of something unexpected happens. (like someone changed
                // code in SpillableBuffer.)
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws Exception {
        return sinkWriter.closeAndGetCommitters();
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {}

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        this.files.addAll(files);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return new ArrayList<>(files);
    }

    @Override
    public long maxSequenceNumber() {
        return seqNumCounter.getValue() - 1;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.memorySegmentPool = memoryPool;
        sinkWriter.setMemoryPool(memoryPool);
    }

    @Override
    public long memoryOccupancy() {
        return sinkWriter.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = sinkWriter.flushMemory();
        if (!success) {
            closeAndGetCommitters();
        }
    }

    private BufferedSinkWriter<InternalRow> createBufferedSinkWriter(boolean spillable) {
        return new BufferedSinkWriter<>(
                this::createRollingRowWriter,
                t -> t,
                t -> t,
                ioManager,
                writeSchema,
                spillable,
                maxDiskSize,
                spillCompression);
    }

    private RowDataRollingFileWriter createRollingRowWriter() {
        return new RowDataRollingFileWriter(
                fileIO,
                0L,
                fileFormat,
                targetFileSize,
                writeSchema,
                pathFactory,
                new LongCounter(0),
                fileCompression,
                null,
                new FileIndexOptions(),
                FileSource.APPEND,
                false,
                false,
                null,
                true);
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        throw new UnsupportedOperationException("Not supported.");
    }

    @VisibleForTesting
    public boolean useBufferedSinkWriter() {
        return sinkWriter instanceof BufferedSinkWriter;
    }

    @Override
    public boolean compactNotCompleted() {
        return false;
    }

    @Override
    public void sync() throws Exception {}

    @Override
    public void close() throws Exception {
        sinkWriter.close();
    }

    @Override
    public void writeBundle(BundleRecords record) throws Exception {}
}
