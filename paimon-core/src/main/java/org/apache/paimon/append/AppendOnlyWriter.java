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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements RecordWriter<InternalRow>, MemoryOwner {

    private final FileIO fileIO;
    private final long schemaId;
    private final FileFormat fileFormat;
    private final long targetFileSize;
    private final RowType writeSchema;
    private final DataFilePathFactory pathFactory;
    private final CompactManager compactManager;
    private final AppendOnlyFileStoreWrite.BucketFileRead bucketFileRead;
    private final boolean forceCompact;
    private final List<DataFileMeta> newFiles;
    private final List<DataFileMeta> deletedFiles;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final LongCounter seqNumCounter;
    private final String fileCompression;
    private final String spillCompression;
    private SinkWriter sinkWriter;
    private final FieldStatsCollector.Factory[] statsCollectors;
    private final IOManager ioManager;

    private final CoreOptions coreOptions;

    private MemorySegmentPool memorySegmentPool;
    private MemorySize maxDiskSize;

    public AppendOnlyWriter(
            FileIO fileIO,
            IOManager ioManager,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            long maxSequenceNumber,
            CompactManager compactManager,
            AppendOnlyFileStoreWrite.BucketFileRead bucketFileRead,
            boolean forceCompact,
            DataFilePathFactory pathFactory,
            @Nullable CommitIncrement increment,
            boolean useWriteBuffer,
            boolean spillable,
            String fileCompression,
            String spillCompression,
            FieldStatsCollector.Factory[] statsCollectors,
            MemorySize maxDiskSize,
            CoreOptions coreOptions) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
        this.writeSchema = writeSchema;
        this.pathFactory = pathFactory;
        this.compactManager = compactManager;
        this.bucketFileRead = bucketFileRead;
        this.forceCompact = forceCompact;
        this.newFiles = new ArrayList<>();
        this.deletedFiles = new ArrayList<>();
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.seqNumCounter = new LongCounter(maxSequenceNumber + 1);
        this.fileCompression = fileCompression;
        this.spillCompression = spillCompression;
        this.ioManager = ioManager;
        this.statsCollectors = statsCollectors;
        this.maxDiskSize = maxDiskSize;

        this.sinkWriter =
                useWriteBuffer
                        ? new BufferedSinkWriter(spillable, maxDiskSize, spillCompression)
                        : new DirectSinkWriter();

        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            compactBefore.addAll(increment.compactIncrement().compactBefore());
            compactAfter.addAll(increment.compactIncrement().compactAfter());
        }
        this.coreOptions = coreOptions;
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
            flush(false, false);
            success = sinkWriter.write(rowData);
            if (!success) {
                // Should not get here, because writeBuffer will throw too big exception out.
                // But we throw again in case of something unexpected happens. (like someone changed
                // code in SpillableBuffer.)
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flush(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flush(false, false);
        trySyncLatestCompaction(waitCompaction || forceCompact);
        return drainIncrement();
    }

    @Override
    public boolean isCompacting() {
        return compactManager.isCompacting();
    }

    @VisibleForTesting
    void flush(boolean waitForLatestCompaction, boolean forcedFullCompaction) throws Exception {
        List<DataFileMeta> flushedFiles = sinkWriter.flush();

        // add new generated files
        flushedFiles.forEach(compactManager::addNewFile);
        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
        newFiles.addAll(flushedFiles);
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();

        compactManager.close();
        for (DataFileMeta file : compactAfter) {
            // appendOnlyCompactManager will rewrite the file and no file upgrade will occur, so we
            // can directly delete the file in compactAfter.
            fileIO.deleteQuietly(pathFactory.toPath(file.fileName()));
        }

        sinkWriter.close();
    }

    public void toBufferedWriter() throws Exception {
        if (sinkWriter != null && !sinkWriter.bufferSpillableWriter() && bucketFileRead != null) {
            // fetch the written results
            List<DataFileMeta> files = sinkWriter.flush();

            sinkWriter.close();
            sinkWriter = new BufferedSinkWriter(true, maxDiskSize, spillCompression);
            sinkWriter.setMemoryPool(memorySegmentPool);

            // rewrite small files
            try (RecordReaderIterator<InternalRow> reader = bucketFileRead.read(files)) {
                while (reader.hasNext()) {
                    sinkWriter.write(reader.next());
                }
            } finally {
                // remove small files
                for (DataFileMeta file : files) {
                    fileIO.deleteQuietly(pathFactory.toPath(file.fileName()));
                }
            }
        }
    }

    private RowDataRollingFileWriter createRollingRowWriter() {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                targetFileSize,
                writeSchema,
                pathFactory,
                seqNumCounter,
                fileCompression,
                statsCollectors,
                coreOptions);
    }

    private void trySyncLatestCompaction(boolean blocking)
            throws ExecutionException, InterruptedException {
        compactManager
                .getCompactionResult(blocking)
                .ifPresent(
                        result -> {
                            compactBefore.addAll(result.before());
                            compactAfter.addAll(result.after());
                        });
    }

    private CommitIncrement drainIncrement() {
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        Collections.emptyList());
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore),
                        new ArrayList<>(compactAfter),
                        Collections.emptyList());

        newFiles.clear();
        deletedFiles.clear();
        compactBefore.clear();
        compactAfter.clear();

        return new CommitIncrement(dataIncrement, compactIncrement);
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
            flush(false, false);
        }
    }

    @VisibleForTesting
    public RowBuffer getWriteBuffer() {
        if (sinkWriter instanceof BufferedSinkWriter) {
            return ((BufferedSinkWriter) sinkWriter).writeBuffer;
        } else {
            return null;
        }
    }

    @VisibleForTesting
    List<DataFileMeta> getNewFiles() {
        return newFiles;
    }

    /** Internal interface to Sink Data from input. */
    private interface SinkWriter {

        boolean write(InternalRow data) throws IOException;

        List<DataFileMeta> flush() throws IOException;

        boolean flushMemory() throws IOException;

        long memoryOccupancy();

        void close();

        void setMemoryPool(MemorySegmentPool memoryPool);

        boolean bufferSpillableWriter();
    }

    /**
     * Directly sink data to file, no memory cache here, use OrcWriter/ParquetWrite/etc directly
     * write data. May cause out-of-memory.
     */
    private class DirectSinkWriter implements SinkWriter {

        private RowDataRollingFileWriter writer;

        @Override
        public boolean write(InternalRow data) throws IOException {
            if (writer == null) {
                writer = createRollingRowWriter();
            }
            writer.write(data);
            return true;
        }

        @Override
        public List<DataFileMeta> flush() throws IOException {
            List<DataFileMeta> flushedFiles = new ArrayList<>();
            if (writer != null) {
                writer.close();
                flushedFiles.addAll(writer.result());
                writer = null;
            }
            return flushedFiles;
        }

        @Override
        public boolean flushMemory() throws IOException {
            return false;
        }

        @Override
        public long memoryOccupancy() {
            return 0;
        }

        @Override
        public void close() {
            if (writer != null) {
                writer.abort();
                writer = null;
            }
        }

        @Override
        public void setMemoryPool(MemorySegmentPool memoryPool) {
            // do nothing
        }

        @Override
        public boolean bufferSpillableWriter() {
            return false;
        }
    }

    /**
     * Use buffered writer, segment pooled from segment pool. When spillable, may delay checkpoint
     * acknowledge time. When non-spillable, may cause too many small files.
     */
    private class BufferedSinkWriter implements SinkWriter {

        private final boolean spillable;

        private final MemorySize maxDiskSize;

        private final String compression;

        private RowBuffer writeBuffer;

        private BufferedSinkWriter(boolean spillable, MemorySize maxDiskSize, String compression) {
            this.spillable = spillable;
            this.maxDiskSize = maxDiskSize;
            this.compression = compression;
        }

        @Override
        public boolean write(InternalRow data) throws IOException {
            return writeBuffer.put(data);
        }

        @Override
        public List<DataFileMeta> flush() throws IOException {
            List<DataFileMeta> flushedFiles = new ArrayList<>();
            if (writeBuffer != null) {
                writeBuffer.complete();
                RowDataRollingFileWriter writer = createRollingRowWriter();
                IOException exception = null;
                try (RowBuffer.RowBufferIterator iterator = writeBuffer.newIterator()) {
                    while (iterator.advanceNext()) {
                        writer.write(iterator.getRow());
                    }
                } catch (IOException e) {
                    exception = e;
                } finally {
                    if (exception != null) {
                        IOUtils.closeQuietly(writer);
                        // cleanup code that might throw another exception
                        throw exception;
                    }
                    writer.close();
                }
                flushedFiles.addAll(writer.result());
                // reuse writeBuffer
                writeBuffer.reset();
            }
            return flushedFiles;
        }

        @Override
        public long memoryOccupancy() {
            return writeBuffer.memoryOccupancy();
        }

        @Override
        public void close() {
            if (writeBuffer != null) {
                writeBuffer.reset();
                writeBuffer = null;
            }
        }

        @Override
        public void setMemoryPool(MemorySegmentPool memoryPool) {
            writeBuffer =
                    RowBuffer.getBuffer(
                            ioManager,
                            memoryPool,
                            new InternalRowSerializer(writeSchema),
                            spillable,
                            maxDiskSize,
                            compression);
        }

        @Override
        public boolean bufferSpillableWriter() {
            return spillable;
        }

        @Override
        public boolean flushMemory() throws IOException {
            return writeBuffer.flushMemory();
        }
    }
}
