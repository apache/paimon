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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/** Internal interface to Sink Data from input. */
public interface SinkWriter<T> {

    boolean write(T data) throws IOException;

    List<DataFileMeta> flush() throws IOException;

    boolean flushMemory() throws IOException;

    long memoryOccupancy();

    void close();

    void setMemoryPool(MemorySegmentPool memoryPool);

    boolean bufferSpillableWriter();

    /**
     * Directly sink data to file, no memory cache here, use OrcWriter/ParquetWrite/etc directly
     * write data. May cause out-of-memory.
     */
    class DirectSinkWriter<T> implements SinkWriter<T> {

        private final Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier;

        private RollingFileWriter<T, DataFileMeta> writer;

        public DirectSinkWriter(Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier) {
            this.writerSupplier = writerSupplier;
        }

        @Override
        public boolean write(T data) throws IOException {
            if (writer == null) {
                writer = writerSupplier.get();
            }
            writer.write(data);
            return true;
        }

        public void writeBundle(BundleRecords bundle) throws IOException {
            if (writer == null) {
                writer = writerSupplier.get();
            }
            writer.writeBundle(bundle);
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
     * Base class for buffered sink writers, contains common logic for flushing, writing bundles,
     * and managing lifecycle.
     */
    abstract class BaseBufferedSinkWriter<T> implements SinkWriter<T> {

        protected final Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier;
        protected final Function<T, InternalRow> toRow;
        protected final Function<InternalRow, T> fromRow;
        protected final IOManager ioManager;
        protected final RowType rowType;

        protected BaseBufferedSinkWriter(
                Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier,
                Function<T, InternalRow> toRow,
                Function<InternalRow, T> fromRow,
                IOManager ioManager,
                RowType rowType) {
            this.writerSupplier = writerSupplier;
            this.toRow = toRow;
            this.fromRow = fromRow;
            this.ioManager = ioManager;
            this.rowType = rowType;
        }

        public void writeBundle(BundleRecords bundle) throws IOException {
            for (InternalRow row : bundle) {
                write(fromRow.apply(row));
            }
        }

        protected void writeToWriter(RollingFileWriter<T, DataFileMeta> writer, InternalRow row)
                throws IOException {
            writer.write(fromRow.apply(row));
        }
    }

    /**
     * Use buffered writer, segment pooled from segment pool. When spillable, may delay checkpoint
     * acknowledge time. When non-spillable, may cause too many small files.
     */
    class BufferedSinkWriter<T> extends BaseBufferedSinkWriter<T> {

        private final boolean spillable;
        private final MemorySize maxDiskSize;
        private final CompressOptions compression;

        private RowBuffer writeBuffer;

        public BufferedSinkWriter(
                Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier,
                Function<T, InternalRow> toRow,
                Function<InternalRow, T> fromRow,
                IOManager ioManager,
                RowType rowType,
                boolean spillable,
                MemorySize maxDiskSize,
                CompressOptions compression) {
            super(writerSupplier, toRow, fromRow, ioManager, rowType);
            this.spillable = spillable;
            this.maxDiskSize = maxDiskSize;
            this.compression = compression;
        }

        public RowBuffer rowBuffer() {
            return writeBuffer;
        }

        @Override
        public boolean write(T data) throws IOException {
            return writeBuffer.put(toRow.apply(data));
        }

        @Override
        public List<DataFileMeta> flush() throws IOException {
            List<DataFileMeta> flushedFiles = new ArrayList<>();
            if (writeBuffer != null) {
                RollingFileWriter<T, DataFileMeta> writer = writerSupplier.get();
                IOException exception = null;
                try (RowBuffer.RowBufferIterator iterator = writeBuffer.newIterator()) {
                    while (iterator.advanceNext()) {
                        writeToWriter(writer, iterator.getRow());
                    }
                } catch (IOException e) {
                    exception = e;
                } finally {
                    if (exception != null) {
                        IOUtils.closeQuietly(writer);
                        throw exception;
                    }
                    writer.close();
                }
                flushedFiles.addAll(writer.result());
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
                            new InternalRowSerializer(rowType),
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

    /** Sink writer that sorts data within each file according to clustering configuration. */
    class SortedBufferedSinkWriter<T> extends BaseBufferedSinkWriter<T> {

        private final CoreOptions options;
        private final boolean spillable;

        private SortBuffer sortBuffer;
        private boolean useSpillSortBuffer;

        public SortedBufferedSinkWriter(
                Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier,
                Function<T, InternalRow> toRow,
                Function<InternalRow, T> fromRow,
                IOManager ioManager,
                RowType rowType,
                CoreOptions options,
                boolean spillable) {
            super(writerSupplier, toRow, fromRow, ioManager, rowType);
            this.options = options;
            this.spillable = spillable;
        }

        public SortBuffer sortBuffer() {
            return sortBuffer;
        }

        @Override
        public boolean write(T data) throws IOException {
            return sortBuffer.write(toRow.apply(data));
        }

        @Override
        public List<DataFileMeta> flush() throws IOException {
            List<DataFileMeta> flushedFiles = new ArrayList<>();
            if (sortBuffer.size() > 0) {
                RollingFileWriter<T, DataFileMeta> writer = writerSupplier.get();
                IOException exception = null;
                try {
                    MutableObjectIterator<BinaryRow> sorted = sortBuffer.sortedIterator();
                    BinaryRow row = new BinaryRow(rowType.getFieldCount());
                    BinaryRow reuse;
                    while ((reuse = sorted.next(row)) != null) {
                        writeToWriter(writer, reuse);
                    }
                } catch (IOException e) {
                    exception = e;
                } finally {
                    if (exception != null) {
                        IOUtils.closeQuietly(writer);
                        throw exception;
                    }
                    writer.close();
                }
                flushedFiles.addAll(writer.result());
                sortBuffer.clear();
            }
            return flushedFiles;
        }

        @Override
        public long memoryOccupancy() {
            return sortBuffer.getOccupancy();
        }

        @Override
        public void close() {
            if (sortBuffer != null) {
                sortBuffer.clear();
                sortBuffer = null;
            }
        }

        @Override
        public void setMemoryPool(MemorySegmentPool memoryPool) {
            List<String> clusteringColumns = options.clusteringColumns();
            int[] keyFields =
                    clusteringColumns.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
            RecordComparator comparator =
                    CodeGenUtils.newRecordComparator(rowType.getFieldTypes(), keyFields);
            BinaryInMemorySortBuffer inMemorySortBuffer =
                    BinaryInMemorySortBuffer.createBuffer(
                            CodeGenUtils.newNormalizedKeyComputer(
                                    rowType.getFieldTypes(), keyFields),
                            new InternalRowSerializer(rowType),
                            comparator,
                            memoryPool);

            this.useSpillSortBuffer = ioManager != null && spillable;
            this.sortBuffer =
                    useSpillSortBuffer
                            ? new BinaryExternalSortBuffer(
                                    new BinaryRowSerializer(rowType.getFieldCount()),
                                    comparator,
                                    memoryPool.pageSize(),
                                    inMemorySortBuffer,
                                    ioManager,
                                    options.localSortMaxNumFileHandles(),
                                    options.spillCompressOptions(),
                                    options.writeBufferSpillDiskSize())
                            : inMemorySortBuffer;
        }

        @Override
        public boolean bufferSpillableWriter() {
            return useSpillSortBuffer;
        }

        @Override
        public boolean flushMemory() throws IOException {
            return sortBuffer.flushMemory();
        }
    }
}
