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

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
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

    List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws IOException;

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
        public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws IOException {
            List<TwoPhaseOutputStream.Committer> commits = new ArrayList<>();

            if (writer != null) {
                writer.close();
                commits.addAll(writer.committers());
                writer = null;
            }
            return commits;
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
    class BufferedSinkWriter<T> implements SinkWriter<T> {

        private final Supplier<RollingFileWriter<T, DataFileMeta>> writerSupplier;
        private final Function<T, InternalRow> toRow;
        private final Function<InternalRow, T> fromRow;
        private final IOManager ioManager;
        private final RowType rowType;
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
            this.writerSupplier = writerSupplier;
            this.toRow = toRow;
            this.fromRow = fromRow;
            this.ioManager = ioManager;
            this.rowType = rowType;
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
                        writer.write(fromRow.apply(iterator.getRow()));
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
        public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws IOException {
            List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
            if (writeBuffer != null) {
                RollingFileWriter<T, DataFileMeta> writer = writerSupplier.get();
                IOException exception = null;
                try (RowBuffer.RowBufferIterator iterator = writeBuffer.newIterator()) {
                    while (iterator.advanceNext()) {
                        writer.write(fromRow.apply(iterator.getRow()));
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
                committers.addAll(writer.committers());
                // reuse writeBuffer
                writeBuffer.reset();
            }
            return committers;
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
}
