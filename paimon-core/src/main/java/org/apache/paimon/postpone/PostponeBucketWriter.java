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

package org.apache.paimon.postpone;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SinkWriter;
import org.apache.paimon.utils.SinkWriter.BufferedSinkWriter;
import org.apache.paimon.utils.SinkWriter.DirectSinkWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** {@link RecordWriter} for {@code bucket = -2} tables. */
public class PostponeBucketWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final MergeFunction<KeyValue> mergeFunction;
    private final KeyValueFileWriterFactory writerFactory;
    private final List<DataFileMeta> files;
    private final IOFunction<List<DataFileMeta>, RecordReaderIterator<KeyValue>> fileRead;
    private final @Nullable IOManager ioManager;
    private final CompressOptions spillCompression;
    private final MemorySize maxDiskSize;

    private SinkWriter<KeyValue> sinkWriter;
    private MemorySegmentPool memorySegmentPool;
    private boolean retractValidated = false;

    public PostponeBucketWriter(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            CompressOptions spillCompression,
            MemorySize maxDiskSize,
            @Nullable IOManager ioManager,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            IOFunction<List<DataFileMeta>, RecordReaderIterator<KeyValue>> fileRead,
            boolean useWriteBuffer,
            boolean spillable,
            @Nullable CommitIncrement restoreIncrement) {
        this.ioManager = ioManager;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.fileRead = fileRead;
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.spillCompression = spillCompression;
        this.maxDiskSize = maxDiskSize;
        this.files = new ArrayList<>();
        if (restoreIncrement != null) {
            files.addAll(restoreIncrement.newFilesIncrement().newFiles());
        }
        this.sinkWriter =
                useWriteBuffer
                        ? createBufferedSinkWriter(spillable)
                        : new DirectSinkWriter<>(this::createRollingRowWriter);
    }

    private RollingFileWriterImpl<KeyValue, DataFileMeta> createRollingRowWriter() {
        return writerFactory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
    }

    @Override
    public void write(KeyValue record) throws Exception {
        validateRetract(record);
        boolean success = sinkWriter.write(record);
        if (!success) {
            flush();
            success = sinkWriter.write(record);
            if (!success) {
                // Should not get here, because writeBuffer will throw too big exception out.
                // But we throw again in case of something unexpected happens. (like someone changed
                // code in SpillableBuffer.)
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    private void validateRetract(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            if (retractValidated) {
                return;
            }
            mergeFunction.reset();
            mergeFunction.add(kv);
            mergeFunction.getResult();
            retractValidated = true;
        }
    }

    private void flush() throws Exception {
        files.addAll(sinkWriter.flush());
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
        // see comments in the constructor of PostponeBucketFileStoreWrite
        return 0;
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
            flush();
        }
    }

    private BufferedSinkWriter<KeyValue> createBufferedSinkWriter(boolean spillable) {
        RowType keyType = writerFactory.keyType();
        RowType valueType = writerFactory.valueType();
        RowType kvRowType = KeyValue.schema(keyType, valueType);
        KeyValueSerializer serializer = new KeyValueSerializer(keyType, valueType);
        return new BufferedSinkWriter<>(
                this::createRollingRowWriter,
                serializer::toRow,
                serializer::fromRow,
                ioManager,
                kvRowType,
                spillable,
                maxDiskSize,
                spillCompression);
    }

    public void toBufferedWriter() throws Exception {
        if (sinkWriter != null && !sinkWriter.bufferSpillableWriter() && fileRead != null) {
            // fetch the written results
            List<DataFileMeta> files = sinkWriter.flush();

            sinkWriter.close();
            sinkWriter = createBufferedSinkWriter(true);
            sinkWriter.setMemoryPool(memorySegmentPool);

            // rewrite small files
            try (RecordReaderIterator<KeyValue> reader = fileRead.apply(files)) {
                while (reader.hasNext()) {
                    sinkWriter.write(reader.next());
                }
            } finally {
                // remove small files
                for (DataFileMeta file : files) {
                    fileIO.deleteQuietly(pathFactory.toPath(file));
                }
            }
        }
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flush();
        List<DataFileMeta> result = new ArrayList<>(files);
        files.clear();
        return new CommitIncrement(
                new DataIncrement(result, Collections.emptyList(), Collections.emptyList()),
                CompactIncrement.emptyIncrement(),
                null);
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
}
