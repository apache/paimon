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
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.MemoryFileStoreWrite;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.apache.paimon.format.FileFormat.fileFormat;

/** File write for format table. */
public class FormatTableFileWrite extends MemoryFileStoreWrite<InternalRow> {

    private final FileIO fileIO;
    private final RowType rowType;
    private final FileFormat fileFormat;
    private final FileStorePathFactory pathFactory;
    private boolean forceBufferSpill = false;
    protected final Map<BinaryRow, RecordWriter<InternalRow>> writers;

    public FormatTableFileWrite(
            FileIO fileIO,
            long schemaId,
            RowType rowType,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            CoreOptions options,
            String tableName) {
        super(null, null, options, partitionType, null, null, tableName);
        this.fileIO = fileIO;
        this.rowType = rowType;
        this.fileFormat = fileFormat(options);
        this.pathFactory = pathFactory;
        this.writers = new HashMap<>();
    }

    @Override
    public FileStoreWrite<InternalRow> withMemoryPool(MemorySegmentPool memoryPool) {
        return super.withMemoryPool(memoryPool);
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        // in unaware bucket mode, we need all writers to be empty
        super.withIgnorePreviousFiles(true);
    }

    @Override
    protected Function<WriterContainer<InternalRow>, Boolean> createWriterCleanChecker() {
        return createNoConflictAwareWriterCleanChecker();
    }

    @Override
    protected RecordWriter<InternalRow> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
        throw new UnsupportedOperationException();
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

    public void flush() throws Exception {
        for (RecordWriter<InternalRow> writer : writers.values()) {
            writer.prepareCommit(false);
        }
    }
}
