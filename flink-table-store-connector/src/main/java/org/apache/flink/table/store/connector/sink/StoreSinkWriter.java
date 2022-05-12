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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.log.LogWriteCallback;
import org.apache.flink.table.store.sink.SinkRecord;
import org.apache.flink.table.store.sink.SinkRecordConverter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A {@link SinkWriter} for dynamic store. */
public class StoreSinkWriter<WriterStateT> extends StoreSinkWriterBase<WriterStateT> {

    private static final BinaryRowData DUMMY_KEY = BinaryRowDataUtil.EMPTY_ROW;

    private final FileStoreWrite fileStoreWrite;

    private final SinkRecordConverter recordConverter;

    private final WriteMode writeMode;
    private final boolean overwrite;

    @Nullable private final SinkWriter<SinkRecord> logWriter;

    @Nullable private final LogWriteCallback logCallback;

    private final ExecutorService compactExecutor;

    public StoreSinkWriter(
            FileStoreWrite fileStoreWrite,
            SinkRecordConverter recordConverter,
            WriteMode writeMode,
            boolean overwrite,
            @Nullable SinkWriter<SinkRecord> logWriter,
            @Nullable LogWriteCallback logCallback) {
        this.fileStoreWrite = fileStoreWrite;
        this.recordConverter = recordConverter;
        this.writeMode = writeMode;
        this.overwrite = overwrite;
        this.logWriter = logWriter;
        this.logCallback = logCallback;
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
    }

    @Override
    protected RecordWriter createWriter(BinaryRowData partition, int bucket) {
        return overwrite
                ? fileStoreWrite.createEmptyWriter(partition.copy(), bucket, compactExecutor)
                : fileStoreWrite.createWriter(partition.copy(), bucket, compactExecutor);
    }

    @Override
    public void write(RowData rowData, Context context) throws IOException, InterruptedException {
        SinkRecord record = recordConverter.convert(rowData);
        RecordWriter writer = getWriter(record.partition(), record.bucket());
        try {
            writeToFileStore(writer, record);
        } catch (Exception e) {
            throw new IOException(e);
        }

        // write to log store, need to preserve original pk (which includes partition fields)
        if (logWriter != null) {
            record = recordConverter.convertToLogSinkRecord(record);
            logWriter.write(record, context);
        }
    }

    private void writeToFileStore(RecordWriter writer, SinkRecord record) throws Exception {
        if (writeMode == WriteMode.APPEND_ONLY) {
            Preconditions.checkState(
                    record.row().getRowKind() == RowKind.INSERT,
                    "Append only writer can not accept row with RowKind %s",
                    record.row().getRowKind());
            writer.write(ValueKind.ADD, DUMMY_KEY, record.row());
            return;
        }

        switch (record.row().getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (record.primaryKey().getArity() == 0) {
                    writer.write(ValueKind.ADD, record.row(), GenericRowData.of(1L));
                } else {
                    writer.write(ValueKind.ADD, record.primaryKey(), record.row());
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                if (record.primaryKey().getArity() == 0) {
                    writer.write(ValueKind.ADD, record.row(), GenericRowData.of(-1L));
                } else {
                    writer.write(ValueKind.DELETE, record.primaryKey(), record.row());
                }
                break;
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (logWriter != null) {
            logWriter.flush(endOfInput);
        }
    }

    @Override
    public List<WriterStateT> snapshotState(long checkpointId) throws IOException {
        if (logWriter != null && logWriter instanceof StatefulSinkWriter) {
            return ((StatefulSinkWriter<?, WriterStateT>) logWriter).snapshotState(checkpointId);
        }
        return super.snapshotState(checkpointId);
    }

    @Override
    public List<Committable> prepareCommit() throws IOException, InterruptedException {
        List<Committable> committables = super.prepareCommit();
        if (logWriter != null) {
            if (logWriter instanceof PrecommittingSinkWriter) {
                Collection<?> logCommittables =
                        ((PrecommittingSinkWriter<?, ?>) logWriter).prepareCommit();
                for (Object logCommittable : logCommittables) {
                    committables.add(new Committable(Committable.Kind.LOG, logCommittable));
                }
            }

            Objects.requireNonNull(logCallback, "logCallback should not be null.");
            logCallback
                    .offsets()
                    .forEach(
                            (k, v) ->
                                    committables.add(
                                            new Committable(
                                                    Committable.Kind.LOG_OFFSET,
                                                    new LogOffsetCommittable(k, v))));
        }
        return committables;
    }

    @Override
    public void close() throws Exception {
        this.compactExecutor.shutdownNow();
        if (logWriter != null) {
            logWriter.close();
        }
        super.close();
    }
}
