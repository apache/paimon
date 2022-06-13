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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.connector.StatefulPrecommittingSinkWriter;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.log.LogWriteCallback;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.SinkRecordConverter;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A {@link SinkWriter} for dynamic store. */
public class StoreSinkWriter<WriterStateT>
        implements StatefulPrecommittingSinkWriter<WriterStateT> {

    private final TableWrite write;

    @Nullable private final SinkWriter<SinkRecord> logWriter;

    @Nullable private final LogWriteCallback logCallback;

    private final ExecutorService compactExecutor;

    public StoreSinkWriter(
            TableWrite write,
            @Nullable SinkWriter<SinkRecord> logWriter,
            @Nullable LogWriteCallback logCallback) {
        this.write = write;
        this.logWriter = logWriter;
        this.logCallback = logCallback;
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
    }

    @Override
    public void write(RowData rowData, Context context) throws IOException, InterruptedException {
        SinkRecord record;
        try {
            record = write.write(rowData);
        } catch (Exception e) {
            throw new IOException(e);
        }

        // write to log store, need to preserve original pk (which includes partition fields)
        if (logWriter != null) {
            SinkRecordConverter converter = write.recordConverter();
            logWriter.write(converter.convertToLogSinkRecord(record), context);
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
        return Collections.emptyList();
    }

    @Override
    public List<Committable> prepareCommit() throws IOException, InterruptedException {
        List<Committable> committables = new ArrayList<>();
        try {
            for (FileCommittable committable : write.prepareCommit()) {
                committables.add(new Committable(Committable.Kind.FILE, committable));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

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
        write.close();

        if (logWriter != null) {
            logWriter.close();
        }
    }

    @VisibleForTesting
    Map<BinaryRowData, Map<Integer, RecordWriter>> writers() {
        return write.writers();
    }
}
