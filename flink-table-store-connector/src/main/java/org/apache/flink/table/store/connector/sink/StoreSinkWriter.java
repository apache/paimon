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
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A {@link SinkWriter} for dynamic store. */
public class StoreSinkWriter implements SinkWriter<RowData, LocalCommittable, Void> {

    private final FileStoreWrite fileStoreWrite;

    private final SinkRecordConverter recordConverter;

    private final boolean overwrite;

    private final ExecutorService compactExecutor;

    private final Map<BinaryRowData, Map<Integer, RecordWriter>> writers;

    public StoreSinkWriter(
            FileStoreWrite fileStoreWrite, SinkRecordConverter recordConverter, boolean overwrite) {
        this.fileStoreWrite = fileStoreWrite;
        this.recordConverter = recordConverter;
        this.overwrite = overwrite;
        this.compactExecutor = Executors.newSingleThreadScheduledExecutor();
        this.writers = new HashMap<>();
    }

    private RecordWriter getWriter(BinaryRowData partition, int bucket) {
        Map<Integer, RecordWriter> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket,
                k ->
                        overwrite
                                ? fileStoreWrite.createEmptyWriter(
                                        partition, bucket, compactExecutor)
                                : fileStoreWrite.createWriter(partition, bucket, compactExecutor));
    }

    @Override
    public void write(RowData rowData, Context context) throws IOException {
        RowKind rowKind = rowData.getRowKind();
        SinkRecord record = recordConverter.convert(rowData);
        RecordWriter writer = getWriter(record.partition(), record.bucket());
        try {
            writeToFileStore(writer, record);
        } catch (Exception e) {
            throw new IOException(e);
        }
        rowData.setRowKind(rowKind);
    }

    private void writeToFileStore(RecordWriter writer, SinkRecord record) throws Exception {
        switch (record.rowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (record.key().getArity() == 0) {
                    writer.write(ValueKind.ADD, record.row(), GenericRowData.of(1));
                } else {
                    writer.write(ValueKind.ADD, record.key(), record.row());
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                if (record.key().getArity() == 0) {
                    writer.write(ValueKind.ADD, record.row(), GenericRowData.of(-1));
                } else {
                    writer.write(ValueKind.DELETE, record.key(), record.row());
                }
                break;
        }
    }

    @Override
    public List<LocalCommittable> prepareCommit(boolean flush) throws IOException {
        try {
            return prepareCommit();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private List<LocalCommittable> prepareCommit() throws Exception {
        List<LocalCommittable> committables = new ArrayList<>();
        for (BinaryRowData partition : writers.keySet()) {
            Map<Integer, RecordWriter> buckets = writers.get(partition);
            for (Integer bucket : buckets.keySet()) {
                RecordWriter writer = buckets.get(bucket);
                LocalCommittable committable =
                        new LocalCommittable(partition, bucket, writer.prepareCommit());
                committables.add(committable);

                // clear if no update
                // we need a mechanism to clear writers, otherwise there will be more and more
                // such as yesterday's partition that no longer needs to be written.
                if (committable.increment().newFiles().isEmpty()) {
                    closeWriter(writer);
                    buckets.remove(bucket);
                }
            }

            if (buckets.isEmpty()) {
                writers.remove(partition);
            }
        }
        return committables;
    }

    private void closeWriter(RecordWriter writer) throws Exception {
        writer.sync();
        writer.close();
    }

    @Override
    public void close() throws Exception {
        this.compactExecutor.shutdownNow();
        for (Map<Integer, RecordWriter> bucketWriters : writers.values()) {
            for (RecordWriter writer : bucketWriters.values()) {
                closeWriter(writer);
            }
        }
        writers.clear();
    }

    @VisibleForTesting
    Map<BinaryRowData, Map<Integer, RecordWriter>> writers() {
        return writers;
    }
}
