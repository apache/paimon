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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Base {@link TableWrite} implementation.
 *
 * @param <T> type of record to write into {@link org.apache.flink.table.store.file.FileStore}.
 */
public abstract class AbstractTableWrite<T> implements TableWrite {

    private final FileStoreWrite<T> write;
    private final SinkRecordConverter recordConverter;

    protected final Map<BinaryRowData, Map<Integer, RecordWriter<T>>> writers;
    private final ExecutorService compactExecutor;

    private boolean overwrite = false;

    protected AbstractTableWrite(FileStoreWrite<T> write, SinkRecordConverter recordConverter) {
        this.write = write;
        this.recordConverter = recordConverter;

        this.writers = new HashMap<>();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
    }

    @Override
    public TableWrite withOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    @Override
    public SinkRecordConverter recordConverter() {
        return recordConverter;
    }

    @Override
    public SinkRecord write(RowData rowData) throws Exception {
        SinkRecord record = recordConverter.convert(rowData);
        RecordWriter<T> writer = getWriter(record.partition(), record.bucket());
        writeSinkRecord(record, writer);
        return record;
    }

    @Override
    public List<FileCommittable> prepareCommit(boolean endOfInput) throws Exception {
        List<FileCommittable> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRowData, Map<Integer, RecordWriter<T>>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRowData, Map<Integer, RecordWriter<T>>> partEntry = partIter.next();
            BinaryRowData partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, RecordWriter<T>>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, RecordWriter<T>> entry = bucketIter.next();
                int bucket = entry.getKey();
                RecordWriter<T> writer = entry.getValue();
                FileCommittable committable =
                        new FileCommittable(partition, bucket, writer.prepareCommit(endOfInput));
                result.add(committable);

                // clear if no update
                // we need a mechanism to clear writers, otherwise there will be more and more
                // such as yesterday's partition that no longer needs to be written.
                if (committable.increment().isEmpty()) {
                    writer.close();
                    bucketIter.remove();
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }

    @Override
    public void close() throws Exception {
        for (Map<Integer, RecordWriter<T>> bucketWriters : writers.values()) {
            for (RecordWriter<T> writer : bucketWriters.values()) {
                writer.close();
            }
        }
        writers.clear();
        compactExecutor.shutdownNow();
    }

    @VisibleForTesting
    public Map<BinaryRowData, Map<Integer, RecordWriter<T>>> writers() {
        return writers;
    }

    protected abstract void writeSinkRecord(SinkRecord record, RecordWriter<T> writer)
            throws Exception;

    private RecordWriter<T> getWriter(BinaryRowData partition, int bucket) {
        Map<Integer, RecordWriter<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(bucket, k -> createWriter(partition.copy(), bucket));
    }

    private RecordWriter<T> createWriter(BinaryRowData partition, int bucket) {
        RecordWriter<T> writer =
                overwrite
                        ? write.createEmptyWriter(partition.copy(), bucket, compactExecutor)
                        : write.createWriter(partition.copy(), bucket, compactExecutor);
        notifyNewWriter(writer);
        return writer;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}
}
