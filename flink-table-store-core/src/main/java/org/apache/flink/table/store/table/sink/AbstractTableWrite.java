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
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.operation.FileStoreWrite;
import org.apache.flink.table.store.file.utils.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.utils.MemoryPoolFactory;
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
public abstract class AbstractTableWrite<T>
        implements TableWrite, MemoryPoolFactory.PreemptRunner<RecordWriter<T>> {

    private final FileStoreWrite<T> write;
    private final SinkRecordConverter recordConverter;

    private final Map<BinaryRowData, Map<Integer, RecordWriter<T>>> writers;
    private final ExecutorService compactExecutor;
    private final MemoryPoolFactory<RecordWriter<T>> memoryPoolFactory;

    private boolean overwrite = false;

    protected AbstractTableWrite(
            FileStoreWrite<T> write,
            SinkRecordConverter recordConverter,
            FileStoreOptions options) {
        this.write = write;
        this.recordConverter = recordConverter;

        MergeTreeOptions mergeTreeOptions = options.mergeTreeOptions();
        HeapMemorySegmentPool memoryPool =
                new HeapMemorySegmentPool(
                        mergeTreeOptions.writeBufferSize, mergeTreeOptions.pageSize);
        this.memoryPoolFactory = new MemoryPoolFactory<>(memoryPool, this);

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
    public List<FileCommittable> prepareCommit() throws Exception {
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
                        new FileCommittable(partition, bucket, writer.prepareCommit());
                result.add(committable);

                // clear if no update
                // we need a mechanism to clear writers, otherwise there will be more and more
                // such as yesterday's partition that no longer needs to be written.
                if (committable.increment().newFiles().isEmpty()) {
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

    @Override
    public void preemptMemory(RecordWriter<T> owner) {
        long maxMemory = -1;
        RecordWriter<T> max = null;
        for (Map<Integer, RecordWriter<T>> bucket : writers.values()) {
            for (RecordWriter<T> writer : bucket.values()) {
                // Don't preempt yourself! Write and flush at the same time, which may lead to
                // inconsistent state
                if (writer != owner && writer.memoryOccupancy() > maxMemory) {
                    maxMemory = writer.memoryOccupancy();
                    max = writer;
                }
            }
        }

        if (max != null) {
            try {
                max.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

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
        MemorySegmentPool memoryPool = memoryPoolFactory.create(writer);
        writer.open(memoryPool);
        return writer;
    }
}
