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

/** An abstraction layer above {@link FileStoreWrite} to provide {@link RowData} writing. */
public abstract class TableWrite {

    private final FileStoreWrite write;
    private final SinkRecordConverter recordConverter;
    private final boolean overwrite;

    private final Map<BinaryRowData, Map<Integer, RecordWriter>> writers;
    private final ExecutorService compactExecutor;

    protected TableWrite(
            FileStoreWrite write, SinkRecordConverter recordConverter, boolean overwrite) {
        this.write = write;
        this.recordConverter = recordConverter;
        this.overwrite = overwrite;

        this.writers = new HashMap<>();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
    }

    public void write(RowData rowData) throws Exception {
        SinkRecord record = recordConverter.convert(rowData);
        RecordWriter writer = getWriter(record.partition(), record.bucket());
        writeImpl(record, writer);
    }

    public List<FileCommittable> prepareCommit() throws Exception {
        List<FileCommittable> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRowData, Map<Integer, RecordWriter>>> partIter =
                writers.entrySet().iterator();
        while (partIter.hasNext()) {
            Map.Entry<BinaryRowData, Map<Integer, RecordWriter>> partEntry = partIter.next();
            BinaryRowData partition = partEntry.getKey();
            Iterator<Map.Entry<Integer, RecordWriter>> bucketIter =
                    partEntry.getValue().entrySet().iterator();
            while (bucketIter.hasNext()) {
                Map.Entry<Integer, RecordWriter> entry = bucketIter.next();
                int bucket = entry.getKey();
                RecordWriter writer = entry.getValue();
                FileCommittable committable =
                        new FileCommittable(partition, bucket, writer.prepareCommit());
                result.add(committable);

                // clear if no update
                // we need a mechanism to clear writers, otherwise there will be more and more
                // such as yesterday's partition that no longer needs to be written.
                if (committable.increment().newFiles().isEmpty()) {
                    closeWriter(writer);
                    bucketIter.remove();
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }

    private void closeWriter(RecordWriter writer) throws Exception {
        writer.sync();
        writer.close();
    }

    public void close() throws Exception {
        compactExecutor.shutdownNow();
        for (Map<Integer, RecordWriter> bucketWriters : writers.values()) {
            for (RecordWriter writer : bucketWriters.values()) {
                closeWriter(writer);
            }
        }
        writers.clear();
    }

    protected abstract void writeImpl(SinkRecord record, RecordWriter writer) throws Exception;

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
                                ? write.createEmptyWriter(partition.copy(), bucket, compactExecutor)
                                : write.createWriter(partition.copy(), bucket, compactExecutor));
    }
}
