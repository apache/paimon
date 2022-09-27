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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.store.table.sink.WriteFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Base {@link FileStoreWrite} implementation.
 *
 * @param <T> type of record to write.
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;

    @Nullable protected IOManager ioManager;

    protected final Map<BinaryRowData, Map<Integer, RecordWriter<T>>> writers;
    private final ExecutorService compactExecutor;
    private final WriteFunction<T> writeFunction;

    private boolean overwrite = false;

    protected AbstractFileStoreWrite(SnapshotManager snapshotManager, FileStoreScan scan, WriteFunction<T> writeFunction) {
        this.snapshotManager = snapshotManager;
        this.scan = scan;

        this.writers = new HashMap<>();
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("compaction-thread"));
        this.writeFunction = writeFunction;
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    protected List<DataFileMeta> scanExistingFileMetas(BinaryRowData partition, int bucket) {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        List<DataFileMeta> existingFileMetas = Lists.newArrayList();
        if (latestSnapshotId != null) {
            // Concat all the DataFileMeta of existing files into existingFileMetas.
            scan.withSnapshot(latestSnapshotId)
                    .withPartitionFilter(Collections.singletonList(partition)).withBucket(bucket)
                    .plan().files().stream()
                    .map(ManifestEntry::file)
                    .forEach(existingFileMetas::add);
        }
        return existingFileMetas;
    }

    public void withOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void write(SinkRecord record) throws Exception {
        RecordWriter<T> writer = getWriter(record.partition(), record.bucket());
        writeFunction.write(record, writer);
    }

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
                        ? createEmptyWriter(partition.copy(), bucket, compactExecutor)
                        : createWriter(partition.copy(), bucket, compactExecutor);
        notifyNewWriter(writer);
        return writer;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}
}
