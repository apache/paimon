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

package org.apache.flink.table.store;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreCommitImpl;
import org.apache.flink.table.store.file.operation.FileStoreReadImpl;
import org.apache.flink.table.store.file.operation.FileStoreScanImpl;
import org.apache.flink.table.store.file.operation.FileStoreWriteImpl;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Helper class to write and read {@link RowData} with {@link FileStoreImpl}. */
public class FileStoreTestHelper {

    private final FileStoreImpl store;
    private final BiFunction<RowData, RowData, BinaryRowData> partitionCalculator;
    private final Function<RowData, Integer> bucketCalculator;
    private final Map<BinaryRowData, Map<Integer, RecordWriter>> writers;
    private final ExecutorService compactExecutor;

    public FileStoreTestHelper(
            Configuration conf,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            MergeFunction mergeFunction,
            BiFunction<RowData, RowData, BinaryRowData> partitionCalculator,
            Function<RowData, Integer> bucketCalculator) {
        FileStoreOptions options = new FileStoreOptions(conf);
        this.store =
                new FileStoreImpl(
                        options.path().toString(),
                        0,
                        options,
                        WriteMode.CHANGE_LOG,
                        UUID.randomUUID().toString(),
                        partitionType,
                        keyType,
                        valueType,
                        mergeFunction);
        this.partitionCalculator = partitionCalculator;
        this.bucketCalculator = bucketCalculator;
        this.writers = new HashMap<>();
        this.compactExecutor = Executors.newSingleThreadExecutor();
    }

    public void write(ValueKind kind, RowData key, RowData value) throws Exception {
        BinaryRowData partition = partitionCalculator.apply(key, value);
        int bucket = bucketCalculator.apply(key);
        RecordWriter writer =
                writers.compute(partition, (p, m) -> m == null ? new HashMap<>() : m)
                        .compute(
                                bucket,
                                (b, w) -> {
                                    if (w == null) {
                                        FileStoreWriteImpl write = store.newWrite();
                                        return write.createWriter(
                                                partition, bucket, compactExecutor);
                                    } else {
                                        return w;
                                    }
                                });
        writer.write(kind, key, value);
    }

    public void commit() throws Exception {
        ManifestCommittable committable = new ManifestCommittable(UUID.randomUUID().toString());
        for (Map.Entry<BinaryRowData, Map<Integer, RecordWriter>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                RecordWriter writer = entryWithBucket.getValue();
                writer.sync();
                Increment increment = writer.prepareCommit();
                committable.addFileCommittable(
                        entryWithPartition.getKey(), entryWithBucket.getKey(), increment);
                writer.close();
            }
        }
        writers.clear();
        FileStoreCommitImpl commit = store.newCommit();
        commit.commit(committable, Collections.emptyMap());
    }

    public Tuple2<RecordReader<KeyValue>, Long> read(BinaryRowData partition, int bucket)
            throws Exception {
        FileStoreScanImpl scan = store.newScan();
        scan.withPartitionFilter(Collections.singletonList(partition)).withBucket(bucket);
        List<ManifestEntry> files = scan.plan().files();
        FileStoreReadImpl read = store.newRead();
        RecordReader<KeyValue> wrapped =
                read.createReader(
                        partition,
                        bucket,
                        files.stream().map(ManifestEntry::file).collect(Collectors.toList()));
        return Tuple2.of(wrapped, files.stream().mapToLong(e -> e.file().fileSize()).sum());
    }
}
