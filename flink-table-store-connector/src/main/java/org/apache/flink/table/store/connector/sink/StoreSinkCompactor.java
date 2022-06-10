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
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.connector.StatefulPrecommittingSinkWriter;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A dedicated {@link SinkWriter} for manual triggered compaction. */
public class StoreSinkCompactor implements StatefulPrecommittingSinkWriter<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkCompactor.class);

    private final int subTaskId;
    private final int numOfParallelInstances;

    private final FileStore fileStore;
    private final Map<String, String> partitionSpec;
    private final ExecutorService compactExecutor;

    public StoreSinkCompactor(
            int subTaskId,
            int numOfParallelInstances,
            FileStore fileStore,
            Map<String, String> partitionSpec) {
        this.subTaskId = subTaskId;
        this.numOfParallelInstances = numOfParallelInstances;
        this.fileStore = fileStore;
        this.partitionSpec = partitionSpec;
        this.compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                String.format("compaction-subtask-%d", subTaskId)));
    }

    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        // nothing to write
    }

    @Override
    public void close() throws Exception {
        compactExecutor.shutdownNow();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Committable> prepareCommit() throws IOException {
        List<Committable> committables = new ArrayList<>();

        FileStoreScan.Plan plan =
                fileStore
                        .newScan()
                        .withPartitionFilter(
                                PredicateConverter.CONVERTER.fromMap(
                                        partitionSpec, fileStore.partitionType()))
                        .plan();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> partEntry :
                plan.groupByPartFiles().entrySet()) {
            BinaryRowData partition = partEntry.getKey();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> restoredFiles = bucketEntry.getValue();
                if (select(partition, bucket)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Assign partition {}, bucket {} to subtask {}",
                                FileStorePathFactory.getPartitionComputer(
                                                fileStore.partitionType(),
                                                FileSystemConnectorOptions.PARTITION_DEFAULT_NAME
                                                        .defaultValue())
                                        .generatePartValues(partition),
                                bucket,
                                subTaskId);
                    }
                    RecordWriter writer =
                            fileStore
                                    .newWrite()
                                    .createCompactWriter(
                                            partition.copy(),
                                            bucket,
                                            compactExecutor,
                                            restoredFiles);
                    FileCommittable committable;
                    try {
                        committable =
                                new FileCommittable(
                                        partition, bucketEntry.getKey(), writer.prepareCommit());
                        committables.add(new Committable(Committable.Kind.FILE, committable));
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                }
            }
        }
        return committables;
    }

    private boolean select(BinaryRowData partition, int bucket) {
        return subTaskId == Math.abs(Objects.hash(partition, bucket) % numOfParallelInstances);
    }
}
