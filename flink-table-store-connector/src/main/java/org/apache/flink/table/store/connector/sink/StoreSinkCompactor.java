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
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.compact.CompactStrategy;
import org.apache.flink.table.store.file.mergetree.compact.CompactUnit;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.compact.ManualTriggeredCompaction;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.KeyComparatorSupplier;
import org.apache.flink.table.store.file.writer.RecordWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** A dedicated {@link SinkWriter} for manual triggered compaction. */
public class StoreSinkCompactor<WriterStateT> extends StoreSinkWriterBase<WriterStateT> {

    private static final Logger LOG = LoggerFactory.getLogger(StoreSinkCompactor.class);

    private final int subTaskId;
    private final int numOfParallelInstances;

    private final FileStore fileStore;
    private final int numLevels;
    private final long targetFileSize;
    private final Map<BinaryRowData, Map<Integer, List<CompactUnit>>> partitionedUnits;
    private final Map<String, String> partitionSpec;

    public StoreSinkCompactor(
            int subTaskId,
            int numOfParallelInstances,
            FileStore fileStore,
            int numLevels,
            long targetFileSize,
            Map<String, String> partitionSpec) {
        this.subTaskId = subTaskId;
        this.numOfParallelInstances = numOfParallelInstances;
        this.fileStore = fileStore;
        this.numLevels = numLevels;
        this.targetFileSize = targetFileSize;
        this.partitionSpec = partitionSpec;
        this.partitionedUnits = new HashMap<>();
    }

    @Override
    public void flush(boolean endOfInput) {
        if (endOfInput) {
            FileStoreScan.Plan plan =
                    fileStore
                            .newScan()
                            .withPartitionFilter(
                                    PredicateConverter.CONVERTER.fromMap(
                                            partitionSpec, fileStore.partitionType()))
                            .plan();
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupBy = new HashMap<>();
            plan.groupByPartFiles()
                    .forEach(
                            (partition, bucketEntry) ->
                                    bucketEntry.forEach(
                                            (bucket, meta) -> {
                                                if (select(partition, bucket)) {
                                                    if (LOG.isDebugEnabled()) {
                                                        LOG.info(
                                                                "Assign partition {}, bucket {} to subtask {}",
                                                                FileStorePathFactory
                                                                        .getPartitionComputer(
                                                                                fileStore
                                                                                        .partitionType(),
                                                                                FileSystemConnectorOptions
                                                                                        .PARTITION_DEFAULT_NAME
                                                                                        .defaultValue())
                                                                        .generatePartValues(
                                                                                partition),
                                                                bucket,
                                                                subTaskId);
                                                    }
                                                    groupBy.computeIfAbsent(
                                                                    partition, k -> new HashMap<>())
                                                            .computeIfAbsent(
                                                                    bucket, k -> new ArrayList<>())
                                                            .addAll(meta);
                                                }
                                            }));

            pickManifest(groupBy, new KeyComparatorSupplier(fileStore.keyType()).get());
            for (Map.Entry<BinaryRowData, Map<Integer, List<CompactUnit>>> partEntry :
                    partitionedUnits.entrySet()) {
                BinaryRowData partition = partEntry.getKey();
                for (Map.Entry<Integer, List<CompactUnit>> bucketEntry :
                        partEntry.getValue().entrySet()) {
                    int bucket = bucketEntry.getKey();
                    RecordWriter writer = getWriter(partition, bucket);
                    try {
                        writer.flush();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    protected RecordWriter createWriter(BinaryRowData partition, int bucket) {
        BinaryRowData copied = partition.copy();
        return fileStore
                .newWrite()
                .createCompactWriter(copied, bucket, partitionedUnits.get(copied).get(bucket));
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        // nothing to write
    }

    @Override
    public void close() throws Exception {
        partitionedUnits.clear();
        super.close();
    }

    @VisibleForTesting
    boolean select(BinaryRowData partition, int bucket) {
        return subTaskId == Math.abs(Objects.hash(partition, bucket) % numOfParallelInstances);
    }

    @VisibleForTesting
    void pickManifest(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupBy,
            Comparator<RowData> keyComparator) {
        CompactStrategy<SortedRun> strategy = new ManualTriggeredCompaction(targetFileSize);
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> partEntry :
                groupBy.entrySet()) {
            Map<Integer, List<CompactUnit>> partUnit = new HashMap<>();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                List<List<SortedRun>> sections =
                        new IntervalPartition(bucketEntry.getValue(), keyComparator).partition();
                List<CompactUnit> bucketUnit = new ArrayList<>();
                sections.forEach(
                        section -> strategy.pick(numLevels, section).ifPresent(bucketUnit::add));
                if (bucketUnit.size() > 0) {
                    partUnit.put(bucketEntry.getKey(), bucketUnit);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Pick these files (name, level, size) for partition {}, bucket {} to compact: {}",
                            FileStorePathFactory.getPartitionComputer(
                                    fileStore.partitionType(),
                                    FileSystemConnectorOptions.PARTITION_DEFAULT_NAME
                                            .defaultValue()),
                            bucketEntry.getKey(),
                            bucketUnit.stream()
                                    .map(CompactUnit::files)
                                    .flatMap(Collection::stream)
                                    .map(
                                            file ->
                                                    String.join(
                                                            ",",
                                                            file.fileName(),
                                                            String.valueOf(file.level()),
                                                            String.valueOf(file.fileSize())))
                                    .collect(Collectors.joining(";")));
                }
            }
            if (partUnit.size() > 0) {
                partitionedUnits.put(partEntry.getKey(), partUnit);
            }
        }
    }

    @VisibleForTesting
    Map<BinaryRowData, Map<Integer, List<CompactUnit>>> getPartitionedUnits() {
        return partitionedUnits;
    }
}
