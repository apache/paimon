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

package org.apache.paimon.spark.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Default topology builder. */
public class DefaultGlobalIndexTopoBuilder implements GlobalIndexTopologyBuilder {

    @Override
    public String identifier() {
        return "default";
    }

    @Override
    public List<CommitMessage> buildIndex(
            SparkSession spark,
            DataSourceV2Relation relation,
            PartitionPredicate partitionPredicate,
            FileStoreTable table,
            String indexType,
            RowType readType,
            DataField indexField,
            Options options)
            throws IOException {
        Options tableOptions = table.coreOptions().toConfiguration();
        long rowsPerShard =
                tableOptions
                        .getOptional(GLOBAL_INDEX_ROW_COUNT_PER_SHARD)
                        .orElse(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.defaultValue());
        checkArgument(
                rowsPerShard > 0,
                "Option 'global-index.row-count-per-shard' must be greater than 0.");

        // generate splits for each partition && shard
        Map<BinaryRow, List<IndexedSplit>> splits = split(table, partitionPredicate, rowsPerShard);

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        List<Pair<byte[], byte[]>> taskList = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<IndexedSplit>> entry : splits.entrySet()) {
            BinaryRow partition = entry.getKey();
            List<IndexedSplit> partitions = entry.getValue();

            for (IndexedSplit indexedSplit : partitions) {
                checkArgument(
                        indexedSplit.rowRanges().size() == 1,
                        "Each IndexedSplit should contain exactly one row range.");
                DefaultGlobalIndexBuilder builder =
                        new DefaultGlobalIndexBuilder(
                                table,
                                partition,
                                readType,
                                indexField,
                                indexType,
                                indexedSplit.rowRanges().get(0),
                                options);
                byte[] builderBytes = InstantiationUtil.serializeObject(builder);
                byte[] splitBytes = InstantiationUtil.serializeObject(indexedSplit);
                taskList.add(Pair.of(builderBytes, splitBytes));
            }
        }

        List<byte[]> commitMessageBytes =
                javaSparkContext
                        .parallelize(taskList, taskList.size())
                        .map(DefaultGlobalIndexTopoBuilder::buildIndex)
                        .collect();
        return CommitMessageSerializer.deserializeAll(commitMessageBytes);
    }

    private static byte[] buildIndex(Pair<byte[], byte[]> builderAndSplits) throws Exception {
        ClassLoader classLoader = DefaultGlobalIndexBuilder.class.getClassLoader();
        DefaultGlobalIndexBuilder indexBuilder =
                InstantiationUtil.deserializeObject(builderAndSplits.getLeft(), classLoader);
        byte[] dataSplitBytes = builderAndSplits.getRight();
        IndexedSplit split = InstantiationUtil.deserializeObject(dataSplitBytes, classLoader);
        ReadBuilder builder = indexBuilder.table().newReadBuilder();
        builder.withReadType(indexBuilder.readType());

        try (RecordReader<InternalRow> recordReader = builder.newRead().createReader(split);
                CloseableIterator<InternalRow> data = recordReader.toCloseableIterator()) {
            CommitMessage commitMessage = indexBuilder.build(data);
            return new CommitMessageSerializer().serialize(commitMessage);
        }
    }

    private static Map<BinaryRow, List<IndexedSplit>> split(
            FileStoreTable table, PartitionPredicate partitions, long rowsPerShard) {
        FileStorePathFactory pathFactory = table.store().pathFactory();
        // Get all manifest entries from the table scan
        List<ManifestEntry> entries =
                table.store().newScan().withPartitionFilter(partitions).plan().files();

        // Group manifest entries by partition
        Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                entries.stream().collect(Collectors.groupingBy(ManifestEntry::partition));

        return groupFilesIntoShardsByPartition(
                entriesByPartition, rowsPerShard, pathFactory::bucketPath);
    }

    /**
     * Groups files into shards by partition. This method is extracted from split() to make it more
     * testable.
     *
     * @param entriesByPartition manifest entries grouped by partition
     * @param rowsPerShard number of rows per shard
     * @param pathFactory path factory for creating bucket paths
     * @return map of partition to shard splits
     */
    public static Map<BinaryRow, List<IndexedSplit>> groupFilesIntoShardsByPartition(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, Path> pathFactory) {
        Map<BinaryRow, List<IndexedSplit>> result = new HashMap<>();

        for (Map.Entry<BinaryRow, List<ManifestEntry>> partitionEntry :
                entriesByPartition.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            List<ManifestEntry> partitionEntries = partitionEntry.getValue();

            // Group files into shards - a file may belong to multiple shards
            Map<Long, List<DataFileMeta>> filesByShard = new LinkedHashMap<>();

            for (ManifestEntry entry : partitionEntries) {
                DataFileMeta file = entry.file();
                Long firstRowId = file.firstRowId();
                if (firstRowId == null) {
                    continue; // Skip files without row tracking
                }

                // Calculate the row ID range this file covers
                Range fileRange = file.nonNullRowIdRange();

                // Calculate which shards this file overlaps with
                long startShardId = fileRange.from / rowsPerShard;
                long endShardId = fileRange.to / rowsPerShard;

                // Add this file to all shards it overlaps with
                for (long shardId = startShardId; shardId <= endShardId; shardId++) {
                    long shardStartRowId = shardId * rowsPerShard;
                    filesByShard.computeIfAbsent(shardStartRowId, k -> new ArrayList<>()).add(file);
                }
            }

            // Create DataSplit for each shard with exact ranges
            List<IndexedSplit> shardSplits = new ArrayList<>();
            for (Map.Entry<Long, List<DataFileMeta>> shardEntry : filesByShard.entrySet()) {
                long shardStart = shardEntry.getKey();
                long shardEnd = shardStart + rowsPerShard - 1;
                List<DataFileMeta> shardFiles = shardEntry.getValue();

                if (shardFiles.isEmpty()) {
                    continue;
                }

                // Sort files by firstRowId to ensure sequential order
                shardFiles.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));

                // Group contiguous files and create separate DataSplits for each group
                List<DataFileMeta> currentGroup = new ArrayList<>();
                long currentGroupEnd = -1;

                for (DataFileMeta file : shardFiles) {
                    long fileStart = file.nonNullFirstRowId();
                    long fileEnd = fileStart + file.rowCount() - 1;

                    if (currentGroup.isEmpty()) {
                        // Start a new group
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    } else if (fileStart <= currentGroupEnd + 1) {
                        // File is contiguous with current group (adjacent or overlapping)
                        currentGroup.add(file);
                        currentGroupEnd = Math.max(currentGroupEnd, fileEnd);
                    } else {
                        // Gap detected, finalize current group and start a new one
                        createDataSplitForGroup(
                                currentGroup,
                                shardStart,
                                shardEnd,
                                partition,
                                pathFactory,
                                shardSplits);
                        currentGroup = new ArrayList<>();
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    }
                }

                // Don't forget to process the last group
                if (!currentGroup.isEmpty()) {
                    createDataSplitForGroup(
                            currentGroup,
                            shardStart,
                            shardEnd,
                            partition,
                            pathFactory,
                            shardSplits);
                }
            }

            if (!shardSplits.isEmpty()) {
                result.put(partition, shardSplits);
            }
        }

        return result;
    }

    private static void createDataSplitForGroup(
            List<DataFileMeta> files,
            long shardStart,
            long shardEnd,
            BinaryRow partition,
            BiFunction<BinaryRow, Integer, Path> pathFactory,
            List<IndexedSplit> shardSplits) {
        // Calculate the actual row range covered by the files
        long groupMinRowId = files.get(0).nonNullFirstRowId();
        long groupMaxRowId =
                files.stream().mapToLong(f -> f.nonNullRowIdRange().to).max().getAsLong();

        // Clamp to shard boundaries
        // Range.from >= shardStart, Range.to <= shardEnd
        long rangeFrom = Math.max(groupMinRowId, shardStart);
        long rangeTo = Math.min(groupMaxRowId, shardEnd);

        Range range = new Range(rangeFrom, rangeTo);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(files)
                        .withBucketPath(pathFactory.apply(partition, 0).toString())
                        .rawConvertible(false)
                        .build();

        shardSplits.add(new IndexedSplit(dataSplit, Collections.singletonList(range), null));
    }
}
