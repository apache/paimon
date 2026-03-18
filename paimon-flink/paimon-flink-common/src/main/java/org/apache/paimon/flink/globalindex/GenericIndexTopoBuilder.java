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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.io.CompactIncrement.emptyIncrement;
import static org.apache.paimon.io.DataIncrement.indexIncrement;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Builds a Flink topology for creating generic (non-btree) global indexes with parallelism. Each
 * shard becomes an independent Flink task. Files are assigned to shards at file level (like Spark),
 * so each shard only contains the files whose row ID ranges overlap with its shard range. When a
 * file spans multiple shard boundaries, it is included in each overlapping shard and rows outside
 * the shard's range are filtered during reading.
 */
public class GenericIndexTopoBuilder {

    /**
     * Builds a generic global index from the given manifest entries. The entries can come from a
     * full scan or an incremental scan (only non-indexed row ranges), allowing callers to implement
     * custom scan strategies.
     *
     * @param env Flink execution environment
     * @param table the target table
     * @param indexColumn the column to build index on
     * @param indexType the index type identifier (e.g. "lumina-vector-ann")
     * @param entries manifest entries to build index from (full or incremental)
     * @param mergedOptions merged table + user options
     */
    public static void buildIndex(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            List<ManifestEntry> entries,
            Options mergedOptions)
            throws Exception {
        RowType rowType = table.rowType();
        DataField indexField = rowType.getField(indexColumn);
        RowType projectedRowType = rowType.project(Collections.singletonList(indexColumn));

        // Validate table configuration
        checkArgument(
                table.coreOptions().bucket() == -1,
                "Generic global index only supports unaware-bucket tables (bucket = -1), "
                        + "but table '%s' has bucket = %d.",
                table.name(),
                table.coreOptions().bucket());
        checkArgument(
                !table.coreOptions().deletionVectorsEnabled(),
                "Generic global index does not support tables with deletion vectors enabled. "
                        + "Table '%s' has 'deletion-vectors.enabled' = true, which may cause "
                        + "deleted rows to be indexed.",
                table.name());

        long rowsPerShard =
                mergedOptions
                        .getOptional(CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD)
                        .orElse(CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD.defaultValue());
        checkArgument(
                rowsPerShard > 0,
                "Option 'global-index.row-count-per-shard' must be greater than 0.");

        // Compute shard tasks at file level from the provided entries
        List<ShardTask> shardTasks = computeShardTasks(table, entries, rowsPerShard);
        if (shardTasks.isEmpty()) {
            return;
        }

        int maxParallelism =
                mergedOptions
                        .getOptional(CoreOptions.GLOBAL_INDEX_BUILD_MAX_PARALLELISM)
                        .orElse(CoreOptions.GLOBAL_INDEX_BUILD_MAX_PARALLELISM.defaultValue());
        checkArgument(
                maxParallelism > 0,
                "Option 'global-index.build.max-parallelism' must be greater than 0.");
        int parallelism = Math.min(shardTasks.size(), maxParallelism);

        // Build Flink topology
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(projectedRowType);

        DataStream<ShardTask> source =
                env.fromData(
                                new JavaTypeInfo<>(ShardTask.class),
                                shardTasks.toArray(new ShardTask[0]))
                        .name("Generic Index Source")
                        .setParallelism(1);

        DataStream<Committable> built =
                source.transform(
                                "build-" + indexType + "-index",
                                new CommittableTypeInfo(),
                                new BuildIndexOperator(
                                        readBuilder,
                                        table,
                                        indexType,
                                        indexField,
                                        projectedRowType,
                                        mergedOptions))
                        .setParallelism(parallelism);

        commit(table, indexType, built);

        env.execute("Create " + indexType + " global index for table: " + table.name());
    }

    /**
     * Compute shard tasks at file level from the given manifest entries. Each shard only contains
     * the files whose row ID ranges overlap with its shard range. A file spanning multiple shard
     * boundaries is included in each overlapping shard.
     */
    static List<ShardTask> computeShardTasks(
            FileStoreTable table, List<ManifestEntry> entries, long rowsPerShard)
            throws IOException {
        int partitionFieldSize = table.partitionKeys().size();
        BinaryRowSerializer serializer = new BinaryRowSerializer(partitionFieldSize);

        // Group by partition
        Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                entries.stream().collect(Collectors.groupingBy(ManifestEntry::partition));

        List<ShardTask> tasks = new ArrayList<>();

        for (Map.Entry<BinaryRow, List<ManifestEntry>> partitionEntry :
                entriesByPartition.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            byte[] partitionBytes = serializer.serializeToBytes(partition);
            String partBucketPath = table.store().pathFactory().bucketPath(partition, 0).toString();

            // Assign files to shards by row ID range
            Map<Long, List<DataFileMeta>> filesByShard = new LinkedHashMap<>();
            for (ManifestEntry entry : partitionEntry.getValue()) {
                DataFileMeta file = entry.file();
                if (file.firstRowId() == null) {
                    continue;
                }
                Range fileRange = file.nonNullRowIdRange();
                long startShardId = fileRange.from / rowsPerShard;
                long endShardId = fileRange.to / rowsPerShard;

                for (long shardId = startShardId; shardId <= endShardId; shardId++) {
                    long shardStartRowId = shardId * rowsPerShard;
                    filesByShard.computeIfAbsent(shardStartRowId, k -> new ArrayList<>()).add(file);
                }
            }

            // Create ShardTask for each shard group
            for (Map.Entry<Long, List<DataFileMeta>> shardEntry : filesByShard.entrySet()) {
                long shardStart = shardEntry.getKey();
                long shardEnd = shardStart + rowsPerShard - 1;
                List<DataFileMeta> shardFiles = shardEntry.getValue();
                if (shardFiles.isEmpty()) {
                    continue;
                }

                shardFiles.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));

                // Group contiguous files; gaps produce separate tasks
                List<DataFileMeta> currentGroup = new ArrayList<>();
                long currentGroupEnd = -1;

                for (DataFileMeta file : shardFiles) {
                    long fileStart = file.nonNullFirstRowId();
                    long fileEnd = fileStart + file.rowCount() - 1;

                    if (currentGroup.isEmpty()) {
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    } else if (fileStart <= currentGroupEnd + 1) {
                        currentGroup.add(file);
                        currentGroupEnd = Math.max(currentGroupEnd, fileEnd);
                    } else {
                        tasks.add(
                                createShardTask(
                                        currentGroup,
                                        shardStart,
                                        shardEnd,
                                        partition,
                                        partBucketPath,
                                        partitionBytes,
                                        partitionFieldSize));
                        currentGroup = new ArrayList<>();
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    }
                }
                if (!currentGroup.isEmpty()) {
                    tasks.add(
                            createShardTask(
                                    currentGroup,
                                    shardStart,
                                    shardEnd,
                                    partition,
                                    partBucketPath,
                                    partitionBytes,
                                    partitionFieldSize));
                }
            }
        }
        return tasks;
    }

    private static ShardTask createShardTask(
            List<DataFileMeta> files,
            long shardStart,
            long shardEnd,
            BinaryRow partition,
            String bucketPath,
            byte[] partitionBytes,
            int partitionFieldSize) {
        long groupMinRowId = files.get(0).nonNullFirstRowId();
        long groupMaxRowId =
                files.stream().mapToLong(f -> f.nonNullRowIdRange().to).max().getAsLong();

        // Clamp to shard boundaries
        long rangeFrom = Math.max(groupMinRowId, shardStart);
        long rangeTo = Math.min(groupMaxRowId, shardEnd);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(files)
                        .withBucketPath(bucketPath)
                        .rawConvertible(false)
                        .build();

        return new ShardTask(
                dataSplit, new Range(rangeFrom, rangeTo), partitionBytes, partitionFieldSize);
    }

    private static void commit(
            FileStoreTable table, String indexType, DataStream<Committable> written) {
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        "GenericIndexCommitter-" + indexType + "-" + UUID.randomUUID(),
                        context ->
                                new StoreCommitter(
                                        table, table.newCommit(context.commitUser()), context),
                        new NoopCommittableStateManager());

        written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), committerOperator)
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    /** Serializable descriptor for one shard's work. Each shard has its own DataSplit and Range. */
    static class ShardTask implements Serializable {
        private static final long serialVersionUID = 1L;

        private final DataSplit split;
        private final Range shardRange;
        private final byte[] partitionBytes;
        private final int partitionFieldSize;

        ShardTask(
                DataSplit split, Range shardRange, byte[] partitionBytes, int partitionFieldSize) {
            this.split = split;
            this.shardRange = shardRange;
            this.partitionBytes = partitionBytes;
            this.partitionFieldSize = partitionFieldSize;
        }
    }

    /**
     * Operator that receives a {@link ShardTask}, reads data from its split, builds the index, and
     * emits a {@link Committable}. Each shard's split contains only the files relevant to this
     * shard, so no redundant I/O occurs.
     */
    private static class BuildIndexOperator
            extends BoundedOneInputOperator<ShardTask, Committable> {

        private static final long serialVersionUID = 1L;

        private final ReadBuilder readBuilder;
        private final FileStoreTable table;
        private final String indexType;
        private final DataField indexField;
        private final RowType projectedRowType;
        private final Options mergedOptions;

        BuildIndexOperator(
                ReadBuilder readBuilder,
                FileStoreTable table,
                String indexType,
                DataField indexField,
                RowType projectedRowType,
                Options mergedOptions) {
            this.readBuilder = readBuilder;
            this.table = table;
            this.indexType = indexType;
            this.indexField = indexField;
            this.projectedRowType = projectedRowType;
            this.mergedOptions = mergedOptions;
        }

        @Override
        public void processElement(StreamRecord<ShardTask> element) throws Exception {
            ShardTask task = element.getValue();
            BinaryRowSerializer serializer = new BinaryRowSerializer(task.partitionFieldSize);
            BinaryRow partition = serializer.deserializeFromBytes(task.partitionBytes);

            InternalRow.FieldGetter getter =
                    InternalRow.createFieldGetter(
                            indexField.type(), projectedRowType.getFieldIndex(indexField.name()));

            GlobalIndexSingletonWriter indexWriter =
                    (GlobalIndexSingletonWriter)
                            createIndexWriter(table, indexType, indexField, mergedOptions);

            try {
                // Compute the absolute row ID of the first row in this split
                long splitFirstRowId =
                        task.split.dataFiles().stream()
                                .mapToLong(DataFileMeta::nonNullFirstRowId)
                                .min()
                                .orElse(0);

                try (RecordReader<InternalRow> reader =
                                readBuilder.newRead().createReader(task.split);
                        CloseableIterator<InternalRow> iter = reader.toCloseableIterator()) {
                    long currentRowId = splitFirstRowId;
                    while (iter.hasNext()) {
                        InternalRow row = iter.next();
                        // Only write rows within this shard's range
                        if (currentRowId >= task.shardRange.from
                                && currentRowId <= task.shardRange.to) {
                            indexWriter.write(getter.getFieldOrNull(row));
                        }
                        currentRowId++;
                        if (currentRowId > task.shardRange.to) {
                            break;
                        }
                    }
                }

                List<ResultEntry> resultEntries = indexWriter.finish();
                CommitMessage commitMessage =
                        flushIndex(
                                table,
                                partition,
                                task.shardRange,
                                indexField,
                                indexType,
                                resultEntries);
                output.collect(
                        new StreamRecord<>(
                                new Committable(
                                        BatchWriteBuilder.COMMIT_IDENTIFIER, commitMessage)));
            } catch (Exception e) {
                closeWriterQuietly(indexWriter);
                throw e;
            }
        }

        @Override
        public void endInput() {
            // All work is done in processElement
        }
    }

    private static CommitMessage flushIndex(
            FileStoreTable table,
            BinaryRow partition,
            Range rowRange,
            DataField indexField,
            String indexType,
            List<ResultEntry> resultEntries)
            throws IOException {
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        indexType,
                        resultEntries);
        return new CommitMessageImpl(
                partition, 0, null, indexIncrement(indexFileMetas), emptyIncrement());
    }

    private static void closeWriterQuietly(GlobalIndexSingletonWriter writer) {
        if (writer instanceof Closeable) {
            try {
                ((Closeable) writer).close();
            } catch (IOException ignored) {
            }
        }
    }
}
