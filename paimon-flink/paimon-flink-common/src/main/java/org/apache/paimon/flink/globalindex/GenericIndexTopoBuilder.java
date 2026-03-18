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
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.io.CompactIncrement.emptyIncrement;
import static org.apache.paimon.io.DataIncrement.deleteIndexIncrement;
import static org.apache.paimon.io.DataIncrement.indexIncrement;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Builds a Flink topology for creating generic (non-btree) global indexes with parallelism. Each
 * shard becomes an independent Flink task. Files are assigned to shards at file level, so each
 * shard only contains the files whose row ID ranges overlap with its shard range. When a file spans
 * multiple shard boundaries, it is included in each overlapping shard and rows outside the shard's
 * range are filtered during reading.
 */
public class GenericIndexTopoBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(GenericIndexTopoBuilder.class);

    public static void buildIndex(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        buildIndex(
                env,
                () -> new GenericGlobalIndexBuilder(table),
                table,
                indexColumn,
                indexType,
                partitionPredicate,
                userOptions);
    }

    /**
     * Builds a generic global index using a {@link GenericGlobalIndexBuilder} supplier.
     *
     * @param env Flink execution environment
     * @param indexBuilderSupplier supplier for the index builder (encapsulates scan strategy)
     * @param table the target table
     * @param indexColumn the column to build index on
     * @param indexType the index type identifier (e.g. "lumina-vector-ann")
     * @param partitionPredicate optional partition filter
     * @param userOptions user-provided options (merged with table options)
     */
    public static void buildIndex(
            StreamExecutionEnvironment env,
            Supplier<GenericGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        GenericGlobalIndexBuilder indexBuilder = indexBuilderSupplier.get();
        if (partitionPredicate != null) {
            indexBuilder.withPartitionPredicate(partitionPredicate);
        }

        List<ManifestEntry> entries = indexBuilder.scan();
        List<IndexManifestEntry> deletedIndexEntries = indexBuilder.deletedIndexEntries();
        long totalRowCount = entries.stream().mapToLong(e -> e.file().rowCount()).sum();
        LOG.info(
                "Scanned {} files ({} rows) across {} partitions for {} index on column '{}'.",
                entries.size(),
                totalRowCount,
                entries.stream().map(ManifestEntry::partition).distinct().count(),
                indexType,
                indexColumn);

        RowType rowType = table.rowType();
        DataField indexField = rowType.getField(indexColumn);
        // Project indexColumn + _ROW_ID so we can read the actual row ID from data
        List<String> readColumns = new ArrayList<>();
        readColumns.add(indexColumn);
        readColumns.add(SpecialFields.ROW_ID.name());
        RowType projectedRowType = SpecialFields.rowTypeWithRowId(rowType).project(readColumns);

        Options mergedOptions = new Options(table.options(), userOptions.toMap());

        long rowsPerShard = mergedOptions.get(CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD);
        checkArgument(
                rowsPerShard > 0,
                "Option 'global-index.row-count-per-shard' must be greater than 0.");

        int maxShard = mergedOptions.get(CoreOptions.GLOBAL_INDEX_BUILD_MAX_SHARD);
        checkArgument(
                maxShard > 0, "Option 'global-index.build.max-shard' must be greater than 0.");
        rowsPerShard =
                GlobalIndexBuilderUtils.adjustRowsPerShard(rowsPerShard, totalRowCount, maxShard);

        // Compute shard tasks at file level from the provided entries
        List<ShardTask> shardTasks = computeShardTasks(table, entries, rowsPerShard);
        if (shardTasks.isEmpty()) {
            LOG.info("No shard tasks generated, nothing to index.");
            return;
        }
        LOG.info("Generated {} shard tasks with rowsPerShard={}.", shardTasks.size(), rowsPerShard);

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

        if (!deletedIndexEntries.isEmpty()) {
            List<Committable> deleteCommittables = createDeleteCommittables(deletedIndexEntries);
            DataStream<Committable> deletes =
                    env.fromData(
                                    new CommittableTypeInfo(),
                                    deleteCommittables.toArray(new Committable[0]))
                            .name("Index Delete Source")
                            .setParallelism(1);
            built = built.union(deletes);
        }

        commit(table, indexType, built);

        env.execute("Create " + indexType + " global index for table: " + table.name());
    }

    /**
     * Compute shard tasks at file level from the given manifest entries. Each shard only contains
     * the files whose row ID ranges overlap with its shard range. A file spanning multiple shard
     * boundaries is included in each overlapping shard.
     */
    static List<ShardTask> computeShardTasks(
            FileStoreTable table, List<ManifestEntry> entries, long rowsPerShard) {
        // Group by partition (bucket is always 0 for unaware-bucket tables)
        Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                entries.stream().collect(Collectors.groupingBy(ManifestEntry::partition));

        List<ShardTask> tasks = new ArrayList<>();

        for (Map.Entry<BinaryRow, List<ManifestEntry>> partitionEntry :
                entriesByPartition.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            String partBucketPath = table.store().pathFactory().bucketPath(partition, 0).toString();

            // Assign files to shards by row ID range
            Map<Long, List<DataFileMeta>> filesByShard = new LinkedHashMap<>();
            for (ManifestEntry entry : partitionEntry.getValue()) {
                DataFileMeta file = entry.file();
                if (file.firstRowId() == null) {
                    LOG.warn(
                            "Skipping file '{}' in partition {} because it has no row ID. "
                                    + "This file will NOT be indexed. "
                                    + "Ensure row tracking is enabled for the table.",
                            file.fileName(),
                            partition);
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
                                        partBucketPath));
                        currentGroup = new ArrayList<>();
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    }
                }
                if (!currentGroup.isEmpty()) {
                    tasks.add(
                            createShardTask(
                                    currentGroup, shardStart, shardEnd, partition, partBucketPath));
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
            String bucketPath) {
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

        return new ShardTask(dataSplit, new Range(rangeFrom, rangeTo));
    }

    private static List<Committable> createDeleteCommittables(
            List<IndexManifestEntry> deletedEntries) {
        List<Committable> committables = new ArrayList<>();
        for (IndexManifestEntry entry : deletedEntries) {
            CommitMessage msg =
                    new CommitMessageImpl(
                            entry.partition(),
                            entry.bucket(),
                            null,
                            deleteIndexIncrement(Collections.singletonList(entry.indexFile())),
                            emptyIncrement());
            committables.add(new Committable(BatchWriteBuilder.COMMIT_IDENTIFIER, msg));
        }
        return committables;
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

        final DataSplit split;
        final Range shardRange;

        ShardTask(DataSplit split, Range shardRange) {
            this.split = split;
            this.shardRange = shardRange;
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

        private transient TableRead tableRead;
        private transient InternalRow.FieldGetter indexFieldGetter;
        private transient int rowIdFieldIndex;

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
        public void open() throws Exception {
            super.open();
            this.tableRead = readBuilder.newRead();
            this.indexFieldGetter =
                    InternalRow.createFieldGetter(
                            indexField.type(), projectedRowType.getFieldIndex(indexField.name()));
            this.rowIdFieldIndex = projectedRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        }

        @Override
        public void processElement(StreamRecord<ShardTask> element) throws Exception {
            ShardTask task = element.getValue();
            BinaryRow partition = task.split.partition();

            LOG.info(
                    "Building {} index for partition={}, shardRange=[{}, {}], files={}.",
                    indexType,
                    partition,
                    task.shardRange.from,
                    task.shardRange.to,
                    task.split.dataFiles().size());
            long startTime = System.currentTimeMillis();

            GlobalIndexSingletonWriter indexWriter =
                    (GlobalIndexSingletonWriter)
                            createIndexWriter(table, indexType, indexField, mergedOptions);

            try {
                long rowsWritten = 0;
                long lastRowId = Long.MIN_VALUE;
                try (RecordReader<InternalRow> reader = tableRead.createReader(task.split);
                        CloseableIterator<InternalRow> iter = reader.toCloseableIterator()) {
                    while (iter.hasNext()) {
                        InternalRow row = iter.next();
                        long currentRowId = row.getLong(rowIdFieldIndex);

                        if (currentRowId < lastRowId) {
                            throw new IllegalStateException(
                                    String.format(
                                            "Row IDs are not monotonically increasing: "
                                                    + "previous=%d, current=%d in shard [%d, %d]. "
                                                    + "This may indicate corrupted data files.",
                                            lastRowId,
                                            currentRowId,
                                            task.shardRange.from,
                                            task.shardRange.to));
                        }
                        lastRowId = currentRowId;

                        if (currentRowId > task.shardRange.to) {
                            break;
                        }
                        // Only write rows within this shard's range
                        if (currentRowId >= task.shardRange.from) {
                            indexWriter.write(indexFieldGetter.getFieldOrNull(row));
                            rowsWritten++;
                        }
                    }
                }

                List<ResultEntry> resultEntries = indexWriter.finish();
                long elapsed = System.currentTimeMillis() - startTime;
                LOG.info(
                        "Finished shard [{}, {}]: wrote {} rows, "
                                + "produced {} result entries in {} ms.",
                        task.shardRange.from,
                        task.shardRange.to,
                        rowsWritten,
                        resultEntries.size(),
                        elapsed);

                if (rowsWritten == 0) {
                    LOG.warn(
                            "Shard [{}, {}] produced 0 rows, skipping index flush.",
                            task.shardRange.from,
                            task.shardRange.to);
                    return;
                }

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
                throw e;
            } finally {
                closeWriterQuietly(indexWriter);
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
