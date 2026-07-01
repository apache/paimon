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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createIndexWriter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.createShardIndexedSplits;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.filterEntriesBefore;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.findMinNonIndexableRowId;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.rowRangesAfter;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.unindexedRowRanges;
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
    public static final long NO_MAX_INDEXED_ROW_ID = -1L;

    public static void buildIndexAndExecute(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        buildIndexAndExecuteInternal(
                env,
                table,
                indexColumn,
                Collections.emptyList(),
                indexType,
                partitionPredicate,
                userOptions,
                NO_MAX_INDEXED_ROW_ID,
                true);
    }

    public static void buildIndexAndExecute(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions,
            long maxIndexedRowId)
            throws Exception {
        buildIndexAndExecuteInternal(
                env,
                table,
                indexColumn,
                Collections.emptyList(),
                indexType,
                partitionPredicate,
                userOptions,
                maxIndexedRowId,
                false);
    }

    public static void buildIndexAndExecute(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            List<String> extraColumns,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        buildIndexAndExecuteInternal(
                env,
                table,
                indexColumn,
                extraColumns,
                indexType,
                partitionPredicate,
                userOptions,
                NO_MAX_INDEXED_ROW_ID,
                true);
    }

    public static void buildIndexAndExecute(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            List<String> extraColumns,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions,
            long maxIndexedRowId)
            throws Exception {
        buildIndexAndExecuteInternal(
                env,
                table,
                indexColumn,
                extraColumns,
                indexType,
                partitionPredicate,
                userOptions,
                maxIndexedRowId,
                false);
    }

    private static void buildIndexAndExecuteInternal(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            List<String> extraColumns,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions,
            long maxIndexedRowId,
            boolean autoIncremental)
            throws Exception {
        boolean hasIndexToBuild =
                buildIndexInternal(
                        env,
                        () -> new GenericGlobalIndexBuilder(table),
                        table,
                        indexColumn,
                        extraColumns,
                        indexType,
                        partitionPredicate,
                        userOptions,
                        maxIndexedRowId,
                        autoIncremental);
        if (hasIndexToBuild) {
            env.execute("Create " + indexType + " global index for table: " + table.name());
        } else {
            LOG.info("No index to build, nothing to do.");
        }
    }

    public static boolean buildIndex(
            StreamExecutionEnvironment env,
            Supplier<GenericGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        return buildIndexInternal(
                env,
                indexBuilderSupplier,
                table,
                indexColumn,
                Collections.emptyList(),
                indexType,
                partitionPredicate,
                userOptions,
                NO_MAX_INDEXED_ROW_ID,
                true);
    }

    public static boolean buildIndex(
            StreamExecutionEnvironment env,
            Supplier<GenericGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions,
            long maxIndexedRowId)
            throws Exception {
        return buildIndexInternal(
                env,
                indexBuilderSupplier,
                table,
                indexColumn,
                Collections.emptyList(),
                indexType,
                partitionPredicate,
                userOptions,
                maxIndexedRowId,
                false);
    }

    /**
     * Builds a generic global index topology using a {@link GenericGlobalIndexBuilder} supplier.
     *
     * @return {@code true} if a Flink topology was built and is ready to execute, {@code false} if
     *     there was nothing to index
     */
    public static boolean buildIndex(
            StreamExecutionEnvironment env,
            Supplier<GenericGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            String indexColumn,
            List<String> extraColumns,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions,
            long maxIndexedRowId)
            throws Exception {
        return buildIndexInternal(
                env,
                indexBuilderSupplier,
                table,
                indexColumn,
                extraColumns,
                indexType,
                partitionPredicate,
                userOptions,
                maxIndexedRowId,
                false);
    }

    private static boolean buildIndexInternal(
            StreamExecutionEnvironment env,
            Supplier<GenericGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            String indexColumn,
            List<String> extraColumns,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions,
            long maxIndexedRowId,
            boolean autoIncremental)
            throws Exception {
        GenericGlobalIndexBuilder indexBuilder = indexBuilderSupplier.get();
        if (partitionPredicate != null) {
            indexBuilder.withPartitionPredicate(partitionPredicate);
        }

        List<ManifestEntry> entries = indexBuilder.scan();
        List<IndexManifestEntry> deletedIndexEntries = indexBuilder.deletedIndexEntries();

        return buildTopology(
                env,
                table,
                indexColumn,
                extraColumns,
                indexType,
                userOptions,
                entries,
                deletedIndexEntries,
                partitionPredicate,
                indexBuilder.scanSnapshot(),
                maxIndexedRowId,
                autoIncremental);
    }

    /**
     * Builds the Flink topology for global index creation from pre-scanned entries. Supports both
     * full builds and incremental builds where already indexed row ranges are skipped.
     *
     * @return {@code true} if a Flink topology was built, {@code false} if nothing to index
     */
    private static boolean buildTopology(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            List<String> extraColumns,
            String indexType,
            Options userOptions,
            List<ManifestEntry> entries,
            List<IndexManifestEntry> deletedIndexEntries,
            PartitionPredicate partitionPredicate,
            @Nullable Snapshot scanSnapshot,
            long maxIndexedRowId,
            boolean autoIncremental)
            throws Exception {
        // The primary column followed by the extra columns, in index order.
        List<String> indexColumns = new ArrayList<>(1 + extraColumns.size());
        indexColumns.add(indexColumn);
        indexColumns.addAll(extraColumns);

        long totalRowCount = entries.stream().mapToLong(e -> e.file().rowCount()).sum();
        LOG.info(
                "Scanned {} files ({} rows) across {} partitions for {} index on columns '{}'.",
                entries.size(),
                totalRowCount,
                entries.stream().map(ManifestEntry::partition).distinct().count(),
                indexType,
                indexColumns);

        long minNonIndexableRowId =
                findMinNonIndexableRowId(table.schemaManager(), entries, indexColumns);
        entries = filterEntriesBefore(entries, minNonIndexableRowId);

        RowType rowType = table.rowType();
        DataField indexField = rowType.getField(indexColumn);
        List<DataField> extraFields =
                extraColumns.stream().map(rowType::getField).collect(Collectors.toList());
        List<DataField> indexedFields = new ArrayList<>(1 + extraFields.size());
        indexedFields.add(indexField);
        indexedFields.addAll(extraFields);
        // Project indexColumns + _ROW_ID so we can read the actual row ID from data
        List<String> readColumns = new ArrayList<>(indexColumns);
        readColumns.add(SpecialFields.ROW_ID.name());
        RowType projectedRowType = SpecialFields.rowTypeWithRowId(rowType).project(readColumns);

        Options mergedOptions = new Options(table.options(), userOptions.toMap());

        long rowsPerShard = mergedOptions.get(CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD);
        checkArgument(
                rowsPerShard > 0,
                "Option 'global-index.row-count-per-shard' must be greater than 0.");

        List<Range> rowRangesToBuild = null;
        if (deletedIndexEntries.isEmpty()) {
            if (autoIncremental) {
                Snapshot snapshot =
                        scanSnapshot == null
                                ? table.snapshotManager().latestSnapshot()
                                : scanSnapshot;
                rowRangesToBuild =
                        unindexedRowRanges(
                                table, snapshot, indexType, indexedFields, partitionPredicate);
                LOG.info("Automatically selected unindexed row ranges: {}.", rowRangesToBuild);
            } else if (maxIndexedRowId != NO_MAX_INDEXED_ROW_ID) {
                rowRangesToBuild = rowRangesAfter(maxIndexedRowId);
                LOG.info(
                        "Selected row ranges after maxIndexedRowId={}: {}.",
                        maxIndexedRowId,
                        rowRangesToBuild);
            }
            if (rowRangesToBuild != null && rowRangesToBuild.isEmpty()) {
                LOG.info("No unindexed row ranges found, nothing to index.");
                return false;
            }
        }

        // Compute shard tasks at file level from the provided entries
        List<IndexedSplit> shardTasks =
                computeShardTasks(table, entries, rowsPerShard, rowRangesToBuild);
        if (shardTasks.isEmpty()) {
            LOG.info("No shard tasks generated, nothing to index.");
            return false;
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

        DataStream<IndexedSplit> source =
                StreamExecutionEnvironmentUtils.fromData(
                                env,
                                new JavaTypeInfo<>(IndexedSplit.class),
                                shardTasks.toArray(new IndexedSplit[0]))
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
                                        extraFields,
                                        projectedRowType,
                                        mergedOptions))
                        .setParallelism(parallelism);

        if (!deletedIndexEntries.isEmpty()) {
            List<Committable> deleteCommittables = createDeleteCommittables(deletedIndexEntries);
            DataStream<Committable> deletes =
                    StreamExecutionEnvironmentUtils.fromData(
                                    env,
                                    new CommittableTypeInfo(),
                                    deleteCommittables.toArray(new Committable[0]))
                            .name("Index Delete Source")
                            .setParallelism(1);
            built = built.union(deletes);
        }

        commit(table, indexType, built);
        return true;
    }

    static List<IndexedSplit> computeShardTasks(
            FileStoreTable table, List<ManifestEntry> entries, long rowsPerShard) {
        return createShardIndexedSplits(table, entries, rowsPerShard);
    }

    static List<IndexedSplit> computeShardTasks(
            FileStoreTable table,
            List<ManifestEntry> entries,
            long rowsPerShard,
            long maxIndexedRowId) {
        return computeShardTasks(table, entries, rowsPerShard, rowRangesAfter(maxIndexedRowId));
    }

    static List<IndexedSplit> computeShardTasks(
            FileStoreTable table,
            List<ManifestEntry> entries,
            long rowsPerShard,
            @Nullable List<Range> rowRangesToBuild) {
        return createShardIndexedSplits(table, entries, rowsPerShard, rowRangesToBuild);
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

    /**
     * Operator that receives an {@link IndexedSplit}, reads data from its split, builds the index,
     * and emits a {@link Committable}. Each shard's split contains only the files relevant to this
     * shard, so no redundant I/O occurs.
     */
    private static class BuildIndexOperator
            extends BoundedOneInputOperator<IndexedSplit, Committable> {

        private static final long serialVersionUID = 1L;

        private final ReadBuilder readBuilder;
        private final FileStoreTable table;
        private final String indexType;
        private final DataField indexField;
        private final List<DataField> extraFields;
        private final RowType projectedRowType;
        private final Options mergedOptions;

        private transient TableRead tableRead;
        private transient List<DataField> indexedFields;
        private transient InternalRow.FieldGetter[] indexFieldGetters;
        private transient int rowIdFieldIndex;
        private transient boolean multiColumn;
        private transient ProjectedRow writerProjection;

        BuildIndexOperator(
                ReadBuilder readBuilder,
                FileStoreTable table,
                String indexType,
                DataField indexField,
                List<DataField> extraFields,
                RowType projectedRowType,
                Options mergedOptions) {
            this.readBuilder = readBuilder;
            this.table = table;
            this.indexType = indexType;
            this.indexField = indexField;
            this.extraFields = extraFields;
            this.projectedRowType = projectedRowType;
            this.mergedOptions = mergedOptions;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.tableRead = readBuilder.newRead();
            // The primary column followed by the extra columns, in index order. Field getters and
            // the writer projection both need the full ordered list.
            this.indexedFields = new ArrayList<>(1 + extraFields.size());
            indexedFields.add(indexField);
            indexedFields.addAll(extraFields);
            this.indexFieldGetters = new InternalRow.FieldGetter[indexedFields.size()];
            for (int i = 0; i < indexedFields.size(); i++) {
                DataField field = indexedFields.get(i);
                indexFieldGetters[i] =
                        InternalRow.createFieldGetter(
                                field.type(), projectedRowType.getFieldIndex(field.name()));
            }
            this.rowIdFieldIndex = projectedRowType.getFieldIndex(SpecialFields.ROW_ID.name());
            this.multiColumn = !extraFields.isEmpty();
            if (multiColumn) {
                int[] projection = new int[indexedFields.size()];
                for (int i = 0; i < indexedFields.size(); i++) {
                    projection[i] = projectedRowType.getFieldIndex(indexedFields.get(i).name());
                }
                this.writerProjection = ProjectedRow.from(projection);
            }
        }

        @Override
        public void processElement(StreamRecord<IndexedSplit> element) throws Exception {
            IndexedSplit task = element.getValue();
            checkArgument(
                    task.rowRanges().size() == 1,
                    "Each generic global index task should contain exactly one row range.");
            Range shardRange = task.rowRanges().get(0);
            BinaryRow partition = task.dataSplit().partition();

            LOG.info(
                    "Building {} index for partition={}, shardRange=[{}, {}], files={}.",
                    indexType,
                    partition,
                    shardRange.from,
                    shardRange.to,
                    task.dataSplit().dataFiles().size());
            long startTime = System.currentTimeMillis();

            GlobalIndexWriter indexWriter =
                    createIndexWriter(table, indexType, indexField, extraFields, mergedOptions);

            try {
                long rowsSeen = 0;
                long lastRowId = Long.MIN_VALUE;
                try (RecordReader<InternalRow> reader = tableRead.createReader(task.dataSplit());
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
                                            shardRange.from,
                                            shardRange.to));
                        }
                        lastRowId = currentRowId;

                        if (currentRowId > shardRange.to) {
                            break;
                        }
                        // Only write rows within this shard's range
                        if (currentRowId >= shardRange.from) {
                            long rowId = currentRowId - shardRange.from;
                            if (multiColumn) {
                                ((GlobalIndexMultiColumnWriter) indexWriter)
                                        .write(rowId, writerProjection.replaceRow(row));
                            } else {
                                Object fieldData = indexFieldGetters[0].getFieldOrNull(row);
                                ((GlobalIndexSingleColumnWriter) indexWriter)
                                        .write(fieldData, rowId);
                            }
                            rowsSeen++;
                        }
                    }
                }

                List<ResultEntry> resultEntries = indexWriter.finish();
                if (!resultEntries.isEmpty() && resultEntries.get(0).rowCount() != rowsSeen) {
                    LOG.warn(
                            "rowCount mismatch: writer reported {} but caller saw {} rows",
                            resultEntries.get(0).rowCount(),
                            rowsSeen);
                }
                long elapsed = System.currentTimeMillis() - startTime;
                LOG.info(
                        "Finished shard [{}, {}]: saw {} rows, "
                                + "produced {} result entries in {} ms.",
                        shardRange.from,
                        shardRange.to,
                        rowsSeen,
                        resultEntries.size(),
                        elapsed);

                if (resultEntries.isEmpty()) {
                    LOG.warn(
                            "Shard [{}, {}] produced no index (all null or empty), "
                                    + "skipping index flush.",
                            shardRange.from,
                            shardRange.to);
                    return;
                }

                CommitMessage commitMessage =
                        flushIndex(
                                table,
                                partition,
                                shardRange,
                                indexedFields,
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
            List<DataField> indexFields,
            String indexType,
            List<ResultEntry> resultEntries)
            throws IOException {
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        rowRange,
                        indexFields,
                        indexType,
                        resultEntries);
        return new CommitMessageImpl(
                partition, 0, null, indexIncrement(indexFileMetas), emptyIncrement());
    }

    private static void closeWriterQuietly(GlobalIndexWriter writer) {
        if (writer instanceof Closeable) {
            try {
                ((Closeable) writer).close();
            } catch (IOException ignored) {
            }
        }
    }
}
