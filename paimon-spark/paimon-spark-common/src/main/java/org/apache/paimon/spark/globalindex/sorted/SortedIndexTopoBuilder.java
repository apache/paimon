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

package org.apache.paimon.spark.globalindex.sorted;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexWriter;
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.SparkRow;
import org.apache.paimon.spark.globalindex.GlobalIndexTopologyBuilder;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PaimonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.groupSplitsByRange;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.shardSplitsByRowRange;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.splitByContiguousRowRange;

/** The {@link GlobalIndexTopologyBuilder} for sorted indexes. */
public class SortedIndexTopoBuilder implements GlobalIndexTopologyBuilder {

    private static final String BUILD_TASK_ID_FIELD = "_SORTED_INDEX_BUILD_TASK_ID";
    private static final int BUILD_TASK_ID_FIELD_ID = -1;

    private static final HashSet<String> SUPPORTED_INDEX_TYPES =
            new HashSet<>(Arrays.asList("btree", "bitmap", "multivalue"));

    public static boolean supports(String indexType) {
        return SUPPORTED_INDEX_TYPES.contains(indexType);
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
        SortedGlobalIndexScanner indexScanner =
                new SortedGlobalIndexScanner(table, indexType, options)
                        .withIndexField(indexField.name());
        if (partitionPredicate != null) {
            indexScanner = indexScanner.withPartitionPredicate(partitionPredicate);
        }

        Optional<ScanResult<DataSplit>> optionalScanResult = indexScanner.incrementalScan();
        if (!optionalScanResult.isPresent()) {
            return Collections.emptyList();
        }

        ScanResult<DataSplit> scanResult = optionalScanResult.get();
        long scanSnapshotId = scanResult.scanSnapshotId();
        List<DataSplit> splits = splitByContiguousRowRange(scanResult.entries());
        if (splits.isEmpty()) {
            return Collections.emptyList();
        }

        Map<BinaryRow, Map<Range, List<Split>>> partitionRangeSplits =
                groupSplitsByRange(scanResult.rowRangeIndex(), splits);
        if (partitionRangeSplits.isEmpty()) {
            return Collections.emptyList();
        }

        // Calculate maximum parallelism bound
        long recordsPerRange = options.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE);
        int maxParallelism = options.get(SortedIndexOptions.SORTED_INDEX_BUILD_MAX_PARALLELISM);

        List<CommitMessage> allMessages = new ArrayList<>();
        GlobalIndexer indexer = GlobalIndexer.create(indexType, indexField, options);
        if (!(indexer instanceof SortedGlobalIndexer)) {
            throw new IllegalArgumentException(
                    "Index algorithm " + indexType + " does not expose sorted index keys.");
        }
        GlobalIndexKeyExtractor keyExtractor = ((SortedGlobalIndexer) indexer).keyExtractor();
        String taskIdField = buildTaskIdFieldName(readType);
        RowType normalizedReadType =
                normalizedReadType(readType, taskIdField, indexField, keyExtractor.keyType());
        final int partitionKeyNum = table.partitionKeys().size();
        BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        SortedGlobalIndexWriter indexWriter =
                new SortedGlobalIndexWriter(table, indexType, options)
                        .withIndexField(indexField.name());
        final byte[] serializedWriter = InstantiationUtil.serializeObject(indexWriter);
        if (keyExtractor.isIdentity()) {
            List<SortedBuildTask> buildTasks = new ArrayList<>();
            List<Dataset<Row>> taskInputs = new ArrayList<>();
            for (Map.Entry<BinaryRow, Map<Range, List<Split>>> partitionEntry :
                    partitionRangeSplits.entrySet()) {
                byte[] partitionBytes =
                        binaryRowSerializer.serializeToBytes(partitionEntry.getKey());
                for (Map.Entry<Range, List<Split>> entry : partitionEntry.getValue().entrySet()) {
                    Range range = entry.getKey();
                    List<Split> rangeSplits = entry.getValue();
                    if (rangeSplits.isEmpty()) {
                        continue;
                    }

                    long taskId = buildTasks.size();
                    buildTasks.add(new SortedBuildTask(taskId, range, partitionBytes));
                    Dataset<Row> source =
                            PaimonUtils.createDataset(
                                    spark,
                                    ScanPlanHelper$.MODULE$.createNewScanPlan(
                                            rangeSplits.toArray(new Split[0]), relation));
                    Dataset<Row> selected =
                            source.select(
                                            readType.getFieldNames().stream()
                                                    .map(functions::col)
                                                    .toArray(Column[]::new))
                                    .withColumn(taskIdField, functions.lit(taskId).cast("long"));
                    taskInputs.add(
                            selected.select(
                                    functions.col(taskIdField),
                                    functions.col(indexField.name()),
                                    functions.col(SpecialFields.ROW_ID.name())));
                }
            }

            if (!buildTasks.isEmpty()) {
                int partitionNum =
                        calculateParallelism(buildTasks, recordsPerRange, maxParallelism);
                Dataset<Row> partitioned =
                        combineAndSortBuildTaskInputs(
                                taskInputs, partitionNum, taskIdField, indexField.name());
                Map<Long, SortedBuildTask> buildTasksById = new HashMap<>();
                for (SortedBuildTask task : buildTasks) {
                    buildTasksById.put(task.taskId, task);
                }
                JavaRDD<byte[]> written =
                        partitioned
                                .javaRDD()
                                .map(row -> (InternalRow) (new SparkRow(normalizedReadType, row)))
                                .mapPartitions(
                                        (FlatMapFunction<Iterator<InternalRow>, byte[]>)
                                                iter ->
                                                        buildSortedIndexes(
                                                                iter,
                                                                serializedWriter,
                                                                buildTasksById,
                                                                partitionKeyNum,
                                                                scanSnapshotId));
                allMessages.addAll(CommitMessageSerializer.deserializeAll(written.collect()));
            }
            addDeletedIndexMessages(allMessages, scanResult.deletedIndexEntries());
            return allMessages;
        }

        for (Map.Entry<BinaryRow, Map<Range, List<Split>>> partitionEntry :
                partitionRangeSplits.entrySet()) {
            for (Map.Entry<Range, List<Split>> entry : partitionEntry.getValue().entrySet()) {
                Range range = entry.getKey();
                List<Split> rangeSplits = entry.getValue();
                if (rangeSplits.isEmpty()) {
                    continue;
                }

                final byte[] partitionBytes =
                        binaryRowSerializer.serializeToBytes(partitionEntry.getKey());

                Map<Range, List<Split>> shardedSplits =
                        shardSplitsByRowRange(
                                Collections.singletonMap(range, rangeSplits), recordsPerRange);
                if (shardedSplits.isEmpty()) {
                    continue;
                }
                int partitionNum = Math.min(shardedSplits.size(), maxParallelism);
                List<Split> splitsToRead = new ArrayList<>();
                Map<Long, Range> taskRanges = new HashMap<>();
                for (Map.Entry<Range, List<Split>> shard : shardedSplits.entrySet()) {
                    splitsToRead.addAll(shard.getValue());
                    taskRanges.put(shard.getKey().from / recordsPerRange, shard.getKey());
                }

                Dataset<Row> source =
                        PaimonUtils.createDataset(
                                spark,
                                ScanPlanHelper$.MODULE$.createNewScanPlan(
                                        splitsToRead.toArray(new Split[0]), relation));

                Dataset<Row> selected =
                        source.select(
                                        functions.col(indexField.name()),
                                        functions.col(SpecialFields.ROW_ID.name()))
                                .withColumn(
                                        taskIdField,
                                        functions
                                                .expr(
                                                        "`"
                                                                + SpecialFields.ROW_ID.name()
                                                                + "` DIV "
                                                                + recordsPerRange)
                                                .cast("long"));
                Dataset<Row> normalized =
                        selected.select(
                                functions.col(taskIdField),
                                functions
                                        .explode_outer(functions.col(indexField.name()))
                                        .alias(indexField.name()),
                                functions.col(SpecialFields.ROW_ID.name()));

                Column[] sortFields =
                        new Column[] {
                            functions.col(taskIdField),
                            functions.col(indexField.name()),
                            functions.col(SpecialFields.ROW_ID.name())
                        };

                Dataset<Row> partitioned =
                        normalized
                                .repartition(partitionNum, functions.col(taskIdField))
                                .sortWithinPartitions(sortFields);

                JavaRDD<byte[]> written =
                        partitioned
                                .javaRDD()
                                .map(row -> (InternalRow) (new SparkRow(normalizedReadType, row)))
                                .mapPartitions(
                                        (FlatMapFunction<Iterator<InternalRow>, byte[]>)
                                                iter ->
                                                        buildShardedSortedIndexes(
                                                                iter,
                                                                serializedWriter,
                                                                taskRanges,
                                                                partitionKeyNum,
                                                                partitionBytes,
                                                                scanSnapshotId));
                List<byte[]> commitBytes = written.collect();
                allMessages.addAll(CommitMessageSerializer.deserializeAll(commitBytes));
            }
        }
        addDeletedIndexMessages(allMessages, scanResult.deletedIndexEntries());
        return allMessages;
    }

    static Dataset<Row> combineAndSortBuildTaskInputs(
            List<Dataset<Row>> taskInputs,
            int partitionNum,
            String taskIdField,
            String indexField) {
        Dataset<Row> combined = taskInputs.get(0);
        for (int i = 1; i < taskInputs.size(); i++) {
            combined = combined.union(taskInputs.get(i));
        }

        Column[] sortFields =
                new Column[] {
                    functions.col(taskIdField),
                    functions.col(indexField),
                    functions.col(SpecialFields.ROW_ID.name())
                };
        return combined.repartitionByRange(partitionNum, sortFields)
                .sortWithinPartitions(sortFields);
    }

    private static void addDeletedIndexMessages(
            List<CommitMessage> messages, List<IndexManifestEntry> deletedIndexEntries) {
        for (IndexManifestEntry entry : deletedIndexEntries) {
            messages.add(
                    new CommitMessageImpl(
                            entry.partition(),
                            entry.bucket(),
                            null,
                            DataIncrement.deleteIndexIncrement(
                                    Collections.singletonList(entry.indexFile())),
                            CompactIncrement.emptyIncrement()));
        }
    }

    private static Iterator<byte[]> buildSortedIndexes(
            Iterator<InternalRow> input,
            byte[] serializedWriter,
            Map<Long, SortedBuildTask> buildTasksById,
            int partitionKeyNum,
            long scanSnapshotId)
            throws IOException, ClassNotFoundException {
        final BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        SortedGlobalIndexWriter writer =
                InstantiationUtil.deserializeObject(
                        serializedWriter, SortedGlobalIndexWriter.class.getClassLoader());
        SortedTaskInput taskInput = new SortedTaskInput(input, writer.keyExtractor().keyType());
        List<byte[]> results = new ArrayList<>();
        while (taskInput.hasTask()) {
            long taskId = taskInput.taskId();
            SortedBuildTask task = buildTasksById.get(taskId);
            if (task == null) {
                throw new IllegalArgumentException("Unknown sorted index build task id: " + taskId);
            }
            BinaryRow partition = binaryRowSerializer.deserializeFromBytes(task.partition);
            results.addAll(
                    CommitMessageSerializer.serializeAll(
                            writer.buildForSinglePartition(
                                    task.rowRange,
                                    partition,
                                    taskInput.consumeTask(taskId),
                                    scanSnapshotId)));
        }
        return results.iterator();
    }

    private static Iterator<byte[]> buildShardedSortedIndexes(
            Iterator<InternalRow> input,
            byte[] serializedWriter,
            Map<Long, Range> taskRanges,
            int partitionKeyNum,
            byte[] partitionBytes,
            long scanSnapshotId)
            throws IOException, ClassNotFoundException {
        final BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        BinaryRow partition = binaryRowSerializer.deserializeFromBytes(partitionBytes);
        SortedGlobalIndexWriter writer =
                InstantiationUtil.deserializeObject(
                        serializedWriter, SortedGlobalIndexWriter.class.getClassLoader());
        SortedTaskInput taskInput = new SortedTaskInput(input, writer.keyExtractor().keyType());
        List<byte[]> results = new ArrayList<>();
        while (taskInput.hasTask()) {
            long taskId = taskInput.taskId();
            Range range = taskRanges.get(taskId);
            if (range == null) {
                throw new IllegalArgumentException("Unknown sorted index build task id: " + taskId);
            }
            results.addAll(
                    CommitMessageSerializer.serializeAll(
                            writer.buildForSinglePartition(
                                    range,
                                    partition,
                                    taskInput.consumeTask(taskId),
                                    scanSnapshotId)));
        }
        return results.iterator();
    }

    static int calculateParallelism(
            List<SortedBuildTask> buildTasks, long recordsPerRange, int maxParallelism) {
        long totalRecords = 0;
        for (SortedBuildTask task : buildTasks) {
            long count = task.rowRange.count();
            if (Long.MAX_VALUE - totalRecords < count) {
                totalRecords = Long.MAX_VALUE;
            } else {
                totalRecords += count;
            }
        }

        long parallelism = Math.max(totalRecords / recordsPerRange, 1);
        return (int) Math.min(parallelism, maxParallelism);
    }

    private static String buildTaskIdFieldName(RowType readType) {
        String fieldName = BUILD_TASK_ID_FIELD;
        while (readType.containsField(fieldName)) {
            fieldName = "_" + fieldName;
        }
        return fieldName;
    }

    private static RowType normalizedReadType(
            RowType readType,
            String taskIdField,
            DataField sourceField,
            org.apache.paimon.types.DataType keyType) {
        return RowType.of(
                new DataField(BUILD_TASK_ID_FIELD_ID, taskIdField, DataTypes.BIGINT().notNull()),
                new DataField(sourceField.id(), sourceField.name(), keyType),
                readType.getField(SpecialFields.ROW_ID.name()));
    }

    /** Metadata for one sorted index build range. */
    static class SortedBuildTask implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long taskId;
        private final Range rowRange;
        private final byte[] partition;

        SortedBuildTask(long taskId, Range rowRange, byte[] partition) {
            this.taskId = taskId;
            this.rowRange = rowRange;
            this.partition = partition;
        }
    }

    /** Groups a partition already sorted by task id, normalized key and row id. */
    private static class SortedTaskInput {

        private final Iterator<InternalRow> input;
        private final InternalRow.FieldGetter keyGetter;

        private InternalRow next;

        private SortedTaskInput(
                Iterator<InternalRow> input, org.apache.paimon.types.DataType keyType) {
            this.input = input;
            this.keyGetter = InternalRow.createFieldGetter(keyType, 1);
            advance();
        }

        private boolean hasTask() {
            return next != null;
        }

        private long taskId() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            return next.getLong(0);
        }

        private Iterator<InternalRow> consumeTask(long taskId) {
            return new Iterator<InternalRow>() {
                @Override
                public boolean hasNext() {
                    return next != null && next.getLong(0) == taskId;
                }

                @Override
                public InternalRow next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    InternalRow row = next;
                    InternalRow result =
                            GenericRow.of(keyGetter.getFieldOrNull(row), row.getLong(2));
                    advance();
                    return result;
                }
            };
        }

        private void advance() {
            next = input.hasNext() ? input.next() : null;
        }
    }
}
