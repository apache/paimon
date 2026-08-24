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
        for (Map.Entry<BinaryRow, Map<Range, List<Split>>> partitionEntry :
                partitionRangeSplits.entrySet()) {
            for (Map.Entry<Range, List<Split>> entry : partitionEntry.getValue().entrySet()) {
                Range range = entry.getKey();
                List<Split> rangeSplits = entry.getValue();
                if (rangeSplits.isEmpty()) {
                    continue;
                }

                final byte[] serializedWriter = InstantiationUtil.serializeObject(indexWriter);
                final byte[] partitionBytes =
                        binaryRowSerializer.serializeToBytes(partitionEntry.getKey());
                if (keyExtractor.isIdentity()) {
                    int partitionNum = Math.max((int) (range.count() / recordsPerRange), 1);
                    partitionNum = Math.min(partitionNum, maxParallelism);

                    Dataset<Row> source =
                            PaimonUtils.createDataset(
                                    spark,
                                    ScanPlanHelper$.MODULE$.createNewScanPlan(
                                            rangeSplits.toArray(new Split[0]), relation));
                    Dataset<Row> selected =
                            source.select(
                                    readType.getFieldNames().stream()
                                            .map(functions::col)
                                            .toArray(Column[]::new));
                    Column[] sortFields =
                            new Column[] {
                                functions.col(indexField.name()),
                                functions.col(SpecialFields.ROW_ID.name())
                            };
                    Dataset<Row> partitioned =
                            selected.repartitionByRange(partitionNum, sortFields)
                                    .sortWithinPartitions(sortFields);
                    JavaRDD<byte[]> written =
                            partitioned
                                    .javaRDD()
                                    .map(row -> (InternalRow) (new SparkRow(readType, row)))
                                    .mapPartitions(
                                            (FlatMapFunction<Iterator<InternalRow>, byte[]>)
                                                    iter ->
                                                            buildSortedIndex(
                                                                    iter,
                                                                    serializedWriter,
                                                                    range,
                                                                    partitionKeyNum,
                                                                    partitionBytes,
                                                                    scanSnapshotId));
                    allMessages.addAll(CommitMessageSerializer.deserializeAll(written.collect()));
                    continue;
                }

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
                                                        buildSortedIndexes(
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
        for (IndexManifestEntry entry : scanResult.deletedIndexEntries()) {
            allMessages.add(
                    new CommitMessageImpl(
                            entry.partition(),
                            entry.bucket(),
                            null,
                            DataIncrement.deleteIndexIncrement(
                                    Collections.singletonList(entry.indexFile())),
                            CompactIncrement.emptyIncrement()));
        }
        return allMessages;
    }

    private static Iterator<byte[]> buildSortedIndex(
            Iterator<InternalRow> input,
            byte[] serializedWriter,
            Range range,
            int partitionKeyNum,
            byte[] partitionBytes,
            long scanSnapshotId)
            throws IOException, ClassNotFoundException {
        final BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        BinaryRow partition = binaryRowSerializer.deserializeFromBytes(partitionBytes);
        SortedGlobalIndexWriter writer =
                InstantiationUtil.deserializeObject(
                        serializedWriter, SortedGlobalIndexWriter.class.getClassLoader());
        return CommitMessageSerializer.serializeAll(
                        writer.buildForSinglePartition(range, partition, input, scanSnapshotId))
                .iterator();
    }

    private static Iterator<byte[]> buildSortedIndexes(
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
