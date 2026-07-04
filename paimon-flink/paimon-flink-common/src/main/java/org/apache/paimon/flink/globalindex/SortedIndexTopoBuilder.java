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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.flink.sink.CommitterOperatorFactory;
import org.apache.paimon.flink.sink.NoopCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder.groupSplitsByRange;
import static org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder.splitByContiguousRowRange;

/** The topology builder for sorted indexes in Flink. */
public class SortedIndexTopoBuilder {

    private static final String BUILD_TASK_ID_FIELD = "_SORTED_INDEX_BUILD_TASK_ID";
    private static final int BUILD_TASK_ID_FIELD_ID = -1;
    private static final HashSet<String> SUPPORTED_INDEX_TYPES =
            new HashSet<>(Arrays.asList("btree", "bitmap"));

    public static boolean supports(String indexType) {
        return SUPPORTED_INDEX_TYPES.contains(indexType);
    }

    public static boolean buildIndex(
            StreamExecutionEnvironment env,
            Supplier<SortedGlobalIndexBuilder> indexBuilderSupplier,
            FileStoreTable table,
            List<String> indexColumns,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        List<DataStream<Committable>> allStreams = new ArrayList<>();
        for (String indexColumn : indexColumns) {
            SortedGlobalIndexBuilder indexBuilder =
                    indexBuilderSupplier.get().withIndexField(indexColumn);
            if (partitionPredicate != null) {
                indexBuilder = indexBuilder.withPartitionPredicate(partitionPredicate);
            }

            Optional<Pair<RowRangeIndex, List<DataSplit>>> indexRangeAndSplits =
                    indexBuilder.incrementalScan();
            if (!indexRangeAndSplits.isPresent()) {
                continue;
            }

            Pair<RowRangeIndex, List<DataSplit>> scanResult = indexRangeAndSplits.get();
            List<DataSplit> splits = splitByContiguousRowRange(scanResult.getRight());
            if (splits.isEmpty()) {
                continue;
            }
            Map<BinaryRow, Map<Range, List<Split>>> partitionRangeSplits =
                    groupSplitsByRange(scanResult.getLeft(), splits);
            if (partitionRangeSplits.isEmpty()) {
                continue;
            }

            // 2. Select necessary columns (index field + ROW_ID)
            List<String> selectedColumns = new ArrayList<>();
            selectedColumns.add(indexColumn);

            RowType dataReadType =
                    SpecialFields.rowTypeWithRowId(table.rowType().project(selectedColumns));
            String buildTaskIdField = buildTaskIdFieldName(dataReadType);
            RowType sortReadType = withBuildTaskId(dataReadType, buildTaskIdField);
            int taskIdPos = sortReadType.getFieldIndex(buildTaskIdField);
            int indexFieldPos = sortReadType.getFieldIndex(indexColumn);
            int rowIdPos = sortReadType.getFieldIndex(SpecialFields.ROW_ID.name());
            DataType indexFieldType = sortReadType.getTypeAt(indexFieldPos);

            // 3. Calculate maximum parallelism bound
            long recordsPerRange =
                    userOptions.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE);
            int maxParallelism =
                    userOptions.get(SortedIndexOptions.SORTED_INDEX_BUILD_MAX_PARALLELISM);

            // 4. Build one topology for all contiguous row ranges
            CoreOptions coreOptions = table.coreOptions();
            ReadBuilder readBuilder = table.newReadBuilder().withReadType(dataReadType);
            List<String> sortColumns = new ArrayList<>();
            sortColumns.add(buildTaskIdField);
            sortColumns.add(indexColumn);
            int partitionFieldSize = table.partitionKeys().size();
            BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionFieldSize);
            List<SortedBuildTask> buildTasks = new ArrayList<>();
            List<SortedSplitTask> splitTasks = new ArrayList<>();
            for (Map.Entry<BinaryRow, Map<Range, List<Split>>> partitionEntry :
                    partitionRangeSplits.entrySet()) {
                BinaryRow partition = partitionEntry.getKey();
                byte[] partitionBytes = binaryRowSerializer.serializeToBytes(partition);
                for (Map.Entry<Range, List<Split>> entry : partitionEntry.getValue().entrySet()) {
                    Range range = entry.getKey();
                    List<Split> rangeSplits = entry.getValue();
                    if (rangeSplits.isEmpty()) {
                        continue;
                    }

                    int taskId = buildTasks.size();
                    buildTasks.add(new SortedBuildTask(taskId, range, partitionBytes));
                    for (Split split : rangeSplits) {
                        splitTasks.add(new SortedSplitTask(taskId, split));
                    }
                }
            }

            if (buildTasks.isEmpty()) {
                return false;
            }

            DataStream<Committable> commitMessages =
                    executeForBuildTasks(
                            env,
                            buildTasks,
                            splitTasks,
                            readBuilder,
                            indexBuilder,
                            partitionFieldSize,
                            taskIdPos,
                            indexFieldPos,
                            rowIdPos,
                            indexFieldType,
                            sortColumns,
                            coreOptions,
                            sortReadType,
                            recordsPerRange,
                            maxParallelism);

            allStreams.add(commitMessages);
        }
        if (!allStreams.isEmpty()) {
            @SuppressWarnings("unchecked")
            DataStream<Committable>[] rest =
                    allStreams.subList(1, allStreams.size()).toArray(new DataStream[0]);
            commit(table, allStreams.get(0).union(rest));
            return true;
        }

        return false;
    }

    public static void buildIndexAndExecute(
            StreamExecutionEnvironment env,
            FileStoreTable table,
            String indexColumn,
            String indexType,
            PartitionPredicate partitionPredicate,
            Options userOptions)
            throws Exception {
        if (buildIndex(
                env,
                () -> new SortedGlobalIndexBuilder(table, indexType, userOptions),
                table,
                Collections.singletonList(indexColumn),
                partitionPredicate,
                userOptions)) {
            env.execute("Create " + indexType + " global index for table: " + table.name());
        }
    }

    protected static DataStream<Committable> executeForBuildTasks(
            StreamExecutionEnvironment env,
            List<SortedBuildTask> buildTasks,
            List<SortedSplitTask> splitTasks,
            ReadBuilder readBuilder,
            SortedGlobalIndexBuilder indexBuilder,
            int partitionFieldSize,
            int taskIdPos,
            int indexFieldPos,
            int rowIdPos,
            DataType indexFieldType,
            List<String> sortColumns,
            CoreOptions coreOptions,
            RowType readType,
            long recordsPerRange,
            int maxParallelism) {
        int parallelism = calculateParallelism(buildTasks, recordsPerRange, maxParallelism);

        DataStream<SortedSplitTask> sourceStream =
                StreamExecutionEnvironmentUtils.fromData(
                                env, splitTasks, new JavaTypeInfo<>(SortedSplitTask.class))
                        .name("Global Index Source")
                        .setParallelism(1);

        DataStream<RowData> rowDataStream =
                sourceStream
                        .transform(
                                "Read Data",
                                InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(readType)),
                                new ReadDataOperator(readBuilder))
                        .setParallelism(parallelism);

        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(sortColumns)
                        .setSortStrategy(CoreOptions.OrderType.ORDER)
                        .setSinkParallelism(parallelism)
                        .setLocalSampleSize(parallelism * coreOptions.getLocalSampleMagnification())
                        .setGlobalSampleSize(parallelism * 1000)
                        .setRangeNumber(parallelism * 10)
                        .build();

        TableSorter sorter =
                TableSorter.getSorter(env, rowDataStream, coreOptions, readType, sortInfo);
        DataStream<RowData> sortedStream = sorter.sort();

        return sortedStream
                .transform(
                        "write-sorted-index",
                        new CommittableTypeInfo(),
                        new WriteIndexOperator(
                                buildTasks,
                                partitionFieldSize,
                                indexBuilder,
                                taskIdPos,
                                indexFieldPos,
                                rowIdPos,
                                indexFieldType))
                .setParallelism(parallelism);
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

    private static RowType withBuildTaskId(RowType readType, String buildTaskIdField) {
        List<DataField> fields = new ArrayList<>();
        fields.add(
                new DataField(BUILD_TASK_ID_FIELD_ID, buildTaskIdField, DataTypes.INT().notNull()));
        fields.addAll(readType.getFields());
        return new RowType(readType.isNullable(), fields);
    }

    private static void commit(FileStoreTable table, DataStream<Committable> written) {
        OneInputStreamOperatorFactory<Committable, Committable> committerOperator =
                new CommitterOperatorFactory<>(
                        false,
                        true,
                        "SortedIndexCommitter-" + UUID.randomUUID(),
                        context ->
                                new StoreCommitter(
                                        table, table.newCommit(context.commitUser()), context),
                        new NoopCommittableStateManager());

        written.transform("COMMIT OPERATOR", new CommittableTypeInfo(), committerOperator)
                .setParallelism(1)
                .setMaxParallelism(1);
    }

    /** Operator to read data from splits. */
    private static class ReadDataOperator
            extends org.apache.flink.table.runtime.operators.TableStreamOperator<RowData>
            implements org.apache.flink.streaming.api.operators.OneInputStreamOperator<
                    SortedSplitTask, RowData> {

        private static final long serialVersionUID = 1L;

        private final ReadBuilder readBuilder;

        private transient TableRead tableRead;

        public ReadDataOperator(ReadBuilder readBuilder) {
            this.readBuilder = readBuilder;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.tableRead = readBuilder.newRead();
        }

        @Override
        public void processElement(StreamRecord<SortedSplitTask> element) throws Exception {
            SortedSplitTask buildTask = element.getValue();
            GenericRow taskId = GenericRow.of(buildTask.taskId);
            try (RecordReader<InternalRow> reader = tableRead.createReader(buildTask.split)) {
                reader.forEachRemaining(
                        row ->
                                output.collect(
                                        new StreamRecord<>(
                                                new FlinkRowData(new JoinedRow(taskId, row)))));
            }
        }
    }

    private static class WriteIndexOperator extends BoundedOneInputOperator<RowData, Committable> {

        private final List<SortedBuildTask> buildTasks;
        private final int partitionFieldSize;
        private final SortedGlobalIndexBuilder builder;
        private final int taskIdPos;
        private final int indexFieldPos;
        private final int rowIdPos;
        private final DataType indexFieldType;

        private transient long counter;
        private transient SortedBuildTask currentTask;
        private transient BinaryRow currentPartition;
        private transient GlobalIndexSingleColumnWriter currentWriter;
        private transient List<CommitMessage> commitMessages;
        private transient Map<Integer, SortedBuildTask> buildTasksById;
        private transient InternalRow.FieldGetter indexFieldGetter;
        private transient BinaryRowSerializer binaryRowSerializer;

        public WriteIndexOperator(
                List<SortedBuildTask> buildTasks,
                int partitionFieldSize,
                SortedGlobalIndexBuilder builder,
                int taskIdPos,
                int indexFieldPos,
                int rowIdPos,
                DataType indexFieldType) {
            this.buildTasks = buildTasks;
            this.partitionFieldSize = partitionFieldSize;
            this.builder = builder;
            this.taskIdPos = taskIdPos;
            this.indexFieldPos = indexFieldPos;
            this.rowIdPos = rowIdPos;
            this.indexFieldType = indexFieldType;
        }

        @Override
        public void open() throws Exception {
            super.open();
            commitMessages = new ArrayList<>();
            buildTasksById = new HashMap<>();
            for (SortedBuildTask task : buildTasks) {
                buildTasksById.put(task.taskId, task);
            }
            indexFieldGetter = InternalRow.createFieldGetter(indexFieldType, indexFieldPos);
            this.binaryRowSerializer = new BinaryRowSerializer(partitionFieldSize);
        }

        @Override
        public void processElement(StreamRecord<RowData> element) throws IOException {
            InternalRow row = new FlinkRowWrapper(element.getValue());
            int taskId = row.getInt(taskIdPos);
            SortedBuildTask task = buildTasksById.get(taskId);
            if (task == null) {
                throw new IllegalArgumentException("Unknown sorted index build task id: " + taskId);
            }

            if (currentTask == null || currentTask.taskId != taskId) {
                flushCurrentWriter();
                currentTask = task;
                currentPartition = binaryRowSerializer.deserializeFromBytes(task.partition);
            }

            if (currentWriter != null && counter >= builder.recordsPerRange()) {
                flushCurrentWriter();
            }

            counter++;

            if (currentWriter == null) {
                currentWriter = builder.createWriter();
            }

            long localRowId = row.getLong(rowIdPos) - currentTask.rowRange.from;
            currentWriter.write(indexFieldGetter.getFieldOrNull(row), localRowId);
        }

        @Override
        public void endInput() throws IOException {
            flushCurrentWriter();
            for (CommitMessage message : commitMessages) {
                output.collect(
                        new StreamRecord<>(
                                new Committable(BatchWriteBuilder.COMMIT_IDENTIFIER, message)));
            }
            commitMessages.clear();
        }

        private void flushCurrentWriter() throws IOException {
            if (counter > 0 && currentWriter != null) {
                commitMessages.add(
                        builder.flushIndex(
                                currentTask.rowRange, currentWriter.finish(), currentPartition));
            }
            currentWriter = null;
            counter = 0;
        }
    }

    /** Metadata for one sorted index build range. */
    public static class SortedBuildTask implements Serializable {

        private static final long serialVersionUID = 1L;

        private int taskId;
        private Range rowRange;
        private byte[] partition;

        public SortedBuildTask() {}

        public SortedBuildTask(int taskId, Range rowRange, byte[] partition) {
            this.taskId = taskId;
            this.rowRange = rowRange;
            this.partition = partition;
        }
    }

    /** Split assigned to one sorted index build task. */
    public static class SortedSplitTask implements Serializable {

        private static final long serialVersionUID = 1L;

        private int taskId;
        private Split split;

        public SortedSplitTask() {}

        public SortedSplitTask(int taskId, Split split) {
            this.taskId = taskId;
            this.split = split;
        }
    }
}
