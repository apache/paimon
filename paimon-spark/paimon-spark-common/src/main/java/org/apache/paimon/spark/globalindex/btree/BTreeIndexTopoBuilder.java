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

package org.apache.paimon.spark.globalindex.btree;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.globalindex.btree.BTreeWithFileMetaBuilder;
import org.apache.paimon.globalindex.btree.BinaryFileMetaWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.SparkRow;
import org.apache.paimon.spark.globalindex.GlobalIndexTopologyBuilder;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PaimonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.functions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder.groupSplitsByRange;
import static org.apache.paimon.globalindex.btree.BTreeGlobalIndexBuilder.splitByContiguousRowRange;

/** The {@link GlobalIndexTopologyBuilder} for BTree index. */
public class BTreeIndexTopoBuilder implements GlobalIndexTopologyBuilder {

    @Override
    public String identifier() {
        return "btree";
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
        // 1. read the whole dataset of target partitions
        BTreeGlobalIndexBuilder indexBuilder =
                new BTreeGlobalIndexBuilder(table).withIndexField(indexField.name());
        if (partitionPredicate != null) {
            indexBuilder = indexBuilder.withPartitionPredicate(partitionPredicate);
        }

        Optional<Pair<RowRangeIndex, List<DataSplit>>> indexRangeAndSplits = indexBuilder.scan();
        if (!indexRangeAndSplits.isPresent()) {
            return Collections.emptyList();
        }

        Pair<RowRangeIndex, List<DataSplit>> scanResult = indexRangeAndSplits.get();
        List<DataSplit> splits = splitByContiguousRowRange(scanResult.getRight());
        if (splits.isEmpty()) {
            return Collections.emptyList();
        }

        Map<BinaryRow, Map<Range, List<Split>>> partitionRangeSplits =
                groupSplitsByRange(scanResult.getKey(), splits);
        if (partitionRangeSplits.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> selectedColumns = new ArrayList<>(readType.getFieldNames());

        // Calculate maximum parallelism bound
        boolean withFileMeta = options.get(BTreeIndexOptions.BTREE_WITH_FILE_META);
        long recordsPerRange = options.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE);
        int maxParallelism = options.get(BTreeIndexOptions.BTREE_INDEX_BUILD_MAX_PARALLELISM);

        List<CommitMessage> allMessages = new ArrayList<>();
        List<String> sortColumns = new ArrayList<>();
        sortColumns.add(indexField.name());
        final int partitionKeyNum = table.partitionKeys().size();
        BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        ManifestEntrySerializer manifestSerializer =
                withFileMeta ? new ManifestEntrySerializer() : null;
        for (Map.Entry<BinaryRow, Map<Range, List<Split>>> partitionEntry :
                partitionRangeSplits.entrySet()) {
            for (Map.Entry<Range, List<Split>> entry : partitionEntry.getValue().entrySet()) {
                Range range = entry.getKey();
                List<Split> rangeSplits = entry.getValue();
                if (rangeSplits.isEmpty()) {
                    continue;
                }
                int partitionNum = Math.max((int) (range.count() / recordsPerRange), 1);
                partitionNum = Math.min(partitionNum, maxParallelism);

                // Pre-serialize ManifestEntries for file-meta index (if withFileMeta is enabled)
                List<byte[]> manifestEntryBytesList = null;
                if (withFileMeta && manifestSerializer != null) {
                    manifestEntryBytesList = new ArrayList<>();
                    for (Split split : rangeSplits) {
                        DataSplit dataSplit =
                                split instanceof IndexedSplit
                                        ? ((IndexedSplit) split).dataSplit()
                                        : (split instanceof DataSplit ? (DataSplit) split : null);
                        if (dataSplit == null) {
                            continue;
                        }
                        for (DataFileMeta file : dataSplit.dataFiles()) {
                            ManifestEntry me =
                                    ManifestEntry.create(
                                            FileKind.ADD,
                                            partitionEntry.getKey(),
                                            dataSplit.bucket(),
                                            dataSplit.totalBuckets(),
                                            file);
                            manifestEntryBytesList.add(manifestSerializer.serializeToBytes(me));
                        }
                    }
                }

                Dataset<Row> source =
                        PaimonUtils.createDataset(
                                spark,
                                ScanPlanHelper$.MODULE$.createNewScanPlan(
                                        rangeSplits.toArray(new Split[0]), relation));

                Dataset<Row> selected =
                        source.select(
                                selectedColumns.stream()
                                        .map(functions::col)
                                        .toArray(Column[]::new));

                Column[] sortFields =
                        sortColumns.stream().map(functions::col).toArray(Column[]::new);

                Dataset<Row> partitioned =
                        selected.repartitionByRange(partitionNum, sortFields)
                                .sortWithinPartitions(sortFields);

                final byte[] serializedBuilder = InstantiationUtil.serializeObject(indexBuilder);
                final byte[] partitionBytes =
                        binaryRowSerializer.serializeToBytes(partitionEntry.getKey());
                final byte[] serializedFileMetaBuilder =
                        withFileMeta
                                ? InstantiationUtil.serializeObject(
                                        new BTreeWithFileMetaBuilder(table, indexField))
                                : null;
                final byte[] serializedManifestEntries =
                        (withFileMeta
                                        && manifestEntryBytesList != null
                                        && !manifestEntryBytesList.isEmpty())
                                ? InstantiationUtil.serializeObject(manifestEntryBytesList)
                                : null;
                JavaRDD<byte[]> written =
                        partitioned
                                .javaRDD()
                                .map(row -> (InternalRow) (new SparkRow(readType, row)))
                                .mapPartitions(
                                        (FlatMapFunction<Iterator<InternalRow>, byte[]>)
                                                iter ->
                                                        buildBTreeIndex(
                                                                iter,
                                                                serializedBuilder,
                                                                range,
                                                                partitionKeyNum,
                                                                partitionBytes,
                                                                serializedFileMetaBuilder,
                                                                serializedManifestEntries));
                List<byte[]> commitBytes = written.collect();
                allMessages.addAll(CommitMessageSerializer.deserializeAll(commitBytes));
            }
        }
        return allMessages;
    }

    private static Iterator<byte[]> buildBTreeIndex(
            Iterator<InternalRow> input,
            byte[] serializedBuilder,
            Range range,
            int partitionKeyNum,
            byte[] partitionBytes,
            @Nullable byte[] serializedFileMetaBuilder,
            @Nullable byte[] serializedManifestEntries)
            throws IOException, ClassNotFoundException {
        final BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        BinaryRow partition = binaryRowSerializer.deserializeFromBytes(partitionBytes);
        BTreeGlobalIndexBuilder builder =
                InstantiationUtil.deserializeObject(
                        serializedBuilder, BTreeGlobalIndexBuilder.class.getClassLoader());

        if (serializedFileMetaBuilder != null) {
            BTreeWithFileMetaBuilder fileMetaBuilder =
                    InstantiationUtil.deserializeObject(
                            serializedFileMetaBuilder,
                            BTreeWithFileMetaBuilder.class.getClassLoader());
            List<byte[]> manifestBytes =
                    serializedManifestEntries != null
                            ? InstantiationUtil.<List<byte[]>>deserializeObject(
                                    serializedManifestEntries,
                                    BTreeIndexTopoBuilder.class.getClassLoader())
                            : new ArrayList<>();
            return CommitMessageSerializer.serializeAll(
                            buildWithFileMeta(
                                    builder,
                                    fileMetaBuilder,
                                    range,
                                    partition,
                                    input,
                                    manifestBytes))
                    .iterator();
        }
        return CommitMessageSerializer.serializeAll(
                        builder.buildForSinglePartition(range, partition, input))
                .iterator();
    }

    private static List<CommitMessage> buildWithFileMeta(
            BTreeGlobalIndexBuilder builder,
            BTreeWithFileMetaBuilder fileMetaBuilder,
            Range range,
            BinaryRow partition,
            Iterator<InternalRow> data,
            List<byte[]> manifestEntryBytes)
            throws IOException {
        // Build file-meta index entries from pre-serialized manifest entry bytes
        ManifestEntrySerializer meSerializer = new ManifestEntrySerializer();
        BinaryFileMetaWriter binaryFileMetaWriter = fileMetaBuilder.createBinaryFileMetaWriter();
        for (byte[] bytes : manifestEntryBytes) {
            ManifestEntry entry = meSerializer.deserializeFromBytes(bytes);
            binaryFileMetaWriter.write(entry.file().fileName(), entry);
        }
        List<ResultEntry> binaryFileMetaEntries = binaryFileMetaWriter.finish();

        // Build key index entries, respecting recordsPerRange for splitting (mirrors Flink logic).
        // file-meta entries are only attached to the first flush; subsequent batches carry none to
        // avoid duplicate btree_file_meta SSTs in the index manifest.
        long counter = 0;
        boolean firstFlush = true;
        GlobalIndexParallelWriter currentWriter = null;
        List<CommitMessage> messages = new ArrayList<>();
        InternalRow.FieldGetter indexFieldGetter =
                InternalRow.createFieldGetter(fileMetaBuilder.getIndexField().type(), 0);

        while (data.hasNext()) {
            InternalRow row = data.next();
            if (currentWriter != null && counter >= builder.recordsPerRange()) {
                messages.add(
                        fileMetaBuilder.flushIndex(
                                range,
                                currentWriter.finish(),
                                firstFlush ? binaryFileMetaEntries : Collections.emptyList(),
                                partition));
                firstFlush = false;
                currentWriter = null;
                counter = 0;
            }
            counter++;
            if (currentWriter == null) {
                currentWriter = builder.createWriter();
            }
            long localRowId = row.getLong(1) - range.from;
            currentWriter.write(indexFieldGetter.getFieldOrNull(row), localRowId);
        }

        if (counter > 0) {
            messages.add(
                    fileMetaBuilder.flushIndex(
                            range,
                            currentWriter.finish(),
                            firstFlush ? binaryFileMetaEntries : Collections.emptyList(),
                            partition));
        }
        return messages;
    }
}
