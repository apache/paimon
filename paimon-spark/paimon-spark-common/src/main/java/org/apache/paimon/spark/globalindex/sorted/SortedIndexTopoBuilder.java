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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.globalindex.ScanResult;
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
import org.apache.paimon.types.ResolvedFieldPath;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.groupSplitsByRange;
import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.splitByContiguousRowRange;

/** The {@link GlobalIndexTopologyBuilder} for sorted indexes. */
public class SortedIndexTopoBuilder implements GlobalIndexTopologyBuilder {

    private static final String INDEX_KEY_FIELD = "_SORTED_INDEX_KEY";
    private static final HashSet<String> SUPPORTED_INDEX_TYPES =
            new HashSet<>(Arrays.asList("btree", "bitmap"));

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
        ResolvedFieldPath indexFieldPath =
                ResolvedFieldPath.resolve(table.rowType(), indexField.id()).get();
        SortedGlobalIndexScanner indexScanner =
                new SortedGlobalIndexScanner(table, indexType, options)
                        .withIndexField(indexFieldPath.fullName());
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

        RowType flattenedReadType =
                SpecialFields.rowTypeWithRowId(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                indexField.id(),
                                                INDEX_KEY_FIELD,
                                                indexField.type()))));

        // Calculate maximum parallelism bound
        long recordsPerRange = options.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE);
        int maxParallelism = options.get(SortedIndexOptions.SORTED_INDEX_BUILD_MAX_PARALLELISM);

        List<CommitMessage> allMessages = new ArrayList<>();
        List<String> sortColumns = Collections.singletonList(INDEX_KEY_FIELD);
        final int partitionKeyNum = table.partitionKeys().size();
        BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        SortedGlobalIndexWriter indexWriter =
                new SortedGlobalIndexWriter(table, indexType, options)
                        .withIndexField(indexFieldPath.fullName());
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

                Dataset<Row> source =
                        PaimonUtils.createDataset(
                                spark,
                                ScanPlanHelper$.MODULE$.createNewScanPlan(
                                        rangeSplits.toArray(new Split[0]), relation));

                Dataset<Row> selected =
                        source.select(
                                indexColumn(indexFieldPath).alias(INDEX_KEY_FIELD),
                                quotedColumn(SpecialFields.ROW_ID.name()));

                Column[] sortFields =
                        sortColumns.stream()
                                .map(SortedIndexTopoBuilder::quotedColumn)
                                .toArray(Column[]::new);

                Dataset<Row> partitioned =
                        selected.repartitionByRange(partitionNum, sortFields)
                                .sortWithinPartitions(sortFields);

                final byte[] serializedWriter = InstantiationUtil.serializeObject(indexWriter);
                final byte[] partitionBytes =
                        binaryRowSerializer.serializeToBytes(partitionEntry.getKey());
                JavaRDD<byte[]> written =
                        partitioned
                                .javaRDD()
                                .map(row -> (InternalRow) (new SparkRow(flattenedReadType, row)))
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

    static Column indexColumn(ResolvedFieldPath fieldPath) {
        List<String> fieldNames = fieldPath.fieldNames();
        Column column = quotedColumn(fieldNames.get(0));
        for (int i = 1; i < fieldNames.size(); i++) {
            column = column.getField(fieldNames.get(i));
        }
        return column;
    }

    private static Column quotedColumn(String fieldName) {
        return functions.col("`" + fieldName.replace("`", "``") + "`");
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
}
