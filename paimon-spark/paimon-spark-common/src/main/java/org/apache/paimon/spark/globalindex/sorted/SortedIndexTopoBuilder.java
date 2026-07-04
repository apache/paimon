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
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder;
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder.groupSplitsByRange;
import static org.apache.paimon.globalindex.sorted.SortedGlobalIndexBuilder.splitByContiguousRowRange;

/** The {@link GlobalIndexTopologyBuilder} for sorted indexes. */
public class SortedIndexTopoBuilder implements GlobalIndexTopologyBuilder {

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
        SortedGlobalIndexBuilder indexBuilder =
                new SortedGlobalIndexBuilder(table, indexType, options)
                        .withIndexField(indexField.name());
        if (partitionPredicate != null) {
            indexBuilder = indexBuilder.withPartitionPredicate(partitionPredicate);
        }

        Optional<Pair<RowRangeIndex, List<DataSplit>>> indexRangeAndSplits =
                indexBuilder.incrementalScan();
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
        long recordsPerRange = options.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE);
        int maxParallelism = options.get(SortedIndexOptions.SORTED_INDEX_BUILD_MAX_PARALLELISM);

        List<CommitMessage> allMessages = new ArrayList<>();
        List<String> sortColumns = new ArrayList<>();
        sortColumns.add(indexField.name());
        final int partitionKeyNum = table.partitionKeys().size();
        BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
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
                JavaRDD<byte[]> written =
                        partitioned
                                .javaRDD()
                                .map(row -> (InternalRow) (new SparkRow(readType, row)))
                                .mapPartitions(
                                        (FlatMapFunction<Iterator<InternalRow>, byte[]>)
                                                iter ->
                                                        buildSortedIndex(
                                                                iter,
                                                                serializedBuilder,
                                                                range,
                                                                partitionKeyNum,
                                                                partitionBytes));
                List<byte[]> commitBytes = written.collect();
                allMessages.addAll(CommitMessageSerializer.deserializeAll(commitBytes));
            }
        }
        return allMessages;
    }

    private static Iterator<byte[]> buildSortedIndex(
            Iterator<InternalRow> input,
            byte[] serializedBuilder,
            Range range,
            int partitionKeyNum,
            byte[] partitionBytes)
            throws IOException, ClassNotFoundException {
        final BinaryRowSerializer binaryRowSerializer = new BinaryRowSerializer(partitionKeyNum);
        BinaryRow partition = binaryRowSerializer.deserializeFromBytes(partitionBytes);
        SortedGlobalIndexBuilder builder =
                InstantiationUtil.deserializeObject(
                        serializedBuilder, SortedGlobalIndexBuilder.class.getClassLoader());
        return CommitMessageSerializer.serializeAll(
                        builder.buildForSinglePartition(range, partition, input))
                .iterator();
    }
}
