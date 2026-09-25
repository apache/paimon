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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.source.assigners.DynamicPartitionPruningAssigner;
import org.apache.paimon.flink.source.assigners.FIFOSplitAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.globalindex.IndexQuerySplit;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.DataEvolutionTestBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests reader-side index queries, assignment and position recovery in the Flink source. */
public class IndexQuerySourceTest extends DataEvolutionTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testDistributedIndexAndDedicatedSplitGeneration() throws Exception {
        FileStoreTable table = indexedTable();
        assertThat(plan(table, FlinkConnectorOptions.SplitAssignMode.FAIR).get(0).split())
                .isInstanceOf(IndexedSplit.class);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "true");
        for (FlinkConnectorOptions.SplitAssignMode mode :
                FlinkConnectorOptions.SplitAssignMode.values()) {
            assertThat(plan(table.copy(options), mode).get(0).split())
                    .isInstanceOf(IndexQuerySplit.class);
        }
        options.put(CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "false");
        assertThat(
                        plan(table.copy(options), FlinkConnectorOptions.SplitAssignMode.FAIR)
                                .get(0)
                                .split())
                .isInstanceOf(IndexedSplit.class);
        options.put(CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "true");
        options.put("scan.dedicated-split-generation", "true");
        FileStoreTable enabled = table.copy(options);
        DataStream<RowData> stream =
                buildSource(enabled, FlinkConnectorOptions.SplitAssignMode.FAIR);
        SourceTransformation<Split, SimpleSourceSplit, ?> transformation =
                (SourceTransformation<Split, SimpleSourceSplit, ?>)
                        stream.getTransformation().getTransitivePredecessors().stream()
                                .filter(value -> value instanceof SourceTransformation)
                                .findFirst()
                                .get();
        List<Split> splits = new ArrayList<>();
        ReaderOutput<Split> output = mock(ReaderOutput.class);
        doAnswer(
                        invocation -> {
                            splits.add(invocation.getArgument(0));
                            return null;
                        })
                .when(output)
                .collect(any(Split.class));
        try (SourceReader<Split, SimpleSourceSplit> reader =
                transformation.getSource().createReader(mock(SourceReaderContext.class))) {
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
        }
        assertThat(splits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);
        assertThat(enabled.options())
                .containsEntry(CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "true");
    }

    @Test
    public void testReaderRestoresIndexQuerySplitAndPosition() throws Exception {
        FileStoreTable table = distributedTable(indexedTable());
        FileStoreSourceSplit split = plan(table, FlinkConnectorOptions.SplitAssignMode.FAIR).get(0);
        List<Integer> complete = readSplit(readBuilder(table).newRead().executeFilter(), split);
        assertThat(complete).hasSize(19);
        FileStoreSourceSplit checkpoint = split.updateWithRecordsToSkip(7);
        FileStoreSourceSplitSerializer serializer = new FileStoreSourceSplitSerializer();
        FileStoreSourceSplit restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));
        assertThat(restored).isEqualTo(checkpoint);
        assertThat(restored.split()).isInstanceOf(IndexQuerySplit.class);
        assertThat(readSplit(readBuilder(table).newRead().executeFilter(), restored))
                .containsExactlyElementsOf(complete.subList(7, complete.size()));
    }

    @Test
    public void testWrappedIndexQuerySplitSupportsPartitionFiltering() throws Exception {
        FileStoreTable table = distributedTable(indexedTable());
        FileStoreSourceSplit indexQuery =
                plan(table, FlinkConnectorOptions.SplitAssignMode.FAIR).get(0);
        FileStoreSourceSplit wrapped =
                new FileStoreSourceSplit(
                        indexQuery.splitId(),
                        new QueryAuthSplit(
                                indexQuery.split(), new TableQueryAuthResult(null, null)));
        DynamicFilteringData filtering = mock(DynamicFilteringData.class);
        when(filtering.contains(any(RowData.class))).thenReturn(true);
        DynamicPartitionFilteringInfo filteringInfo =
                new DynamicPartitionFilteringInfo(
                        table.schema().logicalPartitionType(), Collections.emptyList());
        for (boolean fair : new boolean[] {false, true}) {
            SplitAssigner assigner =
                    fair
                            ? new PreAssignSplitAssigner(1, 1, Collections.singletonList(wrapped))
                                    .ofDynamicPartitionPruning(filteringInfo, filtering)
                            : new DynamicPartitionPruningAssigner(
                                    new FIFOSplitAssigner(Collections.singletonList(wrapped)),
                                    filteringInfo,
                                    filtering);
            assertThat(assigner.getNext(0, null)).containsExactly(wrapped);
        }
    }

    private FileStoreTable distributedTable(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(
                        CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "true"));
    }

    private FileStoreTable indexedTable() throws Exception {
        write(100);
        FileStoreTable table = getTableDefault();
        List<CommitMessage> commits = new ArrayList<>();
        for (String field : new String[] {"f1", "f2"}) {
            SortedGlobalIndexScanner builder =
                    new SortedGlobalIndexScanner(table, "btree").withIndexField(field);
            for (DataSplit split : builder.scan().get().entries()) {
                commits.addAll(
                        SortedGlobalIndexTestUtils.buildIndex(
                                table, "btree", field, split, table.latestSnapshot().get().id()));
            }
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commits);
        }
        return getTableDefault();
    }

    private ReadBuilder readBuilder(FileStoreTable table) {
        return table.newReadBuilder().withFilter(predicate(table));
    }

    private Predicate predicate(FileStoreTable table) {
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        return PredicateBuilder.and(
                b.contains(1, BinaryString.fromString("5")),
                b.startsWith(2, BinaryString.fromString("b")));
    }

    private DataStream<RowData> buildSource(
            FileStoreTable table, FlinkConnectorOptions.SplitAssignMode mode) {
        return new FlinkSourceBuilder(
                        table.copy(
                                Collections.singletonMap(
                                        FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE
                                                .key(),
                                        mode.name())))
                .env(StreamExecutionEnvironment.getExecutionEnvironment())
                .sourceBounded(true)
                .predicate(predicate(table))
                .build();
    }

    private List<FileStoreSourceSplit> plan(
            FileStoreTable table, FlinkConnectorOptions.SplitAssignMode mode) throws Exception {
        SplitEnumeratorContext<FileStoreSourceSplit> context = mock(SplitEnumeratorContext.class);
        when(context.currentParallelism()).thenReturn(1);
        Source<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint> source =
                ((SourceTransformation<RowData, FileStoreSourceSplit, PendingSplitsCheckpoint>)
                                buildSource(table, mode).getTransformation())
                        .getSource();
        try (SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> enumerator =
                source.restoreEnumerator(context, null)) {
            return new ArrayList<>(enumerator.snapshotState(1).splits());
        }
    }

    private List<Integer> readSplit(TableRead read, FileStoreSourceSplit split) throws Exception {
        List<Integer> result = new ArrayList<>();
        try (FileStoreSourceSplitReader reader =
                new FileStoreSourceSplitReader(
                        read,
                        null,
                        new FileStoreSourceReaderMetrics(
                                new FileStoreSourceReaderTest.DummyMetricGroup()),
                        null,
                        false)) {
            reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
            boolean finished = false;
            while (!finished) {
                RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>> records = reader.fetch();
                String id;
                while ((id = records.nextSplit()) != null) {
                    BulkFormat.RecordIterator<RowData> batch;
                    while ((batch = records.nextRecordFromSplit()) != null) {
                        RecordAndPosition<RowData> row;
                        while ((row = batch.next()) != null) {
                            result.add(row.getRecord().getInt(0));
                        }
                    }
                }
                finished = records.finishedSplits().contains(split.splitId());
                records.recycle();
            }
        }
        return result;
    }
}
