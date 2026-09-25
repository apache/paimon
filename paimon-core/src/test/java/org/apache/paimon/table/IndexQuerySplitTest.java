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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.DataEvolutionBatchScan;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.IndexQuerySplit;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.fmindex.FMGlobalIndexOptions;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.DataEvolutionUtils;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** End-to-end tests of deferred index plans, data reads and recovery. */
public class IndexQuerySplitTest extends DataEvolutionTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    public void testEmptyIndexResultWithoutDataStats(String indexType) throws Exception {
        Schema schema = schemaDefault();
        Map<String, String> options = new HashMap<>(schema.options());
        options.put(CoreOptions.METADATA_STATS_MODE.key(), "none");
        catalog.createTable(
                identifier(),
                new Schema(
                        schema.fields(),
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment()),
                false);
        appendRows(0, 100);
        createIndex(indexType, "f0");
        FileStoreTable table = distributedTable(smallSplits(getTableDefault()));
        Predicate predicate = new PredicateBuilder(table.rowType()).equal(0, 200);
        assertThat(
                        table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.GLOBAL_INDEX_ENABLED.key(), "false"))
                                .newReadBuilder()
                                .withFilter(predicate)
                                .newScan()
                                .plan()
                                .splits())
                .isNotEmpty();
        ReadBuilder read = table.newReadBuilder().withFilter(predicate);
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).isEmpty();
        assertThat(read(read, splits)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    public void testDetailReusesPlannedDataRanges(String indexType) throws Exception {
        write(100);
        createIndex(indexType, "f1");
        appendRows(100, 200);
        FileStoreTable original =
                distributedTable(smallSplits(getTableDefault()))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "detail"));
        SnapshotReader snapshotReader = spy(original.newSnapshotReader());
        FileStoreTable table = spy(original);
        Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, str("a150"));
        DataEvolutionBatchScan scan =
                (DataEvolutionBatchScan) table.newScan(ignored -> snapshotReader);
        scan.withFilter(predicate);
        List<Split> splits = scan.plan().splits();
        verify(snapshotReader, times(1)).read();
        verify(table, never()).newSnapshotReader();
        assertThat(read(table.newReadBuilder().withFilter(predicate), splits)).containsExactly(150);
    }

    @ParameterizedTest
    @CsvSource({
        "btree,fast",
        "btree,full",
        "btree,detail",
        "bitmap,fast",
        "bitmap,full",
        "bitmap,detail"
    })
    public void testMetadataEmptySplitsRetainUnindexedTail(String indexType, String mode)
            throws Exception {
        write(100);
        createIndex(indexType, "f0");
        createIndex(indexType, "f1");
        FileStoreTable table =
                distributedTable(smallSplits(getTableDefault()))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), mode));
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate miss = b.equal(0, 150);
        assertThat(table.newReadBuilder().withFilter(miss).newScan().plan().splits()).isEmpty();
        appendRows(100, 200);
        for (Predicate predicate :
                Arrays.asList(
                        miss,
                        PredicateBuilder.and(miss, b.startsWith(1, str("a"))),
                        PredicateBuilder.or(miss, b.equal(1, str("missing"))))) {
            ReadBuilder read = table.newReadBuilder().withFilter(predicate);
            List<Split> splits = read.newScan().plan().splits();
            assertThat(splits).hasSize(mode.equals("fast") ? 0 : 1);
            assertThat(read(read, splits))
                    .containsExactlyElementsOf(
                            mode.equals("fast")
                                    ? Collections.emptyList()
                                    : Collections.singletonList(150));
        }
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(PredicateBuilder.or(miss, b.equal(1, str("a50"))));
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).hasSize(mode.equals("fast") ? 1 : 2);
        assertThat(read(read, splits))
                .containsExactlyElementsOf(
                        mode.equals("fast")
                                ? Collections.singletonList(50)
                                : Arrays.asList(50, 150));

        Predicate tail = b.equal(1, str("a150"));
        read = table.newReadBuilder().withFilter(tail);
        List<Integer> result = read(read, read.newScan().plan().splits());
        if (mode.equals("fast")) {
            assertThat(result).isEmpty();
        } else {
            assertThat(result).containsExactly(150);
        }
    }

    private FileStoreTable distributedTable(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(
                        CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "true"));
    }

    @Test
    public void testPlanSelectsDistributedIndex() throws Exception {
        write(100);
        createIndex("btree", "f1");
        FileStoreTable table = getTableDefault();
        Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, str("a50"));
        assertThat(table.newReadBuilder().withFilter(predicate).newScan().plan().splits())
                .isNotEmpty()
                .allMatch(IndexedSplit.class::isInstance);
        FileStoreTable enabled = distributedTable(table);
        ReadBuilder read = enabled.newReadBuilder().withFilter(predicate);
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);
        assertThat(read(read, splits)).containsExactly(50);
        assertThat(enabled.newScan().plan().splits())
                .isNotEmpty()
                .allMatch(DataSplit.class::isInstance);
        assertThat(
                        enabled.copy(
                                        Collections.singletonMap(
                                                CoreOptions.GLOBAL_INDEX_ENABLED.key(), "false"))
                                .newReadBuilder()
                                .withFilter(predicate)
                                .newScan()
                                .plan()
                                .splits())
                .isNotEmpty()
                .allMatch(DataSplit.class::isInstance);
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    public void testResultsAcrossModesAndPredicates(String indexType) throws Exception {
        write(100);
        appendRows(100, 200);
        createIndex(indexType, "f1");
        appendRows(200, 300);
        createIndex(indexType, "f2");
        FileStoreTable table = smallSplits(getTableDefault());
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate f1 = b.in(1, Arrays.asList(str("a50"), str("a150"), str("a250")));
        List<Predicate> predicates =
                Arrays.asList(
                        f1,
                        PredicateBuilder.and(f1, b.startsWith(2, str("b"))),
                        PredicateBuilder.or(f1, b.equal(2, str("b251"))),
                        PredicateBuilder.and(f1, b.greaterThan(0, 40)),
                        PredicateBuilder.or(f1, b.greaterThan(0, 250)),
                        b.equal(1, str("absent")),
                        b.between(1, str("a100"), str("a299")),
                        b.contains(1, str("5")),
                        b.like(1, str("a_5%")),
                        b.notIn(1, Arrays.asList(str("a50"), null)),
                        PredicateBuilder.and(b.isNotNull(1), f1));
        for (String mode : Arrays.asList("fast", "full", "detail")) {
            FileStoreTable configured =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), mode));
            for (Predicate predicate : predicates) {
                ReadBuilder read = configured.newReadBuilder().withFilter(predicate);
                TableScan.Plan indexQueryPlan =
                        distributedTable(configured)
                                .newReadBuilder()
                                .withFilter(predicate)
                                .newScan()
                                .plan();
                List<Integer> actual = read(read, indexQueryPlan.splits());
                assertThat(actual)
                        .as("%s / %s / %s", indexType, mode, predicate)
                        .containsExactlyInAnyOrderElementsOf(
                                read(read, read.newScan().plan().splits()));
                if (!mode.equals("fast")) {
                    ReadBuilder full =
                            configured
                                    .copy(
                                            Collections.singletonMap(
                                                    CoreOptions.GLOBAL_INDEX_ENABLED.key(),
                                                    "false"))
                                    .newReadBuilder()
                                    .withFilter(predicate);
                    assertThat(actual)
                            .containsExactlyInAnyOrderElementsOf(
                                    read(full, full.newScan().plan().splits()));
                }
            }
        }
        ReadBuilder read =
                distributedTable(table).newReadBuilder().withFilter(b.startsWith(1, str("a")));
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).hasSize(2).allMatch(split -> split instanceof IndexQuerySplit);
        // The first range still has both files needed for column merging.
        assertThat(((IndexQuerySplit) splits.get(0)).dataSplit().dataFiles()).hasSize(2);
        assertThat(read(read, splits)).hasSize(200);
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    public void testPlanningDoesNotOpenIndexesAndRestoredSplitUsesPinnedFiles(String indexType)
            throws Exception {
        write(100);
        appendRows(100, 200);
        createIndex(indexType, "f1");
        createIndex(indexType, "f2");
        FileStoreTable original = smallSplits(getTableDefault());
        Set<Path> indexPaths =
                indexFiles(original).stream()
                        .map(
                                file ->
                                        original.store()
                                                .pathFactory()
                                                .globalIndexFileFactory()
                                                .toPath(file))
                        .collect(Collectors.toSet());
        AtomicBoolean allowRead = new AtomicBoolean(false);
        AtomicInteger opens = new AtomicInteger();
        FileIO fileIO = spy(original.fileIO());
        doAnswer(
                        invocation -> {
                            Path path = invocation.getArgument(0);
                            if (indexPaths.contains(path)) {
                                opens.incrementAndGet();
                                if (!allowRead.get()) {
                                    throw new IOException("Index unavailable for test: " + path);
                                }
                            }
                            return invocation.callRealMethod();
                        })
                .when(fileIO)
                .newInputStream(any(Path.class));
        FileStoreTable table =
                distributedTable(
                        new AppendOnlyFileStoreTable(
                                fileIO, original.location(), original.schema()));
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate predicate =
                PredicateBuilder.and(b.startsWith(1, str("a")), b.startsWith(2, str("b")));
        ReadBuilder read = table.newReadBuilder().withFilter(predicate);
        List<Split> splits = read.newScan().plan().splits();
        assertThat(opens).hasValue(0);
        List<Split> restored = new ArrayList<>();
        for (Split split : splits) {
            assertThat(split).isInstanceOf(IndexQuerySplit.class);
            Split binary = SplitSerializer.deserialize(SplitSerializer.serialize(split));
            assertThat(binary).isEqualTo(split);
            Split java =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(split), getClass().getClassLoader());
            assertThat(java).isEqualTo(split);
            restored.add(binary);
        }
        appendRows(200, 300);
        List<DataSplit> tail =
                smallSplits(getTableDefault()).newScan().plan().splits().stream()
                        .map(split -> (DataSplit) split)
                        .filter(
                                split ->
                                        split.dataFiles().stream()
                                                .allMatch(
                                                        file ->
                                                                file.nonNullRowIdRange().from
                                                                        >= 200))
                        .collect(Collectors.toList());
        createIndex(indexType, "f1", tail);
        allowRead.set(true);
        assertThat(read(read, restored))
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 200)
                                .boxed()
                                .collect(Collectors.toList()));
        assertThat(opens.get()).isGreaterThan(0);
        // Re-evaluation preserves the sequence on which Flink's recordsToSkip is based.
        assertThat(read(read, restored)).containsExactlyElementsOf(read(read, splits));
        allowRead.set(false);
        assertThatThrownBy(() -> read(read, restored))
                .hasStackTraceContaining("Index unavailable for test");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testAuthAndReadProtectionKeepExistingPath(boolean masking) throws Exception {
        write(100);
        createIndex("btree", "f1");
        FileStoreTable original = getTableDefault();
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.QUERY_AUTH_ENABLED.key(), "true");
        options.put(CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), "true");
        options.put(CoreOptions.SCAN_PLAN_AUTO_TAG_FOR_READ_TIME_RETAINED.key(), "1 h");
        PredicateBuilder b = new PredicateBuilder(original.rowType());
        TableQueryAuthResult auth =
                new TableQueryAuthResult(
                        Collections.singletonList(JsonSerdeUtil.toJson(b.greaterThan(0, 49))),
                        masking
                                ? Collections.singletonMap(
                                        "f1",
                                        JsonSerdeUtil.toFlatJson(
                                                new ConcatTransform(
                                                        Collections.singletonList(str("masked")))))
                                : null);
        CatalogEnvironment environment = spy(CatalogEnvironment.empty());
        AtomicInteger authCalls = new AtomicInteger();
        TableQueryAuth queryAuth =
                select -> {
                    authCalls.incrementAndGet();
                    return auth;
                };
        doReturn(queryAuth).when(environment).tableQueryAuth(any(CoreOptions.class));
        FileStoreTable table =
                new AppendOnlyFileStoreTable(
                        original.fileIO(),
                        original.location(),
                        original.copy(options).schema(),
                        environment);
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(
                                masking ? b.equal(1, str("masked")) : b.startsWith(1, str("a")));
        List<Split> splits = read.newScan().plan().splits();
        assertThat(authCalls).hasValue(1);
        assertThat(splits)
                .allMatch(
                        split ->
                                split instanceof QueryAuthSplit
                                        && ((QueryAuthSplit) split).split() instanceof DataSplit);
        assertThat(table.tagManager().allTagNames())
                .anyMatch(name -> name.startsWith("batch-read-"));
        List<Split> restored = new ArrayList<>();
        for (Split split : splits) {
            restored.add(SplitSerializer.deserialize(SplitSerializer.serialize(split)));
        }
        assertThat(read(read, restored))
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(50, 100)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    @ParameterizedTest
    @CsvSource({"btree,full", "btree,detail", "bitmap,full", "bitmap,detail"})
    public void testUnindexedResidualDoesNotUndoIndexPruning(String indexType, String mode)
            throws Exception {
        write(100);
        createIndex(indexType, "f1");
        FileStoreTable table =
                getTableDefault()
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), mode));
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate indexed = b.equal(1, str("a42"));
        List<Predicate> predicates =
                Arrays.asList(
                        PredicateBuilder.and(indexed, b.equal(2, str("b42"))),
                        PredicateBuilder.and(
                                indexed,
                                PredicateBuilder.or(
                                        b.equal(1, str("a42")), b.equal(2, str("b42")))),
                        PredicateBuilder.and(b.equal(1, str("a42x")), b.equal(2, str("b42"))));
        for (Predicate predicate : predicates) {
            ReadBuilder read = table.newReadBuilder().withFilter(predicate);
            List<Split> splits =
                    distributedTable(table)
                            .newReadBuilder()
                            .withFilter(predicate)
                            .newScan()
                            .plan()
                            .splits();
            assertThat(splits).allMatch(IndexQuerySplit.class::isInstance);
            List<Range> candidates = new ArrayList<>();
            for (Split split : splits) {
                candidates.addAll(((IndexQuerySplit) split).evaluate(table.fileIO()).rowRanges());
            }
            if (predicate == predicates.get(2)) {
                assertThat(candidates).isEmpty();
            } else {
                assertThat(splits).isNotEmpty();
                assertThat(candidates).containsExactly(new Range(42, 42));
            }
            assertThat(read(read, splits))
                    .containsExactlyElementsOf(read(read, read.newScan().plan().splits()));
        }
    }

    @Test
    public void testPrecomputedRangesAndUnsupportedPredicateKeepExistingPath() throws Exception {
        write(100);
        createIndex("btree", "f1");
        FileStoreTable table = distributedTable(getTableDefault());
        ReadBuilder read =
                table.newReadBuilder().withRowRanges(Collections.singletonList(new Range(5, 9)));
        assertThat(read.newScan().plan().splits()).allMatch(split -> split instanceof IndexedSplit);
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate unsupportedOr = PredicateBuilder.or(b.equal(1, str("a1")), b.equal(0, 99));
        read = table.newReadBuilder().withFilter(unsupportedOr);
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).allMatch(split -> split instanceof DataSplit);
        assertThat(read(read, splits)).containsExactly(1, 99);
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    public void testBudgetFailureIsDeferredToReader(String indexType) throws Exception {
        write(10);
        appendRows(10, 1000);
        FileStoreTable table = smallSplits(getTableDefault());
        List<DataSplit> dataSplits =
                table.newScan().plan().splits().stream()
                        .map(split -> (DataSplit) split)
                        .collect(Collectors.toList());
        createIndex(indexType, "f1", dataSplits);
        List<IndexFileMeta> files = indexFiles(getTableDefault());
        Map<Range, Long> sizes = new HashMap<>();
        for (IndexFileMeta file : files) {
            Range range =
                    new Range(
                            file.globalIndexMeta().rowRangeStart(),
                            file.globalIndexMeta().rowRangeEnd());
            sizes.merge(range, file.fileSize(), Long::sum);
        }
        assertThat(sizes).hasSize(2);
        long min = Collections.min(sizes.values());
        long max = Collections.max(sizes.values());
        assertThat(max).isGreaterThan(min);
        String budgetKey = indexType + "-index.fallback-scan-max-size";
        table = table.copy(Collections.singletonMap(budgetKey, min + " b"));
        Predicate predicate = new PredicateBuilder(table.rowType()).contains(1, str("5"));
        ReadBuilder read = table.newReadBuilder().withFilter(predicate);
        ReadBuilder indexQueryRead = distributedTable(table).newReadBuilder().withFilter(predicate);
        List<Split> splits = indexQueryRead.newScan().plan().splits();
        assertThat(splits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);
        assertThat(read(read, read.newScan().plan().splits())).isNotEmpty();
        assertThatThrownBy(() -> read(indexQueryRead, splits)).isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"fast", "full", "detail"})
    public void testMissingDeferredIndexFileFails(String mode) throws Exception {
        write(100);
        createIndex("btree", "f1");
        FileStoreTable table =
                distributedTable(getTableDefault())
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), mode));
        Predicate predicate = new PredicateBuilder(table.rowType()).equal(1, str("a50"));
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(predicate)
                        .withReadType(table.rowType().project(new int[] {0}));
        DataEvolutionBatchScan scan = (DataEvolutionBatchScan) read.newScan();
        List<Split> splits = scan.plan().splits();
        assertThat(splits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);

        for (IndexFileMeta file : indexFiles(table)) {
            table.fileIO()
                    .delete(
                            table.store().pathFactory().globalIndexFileFactory().toPath(file),
                            false);
        }
        assertThatThrownBy(() -> read(read, splits)).isInstanceOf(IOException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"full", "detail"})
    public void testRestoreAfterIndexLossFailsBeforeSkippingRecords(String mode) throws Exception {
        write(100);
        createIndex("btree", "f1");
        appendRows(100, 200);
        FileStoreTable table =
                distributedTable(getTableDefault())
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), mode));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate predicate =
                PredicateBuilder.and(
                        builder.startsWith(1, str("a")),
                        PredicateBuilder.or(
                                builder.equal(2, str("b50")), builder.equal(2, str("b150"))));
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(predicate)
                        .withReadType(table.rowType().project(new int[] {0}));
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).hasSize(1).allMatch(IndexQuerySplit.class::isInstance);

        // Flink checkpoints the split with recordsToSkip=1 after emitting its first candidate.
        try (CloseableIterator<InternalRow> records =
                read.newRead().createReader(() -> splits).toCloseableIterator()) {
            assertThat(records.next()).isNotNull();
        }

        Split restored = SplitSerializer.deserialize(SplitSerializer.serialize(splits.get(0)));
        for (IndexFileMeta file : indexFiles(table)) {
            table.fileIO()
                    .delete(
                            table.store().pathFactory().globalIndexFileFactory().toPath(file),
                            false);
        }
        assertThatThrownBy(
                        () -> {
                            try (RecordReader<InternalRow> reader =
                                    read.newRead()
                                            .createReader(
                                                    () -> Collections.singletonList(restored))) {
                                reader.readBatch();
                            }
                        })
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testTaggedSnapshotRemainsPinnedWhenSnapshotFileIsExpired() throws Exception {
        write(100);
        createIndex("btree", "f1");
        FileStoreTable table = getTableDefault();
        long snapshotId = table.latestSnapshot().get().id();
        table.createTag("index-query-read", snapshotId);
        appendRows(100, 200);
        table.snapshotManager().deleteSnapshot(snapshotId);
        table =
                distributedTable(getTableDefault())
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_TAG_NAME.key(), "index-query-read"));
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).startsWith(1, str("a")));
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits)
                .allMatch(
                        split ->
                                split instanceof IndexQuerySplit
                                        && ((IndexQuerySplit) split).dataSplit().snapshotId()
                                                == snapshotId);
        assertThat(read(read, splits))
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 100)
                                .boxed()
                                .collect(Collectors.toList()));

        FileStoreTable taggedTable = table;
        long latestSnapshotId = getTableDefault().latestSnapshot().get().id();
        SnapshotReader snapshotReader = spy(table.newSnapshotReader());
        doAnswer(
                        invocation -> {
                            SnapshotReader.Plan plan =
                                    (SnapshotReader.Plan) invocation.callRealMethod();
                            assertThat(plan.snapshot()).isNotNull();
                            assertThat(plan.snapshot().id()).isEqualTo(snapshotId);
                            taggedTable.replaceTag("index-query-read", latestSnapshotId, null);
                            return plan;
                        })
                .when(snapshotReader)
                .read();
        DataEvolutionBatchScan scan =
                (DataEvolutionBatchScan) table.newScan(ignored -> snapshotReader);
        scan.withFilter(new PredicateBuilder(table.rowType()).startsWith(1, str("a")));
        List<Split> plannedSplits = scan.plan().splits();
        assertThat(plannedSplits)
                .allMatch(
                        split ->
                                split instanceof IndexQuerySplit
                                        && ((IndexQuerySplit) split).dataSplit().snapshotId()
                                                == snapshotId);
        assertThat(read(read, plannedSplits))
                .containsExactlyElementsOf(
                        java.util.stream.IntStream.range(0, 100)
                                .boxed()
                                .collect(Collectors.toList()));
    }

    @ParameterizedTest
    @CsvSource({"btree,false", "btree,true", "bitmap,false", "bitmap,true"})
    public void testPartitionIndexDoesNotPruneIndexedTail(String indexType, boolean topN)
            throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .partitionKeys("pt")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = getTableDefault();
        for (int i = 0; i < 2; i++) {
            try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
                    BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
                write.write(GenericRow.of(i, str("a" + i), str("p")));
                commit.commit(write.prepareCommit());
            }
        }
        table = smallSplits(getTableDefault());
        List<Split> dataSplits = table.newScan().plan().splits();
        assertThat(dataSplits).hasSize(2);
        createIndex(indexType, "pt", Collections.singletonList((DataSplit) dataSplits.get(1)));
        table =
                smallSplits(getTableDefault())
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), "fast"));
        ReadBuilder read =
                table.newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).equal(2, str("p")));
        ReadBuilder indexQueryRead =
                distributedTable(table)
                        .newReadBuilder()
                        .withFilter(new PredicateBuilder(table.rowType()).equal(2, str("p")));
        if (topN) {
            TopN order =
                    new TopN(
                            new FieldRef(0, "f0", table.rowType().getTypeAt(0)),
                            SortValue.SortDirection.ASCENDING,
                            SortValue.NullOrdering.NULLS_FIRST,
                            1);
            read.withTopN(order);
            indexQueryRead.withTopN(order);
        } else {
            read.withLimit(1);
            indexQueryRead.withLimit(1);
        }
        List<Integer> expected = read(read, read.newScan().plan().splits());
        // The current TopN path ranks f0, whose index is absent, and falls back to data TopN.
        assertThat(expected).containsExactly(topN ? 0 : 1);
        assertThat(read(indexQueryRead, indexQueryRead.newScan().plan().splits()))
                .containsExactlyElementsOf(expected);
    }

    @Test
    public void testIndexQueryCandidatesStillApplyDeletionVectors() throws Exception {
        Schema schema = schemaDefault();
        Map<String, String> options = new HashMap<>(schema.options());
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        catalog.createTable(
                identifier(),
                new Schema(
                        schema.fields(),
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment()),
                false);
        appendRows(0, 100);
        createIndex("btree", "f1");
        FileStoreTable table = getTableDefault();
        List<DataFileMeta> files = ((DataSplit) table.newScan().plan().splits().get(0)).dataFiles();
        String anchor = DataEvolutionUtils.retrieveAnchorFile(files, file -> file).fileName();
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        DeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(50);
        maintainer.notifyNewDeletionVector(anchor, deletionVector);
        List<IndexFileMeta> deletionFiles = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                deletionFiles.add(entry.indexFile());
            }
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    BucketMode.UNAWARE_BUCKET,
                                    null,
                                    new DataIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            deletionFiles,
                                            Collections.emptyList()),
                                    CompactIncrement.emptyIncrement())));
        }
        ReadBuilder read =
                distributedTable(table)
                        .newReadBuilder()
                        .withFilter(
                                new PredicateBuilder(table.rowType())
                                        .in(1, Arrays.asList(str("a50"), str("a51"))));
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).allMatch(split -> split instanceof IndexQuerySplit);
        assertThat(read(read, splits)).containsExactly(51);
    }

    private FileStoreTable smallSplits(FileStoreTable table) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 b");
        options.put(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(), "1 b");
        return table.copy(options);
    }

    @Test
    public void testFMFallbackAndMixedDistributedIndex() throws Exception {
        write(100);
        FileStoreTable table = getTableDefault();
        GlobalIndexFileReadWrite io =
                new GlobalIndexFileReadWrite(
                        table.fileIO(), table.store().pathFactory().globalIndexFileFactory());
        Options indexOptions = new Options();
        indexOptions.set(FMGlobalIndexOptions.SA_SAMPLE_RATE, 1);
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter)
                        GlobalIndexer.create("fm", table.rowType().getField("f1"), indexOptions)
                                .createWriter(io);
        for (int i = 0; i < 100; i++) {
            writer.write(str("a" + i), i);
        }
        List<IndexFileMeta> indexes = new ArrayList<>();
        for (ResultEntry entry : writer.finish()) {
            indexes.add(
                    new IndexFileMeta(
                            "fm",
                            entry.fileName(),
                            io.fileSize(entry.fileName()),
                            entry.rowCount(),
                            new GlobalIndexMeta(
                                    0, 99, table.rowType().getField("f1").id(), null, entry.meta()),
                            null));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    BucketMode.UNAWARE_BUCKET,
                                    null,
                                    DataIncrement.indexIncrement(indexes),
                                    CompactIncrement.emptyIncrement())));
        }
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), "full");
        options.put(FMGlobalIndexOptions.LOCATE_COST_RATIO.key(), "1");
        table = distributedTable(table.copy(options));
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate predicate =
                PredicateBuilder.and(
                        b.contains(1, str("5")),
                        b.contains(1, str("1")),
                        b.startsWith(2, str("b")));
        ReadBuilder read = table.newReadBuilder().withFilter(predicate);
        List<Split> fmSplits = read.newScan().plan().splits();
        assertThat(fmSplits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);
        assertThat(read(read, fmSplits)).containsExactly(15, 51);

        createIndex("btree", "f2");
        List<Split> splits = read.newScan().plan().splits();
        assertThat(splits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);
        assertThat(read(read, splits)).containsExactly(15, 51);

        ReadBuilder orRead =
                table.newReadBuilder()
                        .withFilter(
                                PredicateBuilder.or(
                                        b.contains(1, str("5")), b.equal(2, str("b10"))));
        List<Split> orSplits = orRead.newScan().plan().splits();
        assertThat(orSplits).isNotEmpty().allMatch(IndexQuerySplit.class::isInstance);
        assertThat(read(orRead, orSplits)).contains(5, 10, 15, 55, 95).hasSize(20);
    }

    private List<Integer> read(ReadBuilder read, List<Split> splits) throws Exception {
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                read.newRead().executeFilter().createReader(() -> splits)) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        return result;
    }

    private static BinaryString str(String value) {
        return BinaryString.fromString(value);
    }

    private void appendRows(int start, int end) throws Exception {
        FileStoreTable table = getTableDefault();
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
                BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            for (int i = start; i < end; i++) {
                write.write(GenericRow.of(i, str("a" + i), str("b" + i)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void createIndex(String type, String field) throws Exception {
        FileStoreTable table = getTableDefault();
        SortedGlobalIndexScanner builder =
                new SortedGlobalIndexScanner(table, type).withIndexField(field);
        createIndex(type, field, builder.scan().get().entries());
    }

    private void createIndex(String type, String field, List<DataSplit> splits) throws Exception {
        FileStoreTable table = getTableDefault();
        List<CommitMessage> commits = new ArrayList<>();
        for (DataSplit split : splits) {
            commits.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, type, field, split, table.latestSnapshot().get().id()));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commits);
        }
    }

    private List<IndexFileMeta> indexFiles(FileStoreTable table) {
        return table.store().newIndexFileHandler()
                .scan(
                        table.latestSnapshot().get(),
                        entry -> entry.indexFile().globalIndexMeta() != null)
                .stream()
                .map(IndexManifestEntry::indexFile)
                .collect(Collectors.toList());
    }
}
