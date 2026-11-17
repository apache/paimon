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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.btree.BTreeIndexOptions;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Compatibility tests for BTree metadata written before explicit null-bound flags. */
class LegacyBTreeGlobalIndexTableTest extends DataEvolutionTestBase {

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testPredicates(boolean legacy, boolean onlyNulls) throws Exception {
        FileStoreTable table = prepare(legacy, onlyNulls);
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        builder.equal(1, BinaryString.EMPTY_UTF8),
                        builder.lessThan(1, BinaryString.fromString("a")),
                        builder.greaterOrEqual(1, BinaryString.EMPTY_UTF8),
                        builder.startsWith(1, BinaryString.EMPTY_UTF8),
                        builder.isNotNull(1))) {
            ReadBuilder read = table.newReadBuilder().withFilter(predicate);
            List<Integer> actual = new ArrayList<>();
            read.newRead()
                    .executeFilter()
                    .createReader(read.newScan().plan())
                    .forEachRemaining(row -> actual.add(row.getInt(0)));
            assertThat(actual)
                    .as("%s", predicate)
                    .containsExactlyInAnyOrderElementsOf(
                            onlyNulls ? Collections.emptyList() : Arrays.asList(0, 2));
        }
        ReadBuilder read = table.newReadBuilder().withFilter(builder.isNull(1));
        List<Integer> nulls = new ArrayList<>();
        read.newRead()
                .executeFilter()
                .createReader(read.newScan().plan())
                .forEachRemaining(row -> nulls.add(row.getInt(0)));
        assertThat(nulls)
                .containsExactlyInAnyOrderElementsOf(
                        onlyNulls ? Arrays.asList(0, 1, 2) : Collections.singletonList(1));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testTopNAcrossLegacyAndModernRanges(boolean legacy, boolean onlyNulls) throws Exception {
        FileStoreTable table = prepare(legacy, onlyNulls);
        writeRows(table, GenericRow.of(3, BinaryString.fromString("a"), null));
        buildIndex(getTableDefault(), false, true);
        table = getTableDefault();
        for (SortValue.SortDirection direction : SortValue.SortDirection.values()) {
            for (SortValue.NullOrdering nullOrdering : SortValue.NullOrdering.values()) {
                TopN topN =
                        new TopN(
                                new FieldRef(1, "f1", table.rowType().getTypeAt(1)),
                                direction,
                                nullOrdering,
                                1);
                ReadBuilder read = table.newReadBuilder().withTopN(topN);
                List<Integer> candidates = new ArrayList<>();
                read.newRead()
                        .createReader(read.newScan().plan())
                        .forEachRemaining(row -> candidates.add(row.getInt(0)));
                if (nullOrdering == SortValue.NullOrdering.NULLS_FIRST) {
                    assertThat(candidates).anyMatch(id -> onlyNulls ? id < 3 : id == 1);
                } else if (onlyNulls || direction == SortValue.SortDirection.DESCENDING) {
                    assertThat(candidates).contains(3);
                } else {
                    assertThat(candidates).anyMatch(id -> id == 0 || id == 2);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testScalarPrefilter(boolean legacy, boolean onlyNulls) throws Exception {
        FileStoreTable table = prepare(legacy, onlyNulls);
        List<IndexFileMeta> files =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(entry -> entry.indexFile())
                        .collect(Collectors.toList());
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForScalarFilters(table, null, null, files)
                        .get()) {
            Predicate filter =
                    new PredicateBuilder(table.rowType()).equal(1, BinaryString.EMPTY_UTF8);
            assertThat(scanner.scan(filter).get().results().toRangeList())
                    .containsExactlyElementsOf(
                            onlyNulls
                                    ? Collections.emptyList()
                                    : Arrays.asList(new Range(0, 0), new Range(2, 2)));
        }
    }

    private FileStoreTable prepare(boolean legacy, boolean onlyNulls) throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BinaryString value = onlyNulls ? null : BinaryString.EMPTY_UTF8;
        writeRows(
                table,
                GenericRow.of(0, value, null),
                GenericRow.of(1, null, null),
                GenericRow.of(2, value, null));
        buildIndex(getTableDefault(), legacy, false);
        return getTableDefault();
    }

    private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void buildIndex(FileStoreTable table, boolean legacy, boolean incremental)
            throws Exception {
        table =
                table.copy(
                        Collections.singletonMap(
                                BTreeIndexOptions.BTREE_INDEX_FILE_VERSION.key(), "1"));
        SortedGlobalIndexScanner scanner =
                new SortedGlobalIndexScanner(table, "btree")
                        .withIndexFields(Collections.singletonList("f1"));
        ScanResult<DataSplit> scan =
                (incremental ? scanner.incrementalScan() : scanner.scan()).get();
        List<CommitMessage> messages = new ArrayList<>();
        for (DataSplit split : scan.entries()) {
            messages.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, "btree", "f1", split, scan.scanSnapshotId()));
        }
        if (legacy) {
            for (CommitMessage message : messages) {
                List<IndexFileMeta> files =
                        ((CommitMessageImpl) message).newFilesIncrement().newIndexFiles();
                for (int i = 0; i < files.size(); i++) {
                    IndexFileMeta file = files.get(i);
                    GlobalIndexMeta global = file.globalIndexMeta();
                    SortedIndexFileMeta meta = SortedIndexFileMeta.deserialize(global.indexMeta());
                    assertThat(meta.hasNulls()).isTrue();
                    assertThat(meta.firstKey() == null || meta.firstKey().length == 0).isTrue();
                    assertThat(meta.lastKey() == null || meta.lastKey().length == 0).isTrue();
                    // Legacy BTreeIndexMeta wrote zero lengths for both null and empty keys.
                    MemorySliceOutput out = new MemorySliceOutput(9);
                    out.writeInt(0);
                    out.writeInt(0);
                    out.writeByte(1);
                    files.set(
                            i,
                            new IndexFileMeta(
                                    file.indexType(),
                                    file.fileName(),
                                    file.fileSize(),
                                    file.rowCount(),
                                    new GlobalIndexMeta(
                                            global.rowRangeStart(),
                                            global.rowRangeEnd(),
                                            global.indexFieldId(),
                                            global.extraFieldIds(),
                                            out.toSlice().copyBytes()),
                                    file.externalPath()));
                }
            }
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }
}
