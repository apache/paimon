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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BTreeGlobalIndexCompactor}. */
public class BTreeGlobalIndexCompactorTest extends TableTestBase {

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testCompactEntriesMergeContiguousRangesAndRebaseRowIds() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        DataField indexField = table.rowType().getField("f0");

        IndexManifestEntry first =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(0, 99),
                        Arrays.asList(
                                record(null, 3), record(10, 0), record(20, 1), record(20, 2)));
        IndexManifestEntry second =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(100, 199),
                        Arrays.asList(record(null, 2), record(20, 0), record(30, 1)));

        BTreeGlobalIndexCompactor compactor = createCompactor(table, Collections.emptyMap());
        List<IndexManifestEntry> compactedEntries = compactor.compact(Arrays.asList(first, second));
        assertThat(compactedEntries).hasSize(1);
        IndexFileMeta mergedMeta = compactedEntries.get(0).indexFile();
        assertThat(mergedMeta.globalIndexMeta()).isNotNull();
        assertThat(mergedMeta.globalIndexMeta().rowRangeStart()).isEqualTo(0);
        assertThat(mergedMeta.globalIndexMeta().rowRangeEnd()).isEqualTo(199);

        List<Long> eq10;
        List<Long> eq20;
        List<Long> eq30;
        List<Long> isNull;
        FieldRef ref = new FieldRef(indexField.id(), indexField.name(), indexField.type());
        try (BTreeIndexReader reader = createReader(table, indexField, mergedMeta)) {
            eq10 = toList(reader.visitEqual(ref, 10).get());
            eq20 = toList(reader.visitEqual(ref, 20).get());
            eq30 = toList(reader.visitEqual(ref, 30).get());
            isNull = toList(reader.visitIsNull(ref).get());
        }

        assertThat(eq10).containsExactly(0L);
        assertThat(eq20).containsExactlyInAnyOrder(1L, 2L, 100L);
        assertThat(eq30).containsExactly(101L);
        assertThat(isNull).containsExactlyInAnyOrder(3L, 102L);
    }

    @Test
    public void testCompactEntriesMergeAllRangeGroups() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        DataField indexField = table.rowType().getField("f0");

        List<IndexManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            long start = i * 10;
            entries.add(
                    createSourceEntry(
                            table,
                            indexField,
                            new Range(start, start + 9),
                            Collections.singletonList(record((int) start, 0))));
        }

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "2");
        BTreeGlobalIndexCompactor compactor = createCompactor(table, dynamicOptions);
        List<IndexManifestEntry> compactedEntries = compactor.compact(entries);
        assertThat(compactedEntries).isNotEmpty();
        assertThat(compactedEntries)
                .allSatisfy(
                        e -> {
                            assertThat(e.indexFile().globalIndexMeta().rowRangeStart())
                                    .isEqualTo(0L);
                            assertThat(e.indexFile().globalIndexMeta().rowRangeEnd())
                                    .isEqualTo(49L);
                        });
    }

    @Test
    public void testCompactEntriesSkipByMinFiles() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        DataField indexField = table.rowType().getField("f0");

        IndexManifestEntry first =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(0, 9),
                        Collections.singletonList(record(0, 0)));
        IndexManifestEntry second =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(20, 29),
                        Collections.singletonList(record(20, 0)));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "3");
        BTreeGlobalIndexCompactor compactor = createCompactor(table, dynamicOptions);
        List<IndexManifestEntry> compactedEntries = compactor.compact(Arrays.asList(first, second));
        assertThat(compactedEntries)
                .extracting(
                        e ->
                                new Range(
                                        e.indexFile().globalIndexMeta().rowRangeStart(),
                                        e.indexFile().globalIndexMeta().rowRangeEnd()))
                .containsExactlyInAnyOrder(new Range(0, 9), new Range(20, 29));
    }

    @Test
    public void testCompactEntriesRespectMinFilesPerSegment() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        DataField indexField = table.rowType().getField("f0");

        List<IndexManifestEntry> entries =
                Arrays.asList(
                        createSourceEntry(
                                table,
                                indexField,
                                new Range(0, 9),
                                Collections.singletonList(record(0, 0))),
                        createSourceEntry(
                                table,
                                indexField,
                                new Range(10, 19),
                                Collections.singletonList(record(10, 0))),
                        createSourceEntry(
                                table,
                                indexField,
                                new Range(100, 109),
                                Collections.singletonList(record(100, 0))),
                        createSourceEntry(
                                table,
                                indexField,
                                new Range(110, 119),
                                Collections.singletonList(record(110, 0))));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES.key(), "3");
        BTreeGlobalIndexCompactor compactor = createCompactor(table, dynamicOptions);

        List<IndexManifestEntry> compactedEntries = compactor.compact(entries);

        assertThat(compactedEntries)
                .extracting(
                        e ->
                                new Range(
                                        e.indexFile().globalIndexMeta().rowRangeStart(),
                                        e.indexFile().globalIndexMeta().rowRangeEnd()))
                .containsExactlyInAnyOrder(
                        new Range(0, 9),
                        new Range(10, 19),
                        new Range(100, 109),
                        new Range(110, 119));
    }

    @Test
    public void testCompactEntriesDoNotMergeGapRanges() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        DataField indexField = table.rowType().getField("f0");

        IndexManifestEntry first =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(0, 999),
                        Collections.singletonList(record(1, 0)));
        IndexManifestEntry second =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(1001, 2000),
                        Collections.singletonList(record(2, 0)));

        BTreeGlobalIndexCompactor compactor = createCompactor(table, Collections.emptyMap());
        List<IndexManifestEntry> compactedEntries = compactor.compact(Arrays.asList(first, second));

        assertThat(compactedEntries).hasSize(2);
        assertThat(compactedEntries)
                .extracting(
                        e ->
                                new Range(
                                        e.indexFile().globalIndexMeta().rowRangeStart(),
                                        e.indexFile().globalIndexMeta().rowRangeEnd()))
                .containsExactlyInAnyOrder(new Range(0, 999), new Range(1001, 2000));
    }

    @Test
    public void testCompactOutputsHaveSameRowRangeAndNonOverlappedMinMax() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        DataField indexField = table.rowType().getField("f0");

        IndexManifestEntry first =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(0, 99),
                        Arrays.asList(record(1, 0), record(2, 1), record(3, 2), record(4, 3)));
        IndexManifestEntry second =
                createSourceEntry(
                        table,
                        indexField,
                        new Range(100, 199),
                        Arrays.asList(record(5, 0), record(6, 1), record(7, 2), record(8, 3)));

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "2");
        BTreeGlobalIndexCompactor compactor = createCompactor(table, dynamicOptions);
        List<IndexManifestEntry> compactedEntries = compactor.compact(Arrays.asList(first, second));
        assertThat(compactedEntries).isNotEmpty();
        List<IndexFileMeta> newFiles = new ArrayList<>();
        for (IndexManifestEntry entry : compactedEntries) {
            newFiles.add(entry.indexFile());
        }
        assertThat(newFiles.size()).isGreaterThan(1);
        assertThat(newFiles)
                .allSatisfy(
                        f -> {
                            assertThat(f.globalIndexMeta().rowRangeStart()).isEqualTo(0L);
                            assertThat(f.globalIndexMeta().rowRangeEnd()).isEqualTo(199L);
                        });

        final Comparator<Object> comparator =
                KeySerializer.create(indexField.type()).createComparator();
        List<BTreeIndexMeta> metas = new ArrayList<>();
        for (IndexFileMeta file : newFiles) {
            metas.add(BTreeIndexMeta.deserialize(file.globalIndexMeta().indexMeta()));
        }
        metas.sort(
                (m1, m2) ->
                        comparator.compare(
                                deserializeKey(indexField, m1.getFirstKey()),
                                deserializeKey(indexField, m2.getFirstKey())));

        for (int i = 1; i < metas.size(); i++) {
            Object prevMax = deserializeKey(indexField, metas.get(i - 1).getLastKey());
            Object currentMin = deserializeKey(indexField, metas.get(i).getFirstKey());
            assertThat(comparator.compare(prevMax, currentMin)).isLessThan(0);
        }
    }

    private BTreeIndexReader createReader(
            FileStoreTable table, DataField indexField, IndexFileMeta mergedMeta) throws Exception {
        GlobalIndexMeta globalMeta = mergedMeta.globalIndexMeta();
        GlobalIndexFileReadWrite readWrite =
                new GlobalIndexFileReadWrite(
                        table.fileIO(), table.store().pathFactory().globalIndexFileFactory());
        GlobalIndexIOMeta ioMeta =
                new GlobalIndexIOMeta(
                        table.store().pathFactory().globalIndexFileFactory().toPath(mergedMeta),
                        mergedMeta.fileSize(),
                        globalMeta.indexMeta());
        return new BTreeIndexReader(
                KeySerializer.create(indexField.type()),
                readWrite,
                ioMeta,
                new CacheManager(MemorySize.ofMebiBytes(8), 0.1));
    }

    private IndexManifestEntry createSourceEntry(
            FileStoreTable table, DataField indexField, Range range, List<IndexRecord> records)
            throws Exception {
        Options options = table.coreOptions().toConfiguration();
        GlobalIndexFileReadWrite readWrite =
                new GlobalIndexFileReadWrite(
                        table.fileIO(), table.store().pathFactory().globalIndexFileFactory());
        BTreeGlobalIndexer indexer = new BTreeGlobalIndexer(indexField, options);
        BTreeIndexWriter writer = indexer.createWriter(readWrite);
        for (IndexRecord record : records) {
            writer.write(record.key, record.localRowId);
        }

        List<ResultEntry> entries = writer.finish();
        List<IndexFileMeta> indexFileMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        table.store().pathFactory().globalIndexFileFactory(),
                        table.coreOptions(),
                        range,
                        indexField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        entries);
        assertThat(indexFileMetas).hasSize(1);
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, indexFileMetas.get(0));
    }

    private static IndexRecord record(Integer key, long localRowId) {
        return new IndexRecord(key, localRowId);
    }

    private static List<Long> toList(GlobalIndexResult result) {
        List<Long> list = new ArrayList<>();
        for (Long rowId : result.results()) {
            list.add(rowId);
        }
        return list;
    }

    private static Object deserializeKey(DataField field, byte[] bytes) {
        return KeySerializer.create(field.type())
                .deserialize(org.apache.paimon.memory.MemorySlice.wrap(bytes));
    }

    private static BTreeGlobalIndexCompactor createCompactor(
            FileStoreTable table, Map<String, String> dynamicOptions) {
        Options options = table.coreOptions().toConfiguration();
        for (Map.Entry<String, String> option : dynamicOptions.entrySet()) {
            options.setString(option.getKey(), option.getValue());
        }
        return new BTreeGlobalIndexCompactor(
                table.fileIO(),
                table.rowType(),
                options,
                table.store().pathFactory().globalIndexFileFactory());
    }

    private static class IndexRecord {
        private final Integer key;
        private final long localRowId;

        private IndexRecord(Integer key, long localRowId) {
            this.key = key;
            this.localRowId = localRowId;
        }
    }
}
