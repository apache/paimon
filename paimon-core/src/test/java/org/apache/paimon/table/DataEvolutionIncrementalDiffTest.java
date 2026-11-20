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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.RangeHelper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual snapshot differences must preserve end-snapshot column and deletion dependencies. */
class DataEvolutionIncrementalDiffTest extends DataEvolutionTestBase {

    @Override
    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        warehouse = new Path(tempPath.toUri());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        catalog.createDatabase(database, true);
    }

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.STRING())
                .column("f3", DataTypes.BLOB())
                .option(CoreOptions.BUCKET.key(), "-1")
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                .option(CoreOptions.WRITE_ONLY.key(), "true")
                .option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b")
                .build();
    }

    @Test
    void testSnapshotAndImplicitTagDiffKeepDuplicatesAndNulls() throws Exception {
        FileStoreTable table = create();
        write(table, row(1, "old", null));
        table.createTag("before", 1);
        write(table, row(2, null, "new"), row(2, null, "new"));
        table.createTag("after", 2);
        assertThat(collect(diff(table, "1,2")))
                .containsExactly(row(2, null, "new"), row(2, null, "new"));
        Map<String, String> tags = Collections.singletonMap("incremental-between", "before,after");
        assertThat(collect(table.copy(tags).newReadBuilder()))
                .containsExactly(row(2, null, "new"), row(2, null, "new"));
        assertThat(collect(table.newReadBuilder())).hasSize(3);
    }

    @Test
    void testPartialUpdatesReconstructUntouchedColumns() throws Exception {
        FileStoreTable table = create();
        write(table, row(0, "old-0", "base-0"), row(1, "old-1", "base-1"));
        update(
                table,
                new String[] {"f2"},
                0,
                GenericRow.of(text("new-0")),
                GenericRow.of((Object) null));
        update(
                table,
                new String[] {"f1"},
                0,
                GenericRow.of(text("name-0")),
                GenericRow.of(text("name-1")));
        assertThat(collect(diff(table, "1,3")))
                .containsExactly(row(0, "name-0", "new-0"), row(1, "name-1", null));
        assertThat(collect(diff(table, "2,3")))
                .containsExactly(row(0, "name-0", "new-0"), row(1, "name-1", null));
        assertThat(collect(diff(table, "1,3").withProjection(new int[] {0})))
                .containsExactly(GenericRow.of(0), GenericRow.of(1));
    }

    @Test
    void testSharedAnchorUsesEndSnapshotDeletionVector() throws Exception {
        FileStoreTable table = create();
        write(table, row(0, "a", "old"), row(1, "b", "old"), row(2, "c", "old"));
        deleteRows(table, 1);
        update(
                table,
                new String[] {"f2"},
                0,
                GenericRow.of(text("new")),
                GenericRow.of(text("new")),
                GenericRow.of(text("new")));
        deleteRows(table, 2);
        assertThat(collect(diff(table, "1,4"))).containsExactly(row(0, "a", "new"));
        assertThat(collect(diff(table, "1,4").withProjection(new int[] {0})))
                .containsExactly(GenericRow.of(0));
    }

    @Test
    void testDedicatedBlobSubrangeRetainsAnchorAndSurvivesSerialization() throws Exception {
        FileStoreTable table = create();
        List<InternalRow> initial = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            initial.add(
                    GenericRow.of(
                            i,
                            text("name-" + i),
                            text("base-" + i),
                            new BlobData(new byte[] {(byte) i})));
        }
        write(table, initial.toArray(new InternalRow[0]));
        deleteRows(table, 2, 4);
        FileStoreTable blobWriter =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "128 mb"));
        update(
                blobWriter,
                new String[] {"f3"},
                2,
                GenericRow.of(new BlobData(new byte[] {102})),
                GenericRow.of(new BlobData(new byte[] {103})));
        List<InternalRow> rows = collect(diff(table, "1,3"));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(3);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("name-3");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("base-3");
        assertThat(rows.get(0).getBlob(3).toData()).containsExactly((byte) 103);
        assertThat(collect(diff(table, "1,3").withProjection(new int[] {0})))
                .containsExactly(GenericRow.of(3));
    }

    @Test
    void testPureDeletionAndEmptyDiffDoNotReturnOldRows() throws Exception {
        FileStoreTable table = create();
        write(table, row(0, "a", "old"));
        deleteRows(table, 0);
        assertThat(collect(diff(table, "1,2"))).isEmpty();
        assertThat(collect(diff(table, "2,2"))).isEmpty();
    }

    @Test
    void testProjectionAndFilterDoNotChangeEventDiscovery() throws Exception {
        FileStoreTable table = create();
        write(table, row(0, "a", "old"), row(1, "b", "old"));
        write(table, row(2, "untouched", "match"));
        update(
                table,
                new String[] {"f2"},
                0,
                GenericRow.of(text("match")),
                GenericRow.of(text("other")));
        ReadBuilder builder = diff(table, "2,3");
        builder.withFilter(new PredicateBuilder(table.rowType()).equal(2, text("match")));
        builder.withProjection(new int[] {0, 1});
        assertThat(collect(builder)).containsExactly(GenericRow.of(0, text("a")));
    }

    @Test
    void testLimitReadsPastAnEntirelyDeletedSplit() throws Exception {
        FileStoreTable table = create();
        write(table, row(0, "a", "old"));
        write(table, row(1, "b", "old"));
        update(table, new String[] {"f2"}, 0, GenericRow.of(text("new")));
        update(table, new String[] {"f2"}, 1, GenericRow.of(text("new")));
        deleteRows(table, 0);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 b");
        options.put(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(), "1 b");
        table = table.copy(options);
        assertThat(diff(table, "2,5").newScan().plan().splits()).hasSize(2);
        assertThat(collect(diff(table, "2,5").withLimit(1))).containsExactly(row(1, "b", "new"));
    }

    @Test
    void testOrdinaryAppendAndPrimaryKeyDiffStillWork() throws Exception {
        for (boolean primaryKey : new boolean[] {false, true}) {
            Schema.Builder schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("v", DataTypes.STRING());
            if (primaryKey) {
                schema.primaryKey("id").option(CoreOptions.BUCKET.key(), "1");
            }
            catalog.createTable(identifier("control" + primaryKey), schema.build(), false);
            FileStoreTable table =
                    (FileStoreTable) catalog.getTable(identifier("control" + primaryKey));
            write(table, GenericRow.of(1, text("old")));
            write(table, GenericRow.of(primaryKey ? 1 : 2, text("new")));
            List<InternalRow> rows = collect(diff(table, "1,2"));
            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getString(1).toString()).isEqualTo("new");
        }
    }

    private FileStoreTable create() throws Exception {
        createTableDefault();
        return getTableDefault();
    }

    private ReadBuilder diff(FileStoreTable table, String between) {
        Map<String, String> options = new HashMap<>();
        options.put("incremental-between", between);
        options.put("incremental-between-scan-mode", "diff");
        return table.copy(options).newReadBuilder();
    }

    private List<InternalRow> collect(ReadBuilder builder) throws Exception {
        List<Split> splits = builder.newScan().plan().splits();
        // Exercise the same split transfer used between planning and physical readers.
        splits =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(splits), getClass().getClassLoader());
        InternalRowSerializer serializer = new InternalRowSerializer(builder.readType());
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().executeFilter().createReader(splits)) {
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }
        return result;
    }

    private void update(
            FileStoreTable table, String[] columns, long firstRowId, InternalRow... rows)
            throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        builder.newWrite()
                                .withWriteType(table.rowType().project(Arrays.asList(columns)));
                BatchTableCommit commit = builder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, firstRowId);
            commit.commit(messages);
        }
    }

    private void deleteRows(FileStoreTable table, long... rowIds) throws Exception {
        List<DataFileMeta> files =
                table.store().newScan().plan().files(FileKind.ADD).stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        BinaryRow.EMPTY_ROW);
        for (List<DataFileMeta> group :
                new RangeHelper<>(DataFileMeta::nonNullRowIdRange).mergeOverlappingRanges(files)) {
            DataFileMeta anchor = retrieveAnchorFile(group, Function.identity());
            DeletionVector vector = new BitmapDeletionVector();
            boolean changed = false;
            for (long rowId : rowIds) {
                if (anchor.nonNullRowIdRange().from <= rowId
                        && rowId <= anchor.nonNullRowIdRange().to) {
                    vector.delete(rowId - anchor.nonNullFirstRowId());
                    changed = true;
                }
            }
            if (changed) {
                maintainer.notifyNewDeletionVector(anchor.fileName(), vector);
            }
        }
        List<IndexFileMeta> added = new ArrayList<>();
        List<IndexFileMeta> deleted = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            (entry.kind() == FileKind.ADD ? added : deleted).add(entry.indexFile());
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    0,
                                    null,
                                    new DataIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            added,
                                            deleted),
                                    CompactIncrement.emptyIncrement())));
        }
    }

    private static BinaryString text(String value) {
        return value == null ? null : BinaryString.fromString(value);
    }

    private static InternalRow row(int id, String f1, String f2) {
        return GenericRow.of(id, text(f1), text(f2), null);
    }
}
