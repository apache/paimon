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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Test class for {@link BTreeGlobalIndexBuilder}. */
public class BTreeGlobalIndexBuilderTest extends TableTestBase {

    private static final long PART_ROW_NUM = 1000L;
    private static final KeySerializer KEY_SERIALIZER = KeySerializer.create(DataTypes.INT());
    private static final Comparator<Object> COMPARATOR = KEY_SERIALIZER.createComparator();

    @Override
    public Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("dt", DataTypes.STRING());
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE.key(), "100");
        schemaBuilder.partitionKeys(Collections.singletonList("dt"));
        return schemaBuilder.build();
    }

    private void write() throws Exception {
        createTableDefault();

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write0 = builder.newWrite()) {
            for (int i = 0; i < PART_ROW_NUM; i++) {
                write0.write(
                        GenericRow.of(
                                BinaryString.fromString("p0"),
                                i,
                                BinaryString.fromString("f1_" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        try (BatchTableWrite write1 = builder.newWrite()) {
            for (int i = 0; i < PART_ROW_NUM; i++) {
                write1.write(
                        GenericRow.of(
                                BinaryString.fromString("p1"),
                                i,
                                BinaryString.fromString("f1_" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write1.prepareCommit());
        }
    }

    private void createIndex(PartitionPredicate partitionPredicate) throws Exception {
        FileStoreTable table = getTableDefault();

        BTreeGlobalIndexBuilder builder = new BTreeGlobalIndexBuilder(table);
        builder.withIndexField("f0");
        builder.withIndexType("btree");
        builder.withPartitionPredicate(partitionPredicate);
        List<CommitMessage> commitMessages = builder.build(builder.scan(), ioManager);

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    @Test
    public void testCreateIndexForSinglePartition() throws Exception {
        write();

        FileStoreTable table = getTableDefault();
        RowType partType = table.rowType().project("dt");
        Predicate predicate =
                PartitionPredicate.createPartitionPredicate(
                        partType, Collections.singletonMap("dt", BinaryString.fromString("p0")));

        createIndex(PartitionPredicate.fromPredicate(partType, predicate));

        Map<BinaryRow, List<Pair<String, FileStats>>> metasByParts = gatherIndexMetas(table);

        Assertions.assertEquals(1, metasByParts.size());

        metasByParts.forEach(this::assertFilesNonOverlapping);
    }

    @Test
    public void testCreateIndexForMultiplePartitions() throws Exception {
        write();

        createIndex(null);

        FileStoreTable table = getTableDefault();

        Map<BinaryRow, List<Pair<String, FileStats>>> metasByParts = gatherIndexMetas(table);

        Assertions.assertEquals(2, metasByParts.size());

        metasByParts.forEach(this::assertFilesNonOverlapping);
    }

    private Map<BinaryRow, List<Pair<String, FileStats>>> gatherIndexMetas(FileStoreTable table) {
        IndexFileHandler handler = table.store().newIndexFileHandler();

        Snapshot snapshot = table.latestSnapshot().get();
        List<IndexManifestEntry> entries = handler.scan(snapshot, "btree");

        Map<BinaryRow, List<Pair<String, FileStats>>> metasByParts = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            IndexFileMeta indexFileMeta = entry.indexFile();
            Assertions.assertNotNull(
                    indexFileMeta.globalIndexMeta(), "Global index meta should not be null");

            metasByParts
                    .computeIfAbsent(entry.partition(), part -> new ArrayList<>())
                    .add(
                            Pair.of(
                                    indexFileMeta.fileName(),
                                    FileStats.fromIndexFileMeta(indexFileMeta)));
        }

        return metasByParts;
    }

    private void assertFilesNonOverlapping(
            BinaryRow partition, List<Pair<String, FileStats>> metas) {
        if (metas.isEmpty()) {
            return;
        }

        metas.sort((m1, m2) -> COMPARATOR.compare(m1.getValue().firstKey, m2.getValue().firstKey));
        String lastFileName = metas.get(0).getKey();
        FileStats lastMeta = metas.get(0).getValue();
        long rowCount = lastMeta.rowCount;
        for (int i = 1; i < metas.size(); i++) {
            String fileName = metas.get(i).getKey();
            FileStats fileMeta = metas.get(i).getValue();
            rowCount += fileMeta.rowCount;

            Assertions.assertTrue(
                    COMPARATOR.compare(lastMeta.lastKey, fileMeta.firstKey) <= 0,
                    String.format(
                            "In partition %s, key range [%s:%s] of file %s overlaps with adjacent file %s [%s:%s]",
                            partition.getString(0),
                            lastMeta.firstKey,
                            lastMeta.lastKey,
                            lastFileName,
                            fileName,
                            fileMeta.firstKey,
                            fileMeta.lastKey));

            lastFileName = fileName;
            lastMeta = fileMeta;
        }

        Assertions.assertEquals(
                PART_ROW_NUM,
                rowCount,
                String.format(
                        "In partition %s, total row count of all btree index files not equals to original data row count.",
                        partition.getString(0)));
    }

    static Object deserialize(byte[] bytes) {
        return KEY_SERIALIZER.deserialize(MemorySlice.wrap(bytes));
    }

    /** File Stats class for assertion. */
    private static class FileStats {

        final long rowCount;
        final Object firstKey;
        final Object lastKey;

        FileStats(long rowCount, Object firstKey, Object lastKey) {
            this.rowCount = rowCount;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        static FileStats fromIndexFileMeta(IndexFileMeta meta) {
            Assertions.assertNotNull(meta.globalIndexMeta());
            GlobalIndexMeta globalIndexMeta = meta.globalIndexMeta();
            BTreeIndexMeta btreeMeta = BTreeIndexMeta.deserialize(globalIndexMeta.indexMeta());

            return new FileStats(
                    meta.rowCount(),
                    deserialize(btreeMeta.getFirstKey()),
                    deserialize(btreeMeta.getLastKey()));
        }
    }
}
