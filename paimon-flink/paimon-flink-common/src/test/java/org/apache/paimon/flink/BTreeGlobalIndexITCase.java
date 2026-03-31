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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeWithFileMetaBuilder;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for btree global index. */
public class BTreeGlobalIndexITCase extends CatalogITCaseBase {

    @Test
    public void testBTreeIndex() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        String values =
                IntStream.range(0, 1_000)
                        .mapToObj(i -> String.format("(%s, %s)", i, "'name_" + i + "'"))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T VALUES " + values);
        sql(
                "CALL sys.create_global_index(`table` => 'default.T', index_column => 'id', index_type => 'btree')");

        // assert has btree index
        FileStoreTable table = paimonTable("T");
        List<IndexFileMeta> btreeEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> "btree".equals(f.indexType()))
                        .collect(Collectors.toList());

        long totalRowCount = btreeEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(btreeEntries).hasSize(1);
        assertThat(totalRowCount).isEqualTo(1000L);

        // assert select with filter
        assertThat(sql("SELECT * FROM T WHERE id = 100")).containsOnly(Row.of(100, "name_100"));
    }

    @Test
    public void testBTreeIndexWithMultiPartition() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_MP (pt INT, id INT, name STRING) PARTITIONED BY (pt) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");

        // write partition 0: 100k rows
        insertPartitionRows("T_MP", 0, 0, 500, "p0_a_");
        insertPartitionRows("T_MP", 0, 500, 500, "p0_a_");
        // write partition 1: 100k rows
        insertPartitionRows("T_MP", 1, 1_000, 1_000, "p1_");
        // write partition 0 again: 100k rows
        insertPartitionRows("T_MP", 0, 2_000, 1_000, "p0_b_");

        buildBTreeIndexForTable("T_MP", "id");

        FileStoreTable table = paimonTable("T_MP");
        List<IndexManifestEntry> btreeEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(e -> "btree".equals(e.indexFile().indexType()))
                        .collect(Collectors.toList());

        long totalRowCount =
                btreeEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .mapToLong(IndexFileMeta::rowCount)
                        .sum();
        Map<Object, Long> partitionRowCounts =
                btreeEntries.stream()
                        .collect(
                                Collectors.groupingBy(
                                        IndexManifestEntry::partition,
                                        Collectors.summingLong(e -> e.indexFile().rowCount())));

        assertThat(partitionRowCounts).hasSize(2);
        assertThat(partitionRowCounts.values()).containsExactlyInAnyOrder(1_000L, 2_000L);
        assertThat(totalRowCount).isEqualTo(3_000L);

        assertThat(sql("SELECT * FROM T_MP WHERE id = 999"))
                .containsOnly(Row.of(0, 999, "p0_a_999"));
        assertThat(sql("SELECT * FROM T_MP WHERE id = 1500"))
                .containsOnly(Row.of(1, 1500, "p1_1500"));
        assertThat(sql("SELECT * FROM T_MP WHERE id = 2500"))
                .containsOnly(Row.of(0, 2500, "p0_b_2500"));
    }

    private void insertPartitionRows(
            String tableName, int partition, int startId, int count, String namePrefix) {
        final int batchSize = 5_000;
        for (int offset = 0; offset < count; offset += batchSize) {
            int batchStart = startId + offset;
            int batchEnd = Math.min(startId + count, batchStart + batchSize);
            String values =
                    IntStream.range(batchStart, batchEnd)
                            .mapToObj(
                                    i ->
                                            String.format(
                                                    "(%d, %d, '%s%d')",
                                                    partition, i, namePrefix, i))
                            .collect(Collectors.joining(","));
            sql("INSERT INTO %s VALUES %s", tableName, values);
        }
    }

    @Test
    public void testBTreeIndexWithFileMeta() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_FM (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        String values =
                IntStream.range(0, 1_000)
                        .mapToObj(i -> String.format("(%s, %s)", i, "'name_" + i + "'"))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T_FM VALUES " + values);
        sql(
                "CALL sys.create_global_index(`table` => 'default.T_FM', index_column => 'name',"
                        + " index_type => 'btree', options => 'index.with-file-meta=true')");

        FileStoreTable table = paimonTable("T_FM");
        List<IndexManifestEntry> allEntries = table.store().newIndexFileHandler().scanEntries();

        // assert key-index (btree) entries exist
        List<IndexFileMeta> keyIndexEntries =
                allEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> BTreeGlobalIndexerFactory.IDENTIFIER.equals(f.indexType()))
                        .collect(Collectors.toList());
        assertThat(keyIndexEntries).isNotEmpty();

        // assert file-meta index (btree_file_meta) entries exist
        List<IndexFileMeta> fileMetaEntries =
                allEntries.stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(
                                f ->
                                        BTreeWithFileMetaBuilder.INDEX_TYPE_FILE_META.equals(
                                                f.indexType()))
                        .collect(Collectors.toList());
        assertThat(fileMetaEntries).isNotEmpty();

        long totalRowCount = keyIndexEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(1000L);

        // assert query returns correct results via manifest-free read path
        assertThat(sql("SELECT * FROM T_FM WHERE name = 'name_100'"))
                .containsOnly(Row.of(100, "name_100"));
        assertThat(sql("SELECT * FROM T_FM WHERE name = 'name_999'"))
                .containsOnly(Row.of(999, "name_999"));
    }

    private void buildBTreeIndexForTable(String tableName, String indexColumn) {
        sql(
                "CALL sys.create_global_index(`table` => 'default.%s', index_column => '%s', index_type => 'btree')",
                tableName, indexColumn);
    }
}
