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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

        sql(
                "CALL sys.create_global_index(`table` => 'default.T_MP', index_column => 'id', index_type => 'btree')");

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

    @Test
    public void testBTreeIndexIncrementalBuild() throws Exception {
        sql(
                "CREATE TABLE T_INC (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");

        insertRows("T_INC", 0, 2_000, "name_");

        buildBTreeIndex("T_INC");
        List<IndexManifestEntry> firstEntries = btreeEntries("T_INC");
        long firstTotal = firstEntries.stream().mapToLong(e -> e.indexFile().rowCount()).sum();
        Set<String> firstFileNames =
                firstEntries.stream()
                        .map(e -> e.indexFile().fileName())
                        .collect(Collectors.toSet());
        assertThat(firstTotal).isEqualTo(2_000L);

        // no new rows: incremental build should not produce new btree index files
        buildBTreeIndex("T_INC");
        List<IndexManifestEntry> secondEntries = btreeEntries("T_INC");
        Set<String> secondFileNames =
                secondEntries.stream()
                        .map(e -> e.indexFile().fileName())
                        .collect(Collectors.toSet());
        assertThat(secondFileNames).isEqualTo(firstFileNames);

        insertRows("T_INC", 2_000, 1_000, "name_");

        buildBTreeIndex("T_INC");
        List<IndexManifestEntry> thirdEntries = btreeEntries("T_INC");
        long thirdTotal = thirdEntries.stream().mapToLong(e -> e.indexFile().rowCount()).sum();
        assertThat(thirdTotal).isGreaterThanOrEqualTo(firstTotal);

        assertThat(sql("SELECT * FROM T_INC WHERE id = 100")).containsOnly(Row.of(100, "name_100"));
    }

    @Test
    public void testBTreeIndexIncrementalBuildWithMixedPartitions() throws Exception {
        sql(
                "CREATE TABLE T_MIX_INC (pt INT, id INT, name STRING) PARTITIONED BY (pt) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");

        // round-1 mixed writes
        insertPartitionRows("T_MIX_INC", 0, 0, 600, "p0_a_");
        insertPartitionRows("T_MIX_INC", 1, 10_000, 400, "p1_a_");
        insertPartitionRows("T_MIX_INC", 0, 20_000, 300, "p0_b_");
        insertPartitionRows("T_MIX_INC", 2, 30_000, 500, "p2_a_");

        buildBTreeIndexForTable("T_MIX_INC", "id");
        List<IndexManifestEntry> firstEntries = btreeEntries("T_MIX_INC");
        long firstTotal = firstEntries.stream().mapToLong(e -> e.indexFile().rowCount()).sum();

        // round-2 mixed writes
        insertPartitionRows("T_MIX_INC", 2, 40_000, 500, "p2_b_");
        insertPartitionRows("T_MIX_INC", 1, 50_000, 400, "p1_b_");
        insertPartitionRows("T_MIX_INC", 0, 60_000, 600, "p0_c_");

        buildBTreeIndexForTable("T_MIX_INC", "id");
        List<IndexManifestEntry> secondEntries = btreeEntries("T_MIX_INC");
        long secondTotal = secondEntries.stream().mapToLong(e -> e.indexFile().rowCount()).sum();
        Map<Object, Long> partitionRowCounts =
                secondEntries.stream()
                        .collect(
                                Collectors.groupingBy(
                                        IndexManifestEntry::partition,
                                        Collectors.summingLong(e -> e.indexFile().rowCount())));

        assertThat(partitionRowCounts).hasSize(3);
        assertThat(secondTotal).isGreaterThanOrEqualTo(firstTotal);
        // Verify existing indexed data is still queryable after incremental rebuild.
        assertThat(sql("SELECT * FROM T_MIX_INC WHERE id = 10"))
                .containsOnly(Row.of(0, 10, "p0_a_10"));
        assertThat(sql("SELECT * FROM T_MIX_INC WHERE id = 10010"))
                .containsOnly(Row.of(1, 10010, "p1_a_10010"));
        assertThat(sql("SELECT * FROM T_MIX_INC WHERE id = 30010"))
                .containsOnly(Row.of(2, 30010, "p2_a_30010"));
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

    private List<IndexManifestEntry> btreeEntries(String tableName)
            throws Catalog.TableNotExistException {
        FileStoreTable table = paimonTable(tableName);
        return table.store().newIndexFileHandler().scanEntries().stream()
                .filter(e -> "btree".equals(e.indexFile().indexType()))
                .collect(Collectors.toList());
    }

    private void buildBTreeIndex(String tableName) throws Exception {
        buildBTreeIndexForTable(tableName, "id");
    }

    private void buildBTreeIndexForTable(String tableName, String indexColumn) throws Exception {
        sql(
                "CALL sys.create_global_index(`table` => 'default.%s', index_column => '%s', index_type => 'btree')",
                tableName, indexColumn);
    }

    private void insertRows(String tableName, int startId, int count, String namePrefix) {
        final int batchSize = 5_000;
        for (int offset = 0; offset < count; offset += batchSize) {
            int batchStart = startId + offset;
            int batchEnd = Math.min(startId + count, batchStart + batchSize);
            String values =
                    IntStream.range(batchStart, batchEnd)
                            .mapToObj(i -> String.format("(%d, '%s%d')", i, namePrefix, i))
                            .collect(Collectors.joining(","));
            sql("INSERT INTO %s VALUES %s", tableName, values);
        }
    }
}
