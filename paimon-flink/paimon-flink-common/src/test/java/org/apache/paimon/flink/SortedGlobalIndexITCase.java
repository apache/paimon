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
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ResolvedFieldPath;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for sorted global indexes. */
public class SortedGlobalIndexITCase extends CatalogITCaseBase {

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
    public void testNestedBTreeIndex() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_NESTED (id INT, profile ROW<zip INT, city STRING>) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        sql("INSERT INTO T_NESTED VALUES (1, ROW(100, 'a')), (2, ROW(200, 'b'))");
        sql(
                "CALL sys.create_global_index(`table` => 'default.T_NESTED', "
                        + "index_column => 'profile.zip', index_type => 'btree')");

        FileStoreTable table = paimonTable("T_NESTED");
        List<IndexFileMeta> btreeEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> "btree".equals(f.indexType()))
                        .collect(Collectors.toList());
        int zipFieldId =
                ResolvedFieldPath.resolve(table.rowType(), "profile.zip").get().leafField().id();

        assertThat(btreeEntries).hasSize(1);
        assertThat(btreeEntries.get(0).rowCount()).isEqualTo(2L);
        assertThat(btreeEntries.get(0).globalIndexMeta()).isNotNull();
        assertThat(btreeEntries.get(0).globalIndexMeta().indexFieldId()).isEqualTo(zipFieldId);
        assertThat(sql("SELECT * FROM T_NESTED WHERE profile.zip = 200"))
                .containsOnly(Row.of(2, Row.of(200, "b")));
    }

    @Test
    public void testBitmapIndex() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_BITMAP (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        String values =
                IntStream.range(0, 1_000)
                        .mapToObj(i -> String.format("(%s, %s)", i, "'name_" + i + "'"))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T_BITMAP VALUES " + values);
        sql(
                "CALL sys.create_global_index(`table` => 'default.T_BITMAP', "
                        + "index_column => 'id', index_type => 'bitmap', "
                        + "options => 'sorted-index.records-per-range=200')");

        FileStoreTable table = paimonTable("T_BITMAP");
        List<IndexFileMeta> bitmapEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> "bitmap".equals(f.indexType()))
                        .collect(Collectors.toList());

        long totalRowCount = bitmapEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(bitmapEntries).hasSizeGreaterThan(1);
        assertThat(totalRowCount).isEqualTo(1000L);

        assertThat(sql("SELECT * FROM T_BITMAP WHERE id = 100"))
                .containsOnly(Row.of(100, "name_100"));
    }

    @Test
    public void testBitmapRefreshesUpdatedDataEvolutionRange() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE T_BITMAP_REFRESH (id INT, idx INT) WITH ("
                        + "'bucket' = '-1', "
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'global-index.column-update-action' = 'IGNORE'"
                        + ")");
        sql(
                "INSERT INTO T_BITMAP_REFRESH VALUES "
                        + IntStream.range(0, 10)
                                .mapToObj(i -> String.format("(%d, %d)", i, i))
                                .collect(Collectors.joining(",")));
        buildBitmapIndexForTable("T_BITMAP_REFRESH", "idx");

        FileStoreTable table = paimonTable("T_BITMAP_REFRESH");
        List<IndexManifestEntry> initial = indexEntries(table, "bitmap");
        assertThat(initial).isNotEmpty();
        Set<String> initialFiles = fileNames(initial);

        sql("CREATE TABLE S_BITMAP_REFRESH (id INT, idx INT)");
        sql("INSERT INTO S_BITMAP_REFRESH VALUES (1, 1001)");
        sql(
                "CALL sys.data_evolution_merge_into("
                        + "'default.T_BITMAP_REFRESH', '', '', 'S_BITMAP_REFRESH', "
                        + "'T_BITMAP_REFRESH.id=S_BITMAP_REFRESH.id', "
                        + "'idx=S_BITMAP_REFRESH.idx', 2)");
        table = paimonTable("T_BITMAP_REFRESH");
        long updateSnapshotId = table.snapshotManager().latestSnapshot().id();
        assertThat(fileNames(indexEntries(table, "bitmap"))).isEqualTo(initialFiles);

        buildBitmapIndexForTable("T_BITMAP_REFRESH", "idx");
        table = paimonTable("T_BITMAP_REFRESH");
        List<IndexManifestEntry> refreshed = indexEntries(table, "bitmap");
        assertThat(fileNames(refreshed)).doesNotContainAnyElementsOf(initialFiles);
        assertThat(refreshed)
                .allSatisfy(
                        entry ->
                                assertThat(
                                                DataEvolutionIndexSourceMeta.fromIndexFile(
                                                                entry.indexFile())
                                                        .scanSnapshotId())
                                        .isEqualTo(updateSnapshotId));
        assertThat(sql("SELECT id FROM T_BITMAP_REFRESH WHERE idx = 1001")).containsOnly(Row.of(1));
    }

    @Test
    public void testBTreeRefreshesUpdatedDataEvolutionRangeAtomically() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE T_REFRESH (id INT, idx INT, payload STRING) WITH ("
                        + "'bucket' = '-1', "
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'global-index.column-update-action' = 'IGNORE', "
                        + "'btree-index.records-per-range' = '2'"
                        + ")");
        sql(
                "INSERT INTO T_REFRESH VALUES "
                        + IntStream.range(0, 10)
                                .mapToObj(i -> String.format("(%d, %d, 'p%d')", i, i, i))
                                .collect(Collectors.joining(",")));
        buildBTreeIndexForTable("T_REFRESH", "idx");

        sql(
                "INSERT INTO T_REFRESH VALUES "
                        + IntStream.range(10, 20)
                                .mapToObj(i -> String.format("(%d, %d, 'p%d')", i, i, i))
                                .collect(Collectors.joining(",")));
        buildBTreeIndexForTable("T_REFRESH", "idx");

        FileStoreTable table = paimonTable("T_REFRESH");
        Map<String, List<IndexManifestEntry>> initial = btreeEntriesByRange(table);
        assertThat(initial.keySet()).containsExactlyInAnyOrder("0:9", "10:19");
        assertThat(initial.get("0:9")).hasSizeGreaterThan(1);
        assertThat(initial.get("10:19")).hasSizeGreaterThan(1);
        assertThat(flatten(initial)).allSatisfy(entry -> assertThat(entry.bucket()).isZero());
        assertThat(table.store().newScan().plan().files())
                .allSatisfy(entry -> assertThat(entry.bucket()).isZero());
        Set<String> initialFirstFiles = fileNames(initial.get("0:9"));
        Set<String> initialSecondFiles = fileNames(initial.get("10:19"));
        Set<String> initialFiles = fileNames(flatten(initial));
        long beforeUpdateSnapshotId = table.snapshotManager().latestSnapshot().id();

        sql("CREATE TABLE S_REFRESH (id INT, idx INT)");
        sql("INSERT INTO S_REFRESH VALUES (1, 1001)");
        sql(
                "CALL sys.data_evolution_merge_into("
                        + "'default.T_REFRESH', '', '', 'S_REFRESH', "
                        + "'T_REFRESH.id=S_REFRESH.id', 'idx=S_REFRESH.idx', 2)");
        table = paimonTable("T_REFRESH");
        long updateSnapshotId = table.snapshotManager().latestSnapshot().id();
        assertThat(updateSnapshotId).isGreaterThan(beforeUpdateSnapshotId);
        assertThat(fileNames(flatten(btreeEntriesByRange(table)))).isEqualTo(initialFiles);
        assertThat(sql("SELECT id FROM T_REFRESH WHERE idx = 1")).isEmpty();
        assertThat(sql("SELECT id FROM T_REFRESH WHERE idx = 1001")).isEmpty();
        assertThat(sql("SELECT idx FROM T_REFRESH WHERE id = 1")).containsOnly(Row.of(1001));
        assertThat(sql("SELECT id FROM T_REFRESH WHERE idx = 2")).containsOnly(Row.of(2));

        buildBTreeIndexForTable("T_REFRESH", "idx");
        table = paimonTable("T_REFRESH");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(updateSnapshotId + 1);

        Map<String, List<IndexManifestEntry>> refreshed = btreeEntriesByRange(table);
        assertThat(refreshed.keySet()).containsExactlyInAnyOrder("0:9", "10:19");
        assertThat(fileNames(refreshed.get("0:9"))).doesNotContainAnyElementsOf(initialFirstFiles);
        assertThat(fileNames(refreshed.get("10:19"))).isEqualTo(initialSecondFiles);
        assertThat(refreshed.get("0:9"))
                .allSatisfy(
                        entry ->
                                assertThat(
                                                DataEvolutionIndexSourceMeta.fromIndexFile(
                                                                entry.indexFile())
                                                        .scanSnapshotId())
                                        .isEqualTo(updateSnapshotId));
        assertThat(sql("SELECT id FROM T_REFRESH WHERE idx = 1")).isEmpty();
        assertThat(sql("SELECT id FROM T_REFRESH WHERE idx = 1001")).containsOnly(Row.of(1));

        long refreshedSnapshotId = table.snapshotManager().latestSnapshot().id();
        Set<String> refreshedFiles = fileNames(flatten(refreshed));
        buildBTreeIndexForTable("T_REFRESH", "idx");
        table = paimonTable("T_REFRESH");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(refreshedSnapshotId);
        assertThat(fileNames(flatten(btreeEntriesByRange(table)))).isEqualTo(refreshedFiles);

        sql("CREATE TABLE S_PAYLOAD (id INT, payload STRING)");
        sql("INSERT INTO S_PAYLOAD VALUES (1, 'new-payload')");
        sql(
                "CALL sys.data_evolution_merge_into("
                        + "'default.T_REFRESH', '', '', 'S_PAYLOAD', "
                        + "'T_REFRESH.id=S_PAYLOAD.id', 'payload=S_PAYLOAD.payload', 1)");
        table = paimonTable("T_REFRESH");
        long payloadUpdateSnapshotId = table.snapshotManager().latestSnapshot().id();
        assertThat(payloadUpdateSnapshotId).isGreaterThan(refreshedSnapshotId);
        assertThat(sql("SELECT payload FROM T_REFRESH WHERE id = 1"))
                .containsOnly(Row.of("new-payload"));
        buildBTreeIndexForTable("T_REFRESH", "idx");
        table = paimonTable("T_REFRESH");
        assertThat(table.snapshotManager().latestSnapshot().id())
                .isEqualTo(payloadUpdateSnapshotId);
        assertThat(fileNames(flatten(btreeEntriesByRange(table)))).isEqualTo(refreshedFiles);
    }

    @Test
    public void testBTreeRefreshesOnlyUpdatedPartition() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CREATE TABLE T_PARTITION_REFRESH (pt INT, id INT, idx INT) "
                        + "PARTITIONED BY (pt) WITH ("
                        + "'bucket' = '-1', "
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'global-index.column-update-action' = 'IGNORE', "
                        + "'btree-index.records-per-range' = '2'"
                        + ")");
        sql(
                "INSERT INTO T_PARTITION_REFRESH VALUES "
                        + IntStream.range(0, 10)
                                .mapToObj(i -> String.format("(0, %d, %d)", i, i))
                                .collect(Collectors.joining(",")));
        sql(
                "INSERT INTO T_PARTITION_REFRESH VALUES "
                        + IntStream.range(10, 20)
                                .mapToObj(i -> String.format("(1, %d, %d)", i, i))
                                .collect(Collectors.joining(",")));
        buildBTreeIndexForTable("T_PARTITION_REFRESH", "idx");

        FileStoreTable table = paimonTable("T_PARTITION_REFRESH");
        Map<String, List<IndexManifestEntry>> initial = btreeEntriesByRange(table);
        assertThat(initial.keySet()).containsExactlyInAnyOrder("0:9", "10:19");
        Set<String> firstPartitionFiles = fileNames(initial.get("0:9"));
        Set<String> secondPartitionFiles = fileNames(initial.get("10:19"));
        assertThat(initial.get("0:9"))
                .allSatisfy(entry -> assertThat(entry.partition().getInt(0)).isZero());
        assertThat(initial.get("10:19"))
                .allSatisfy(entry -> assertThat(entry.partition().getInt(0)).isEqualTo(1));

        sql("CREATE TABLE S_PARTITION_REFRESH (id INT, idx INT)");
        sql("INSERT INTO S_PARTITION_REFRESH VALUES (11, 1011)");
        sql(
                "CALL sys.data_evolution_merge_into("
                        + "'default.T_PARTITION_REFRESH', '', '', 'S_PARTITION_REFRESH', "
                        + "'T_PARTITION_REFRESH.id=S_PARTITION_REFRESH.id', "
                        + "'idx=S_PARTITION_REFRESH.idx', 2)");
        long updateSnapshotId =
                paimonTable("T_PARTITION_REFRESH").snapshotManager().latestSnapshot().id();

        buildBTreeIndexForTable("T_PARTITION_REFRESH", "idx");
        table = paimonTable("T_PARTITION_REFRESH");
        assertThat(table.snapshotManager().latestSnapshot().id()).isEqualTo(updateSnapshotId + 1);
        Map<String, List<IndexManifestEntry>> refreshed = btreeEntriesByRange(table);
        assertThat(fileNames(refreshed.get("0:9"))).isEqualTo(firstPartitionFiles);
        assertThat(fileNames(refreshed.get("10:19")))
                .doesNotContainAnyElementsOf(secondPartitionFiles);
        assertThat(sql("SELECT pt, id FROM T_PARTITION_REFRESH WHERE idx = 1011"))
                .containsOnly(Row.of(1, 11));
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

    private void buildBTreeIndexForTable(String tableName, String indexColumn) {
        sql(
                "CALL sys.create_global_index(`table` => 'default.%s', index_column => '%s', index_type => 'btree')",
                tableName, indexColumn);
    }

    private void buildBitmapIndexForTable(String tableName, String indexColumn) {
        sql(
                "CALL sys.create_global_index(`table` => 'default.%s', "
                        + "index_column => '%s', index_type => 'bitmap', "
                        + "options => 'sorted-index.records-per-range=20')",
                tableName, indexColumn);
    }

    private List<IndexManifestEntry> indexEntries(FileStoreTable table, String indexType) {
        return table.store().newIndexFileHandler().scanEntries().stream()
                .filter(entry -> indexType.equals(entry.indexFile().indexType()))
                .collect(Collectors.toList());
    }

    private Map<String, List<IndexManifestEntry>> btreeEntriesByRange(FileStoreTable table) {
        return table.store().newIndexFileHandler().scanEntries().stream()
                .filter(entry -> "btree".equals(entry.indexFile().indexType()))
                .collect(
                        Collectors.groupingBy(
                                entry ->
                                        entry.indexFile().globalIndexMeta().rowRangeStart()
                                                + ":"
                                                + entry.indexFile()
                                                        .globalIndexMeta()
                                                        .rowRangeEnd()));
    }

    private List<IndexManifestEntry> flatten(Map<String, List<IndexManifestEntry>> entriesByRange) {
        return entriesByRange.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private Set<String> fileNames(List<IndexManifestEntry> entries) {
        return entries.stream()
                .map(entry -> entry.indexFile().fileName())
                .collect(Collectors.toSet());
    }

    @Test
    void testBTreeIndexWithSingleRangeAndParallelWriters() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_SINGLE_RANGE_PARALLEL (id INT, name STRING) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");
        String values =
                IntStream.range(0, 2_000)
                        .mapToObj(i -> String.format("(%s, %s)", i, "'name_" + i + "'"))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T_SINGLE_RANGE_PARALLEL VALUES " + values);
        sql(
                "CALL sys.create_global_index(`table` => 'default.T_SINGLE_RANGE_PARALLEL', "
                        + "index_column => 'id', index_type => 'btree', "
                        + "options => 'btree-index.records-per-range=100;"
                        + "btree-index.build.max-parallelism=4')");

        FileStoreTable table = paimonTable("T_SINGLE_RANGE_PARALLEL");
        List<IndexFileMeta> btreeEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .map(IndexManifestEntry::indexFile)
                        .filter(f -> "btree".equals(f.indexType()))
                        .collect(Collectors.toList());

        long totalRowCount = btreeEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(btreeEntries).hasSizeGreaterThan(1);
        assertThat(totalRowCount).isEqualTo(2_000L);

        assertThat(sql("SELECT * FROM T_SINGLE_RANGE_PARALLEL WHERE id = 1500"))
                .containsOnly(Row.of(1500, "name_1500"));
    }

    @Test
    void testBTreeIndexWithManyPartitions() throws Catalog.TableNotExistException {
        int numPartitions = 50;
        sql(
                "CREATE TABLE T_MANY_PT (pt INT, id INT, name STRING) PARTITIONED BY (pt) WITH ("
                        + "'global-index.enabled' = 'true', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true'"
                        + ")");

        for (int p = 0; p < numPartitions; p++) {
            insertPartitionRows("T_MANY_PT", p, p * 2, 2, "r_");
        }

        buildBTreeIndexForTable("T_MANY_PT", "id");

        FileStoreTable table = paimonTable("T_MANY_PT");
        long totalRowCount =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(e -> "btree".equals(e.indexFile().indexType()))
                        .map(IndexManifestEntry::indexFile)
                        .mapToLong(IndexFileMeta::rowCount)
                        .sum();
        assertThat(totalRowCount).isEqualTo((long) numPartitions * 2);
    }

    @Test
    void testUnionDoesNotStackOverflow() throws InterruptedException {
        int totalUnions = 1000;
        long stackSize = 512 * 1024; // Flink JM default

        // Chained union: result = result.union(new) — causes StackOverflowError
        AtomicReference<Throwable> chainedError = new AtomicReference<>();
        Thread chainedThread =
                new Thread(
                        null,
                        () -> {
                            try {
                                StreamExecutionEnvironment env =
                                        StreamExecutionEnvironment.getExecutionEnvironment();
                                DataStream<String> all = null;
                                for (int i = 0; i < totalUnions; i++) {
                                    DataStream<String> s = env.fromElements("item-" + i);
                                    all = all == null ? s : all.union(s);
                                }
                                all.print();
                                env.getExecutionPlan();
                            } catch (Throwable t) {
                                chainedError.set(t);
                            }
                        },
                        "chained-union-test",
                        stackSize);
        chainedThread.start();
        chainedThread.join();
        assertThat(chainedError.get()).isInstanceOf(StackOverflowError.class);

        // Flat union: first.union(rest...) — no overflow at same stack size
        AtomicReference<Throwable> flatError = new AtomicReference<>();
        Thread flatThread =
                new Thread(
                        null,
                        () -> {
                            try {
                                StreamExecutionEnvironment env =
                                        StreamExecutionEnvironment.getExecutionEnvironment();
                                @SuppressWarnings("unchecked")
                                DataStream<String>[] streams = new DataStream[totalUnions];
                                for (int i = 0; i < totalUnions; i++) {
                                    streams[i] = env.fromElements("item-" + i);
                                }
                                @SuppressWarnings("unchecked")
                                DataStream<String>[] rest = new DataStream[totalUnions - 1];
                                System.arraycopy(streams, 1, rest, 0, totalUnions - 1);
                                streams[0].union(rest).print();
                                env.getExecutionPlan();
                            } catch (Throwable t) {
                                flatError.set(t);
                            }
                        },
                        "flat-union-test",
                        stackSize);
        flatThread.start();
        flatThread.join();
        assertThat(flatError.get()).isNull();
    }
}
