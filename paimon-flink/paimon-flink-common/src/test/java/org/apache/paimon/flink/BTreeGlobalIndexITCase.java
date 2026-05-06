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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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

    private void buildBTreeIndexForTable(String tableName, String indexColumn) {
        sql(
                "CALL sys.create_global_index(`table` => 'default.%s', index_column => '%s', index_type => 'btree')",
                tableName, indexColumn);
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
