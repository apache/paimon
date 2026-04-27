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
import org.apache.paimon.flink.globalindex.GenericGlobalIndexBuilder;
import org.apache.paimon.flink.globalindex.GenericIndexTopoBuilder;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for Lumina vector global index via Flink procedure. */
public class LuminaVectorGlobalIndexITCase extends CatalogITCaseBase {

    private static final String INDEX_TYPE = "lumina";

    @BeforeAll
    static void checkLuminaAvailable() {
        try {
            Class<?> luminaClass = Class.forName("org.aliyun.lumina.Lumina");
            Method isLoaded = luminaClass.getMethod("isLibraryLoaded");
            if (!(boolean) isLoaded.invoke(null)) {
                Method loadLib = luminaClass.getMethod("loadLibrary");
                loadLib.invoke(null);
            }
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Lumina native library not available: " + e.getMessage());
        }
    }

    @Test
    public void testLuminaVectorIndex() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        sql("INSERT INTO T VALUES " + vectorValues(0, 100, 3));

        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);

        assertThat(vectorEntries).isNotEmpty();
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(100L);
    }

    @Test
    public void testMultiBatchInsert() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_MULTI (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        // Multiple batch inserts create separate data files with contiguous row ranges
        sql("INSERT INTO T_MULTI VALUES " + vectorValues(0, 50, 3));
        sql("INSERT INTO T_MULTI VALUES " + vectorValues(50, 50, 3));

        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_MULTI', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T_MULTI");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);

        assertThat(vectorEntries).isNotEmpty();
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(100L);

        // Each index file should produce exactly one file per contiguous row range
        for (IndexFileMeta meta : vectorEntries) {
            assertThat(meta.globalIndexMeta()).isNotNull();
        }
    }

    @Test
    public void testSharding() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_SHARD (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        sql("INSERT INTO T_SHARD VALUES " + vectorValues(0, 100, 3));

        // Build index with small shard size to force multiple shards
        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_SHARD', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "', "
                        + "options => 'global-index.row-count-per-shard=30')");

        FileStoreTable table = paimonTable("T_SHARD");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);

        // With 100 rows and 30 per shard, expect at least 3 shards (30+30+30+10)
        assertThat(vectorEntries.size()).isGreaterThanOrEqualTo(3);
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(100L);

        // Each shard should have its own distinct sub-range
        for (IndexFileMeta meta : vectorEntries) {
            assertThat(meta.globalIndexMeta()).isNotNull();
            assertThat(meta.globalIndexMeta().rowRangeEnd())
                    .isGreaterThanOrEqualTo(meta.globalIndexMeta().rowRangeStart());
        }
    }

    @Test
    public void testPartitionedTable() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_PART (id INT, pt STRING, v ARRAY<FLOAT>) "
                        + "PARTITIONED BY (pt) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        // Insert data into two partitions
        String values1 =
                IntStream.range(0, 50)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, 'a', ARRAY[CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT)])",
                                                i, i, i + 1, i + 2))
                        .collect(Collectors.joining(","));
        String values2 =
                IntStream.range(50, 100)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, 'b', ARRAY[CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT)])",
                                                i, i, i + 1, i + 2))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T_PART VALUES " + values1);
        sql("INSERT INTO T_PART VALUES " + values2);

        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_PART', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T_PART");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);

        assertThat(vectorEntries).isNotEmpty();
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(100L);
    }

    @Test
    public void testPartitionFilter() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_PF (id INT, pt STRING, v ARRAY<FLOAT>) "
                        + "PARTITIONED BY (pt) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        String valuesA =
                IntStream.range(0, 30)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, 'a', ARRAY[CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT)])",
                                                i, i, i + 1, i + 2))
                        .collect(Collectors.joining(","));
        String valuesB =
                IntStream.range(30, 60)
                        .mapToObj(
                                i ->
                                        String.format(
                                                "(%d, 'b', ARRAY[CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT), CAST(%d.0 AS FLOAT)])",
                                                i, i, i + 1, i + 2))
                        .collect(Collectors.joining(","));
        sql("INSERT INTO T_PF VALUES " + valuesA);
        sql("INSERT INTO T_PF VALUES " + valuesB);

        // Only build index for partition 'a'
        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_PF', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "', "
                        + "partitions => 'pt=a')");

        FileStoreTable table = paimonTable("T_PF");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);

        // Only partition 'a' should be indexed (30 rows)
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(30L);
    }

    @Test
    public void testEmptyTable() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_EMPTY (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        // Building index on empty table should not fail
        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_EMPTY', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T_EMPTY");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);
        assertThat(vectorEntries).isEmpty();
    }

    @Test
    public void testRowTrackingRequired() {
        sql(
                "CREATE TABLE T_NO_RT (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.create_global_index("
                                                + "`table` => 'default.T_NO_RT', "
                                                + "index_column => 'v', "
                                                + "index_type => '"
                                                + INDEX_TYPE
                                                + "')"))
                .hasMessageContaining("row-tracking.enabled=true");
    }

    @Test
    public void testInvalidColumn() {
        sql(
                "CREATE TABLE T_BAD_COL (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.create_global_index("
                                                + "`table` => 'default.T_BAD_COL', "
                                                + "index_column => 'nonexistent', "
                                                + "index_type => '"
                                                + INDEX_TYPE
                                                + "')"))
                .hasMessageContaining("does not exist");
    }

    @Test
    public void testGlobalIndexMeta() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_META (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        sql("INSERT INTO T_META VALUES " + vectorValues(0, 50, 3));

        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_META', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T_META");
        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);

        assertThat(vectorEntries).isNotEmpty();
        for (IndexFileMeta meta : vectorEntries) {
            assertThat(meta.indexType()).isEqualTo(INDEX_TYPE);
            assertThat(meta.globalIndexMeta()).isNotNull();
            assertThat(meta.globalIndexMeta().rowRangeStart()).isGreaterThanOrEqualTo(0);
            assertThat(meta.globalIndexMeta().rowRangeEnd())
                    .isGreaterThanOrEqualTo(meta.globalIndexMeta().rowRangeStart());
            assertThat(meta.fileSize()).isGreaterThan(0);
        }
    }

    @Test
    public void testCustomBuilderSupplier() throws Exception {
        sql(
                "CREATE TABLE T_CUSTOM (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        // Two separate inserts create two data files with different firstRowId ranges
        sql("INSERT INTO T_CUSTOM VALUES " + vectorValues(0, 50, 3));
        sql("INSERT INTO T_CUSTOM VALUES " + vectorValues(50, 50, 3));

        FileStoreTable table = paimonTable("T_CUSTOM");
        Options mergedOptions = new Options(table.options());

        // Use a custom builder that only indexes the first batch (firstRowId < 50)
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        boolean hasIndex =
                GenericIndexTopoBuilder.buildIndex(
                        env,
                        () ->
                                new GenericGlobalIndexBuilder(table) {
                                    @Override
                                    public List<ManifestEntry> scan() {
                                        return super.scan().stream()
                                                .filter(
                                                        e ->
                                                                e.file().firstRowId() != null
                                                                        && e.file().firstRowId()
                                                                                < 50)
                                                .collect(Collectors.toList());
                                    }
                                },
                        table,
                        "v",
                        INDEX_TYPE,
                        null,
                        mergedOptions);
        assertThat(hasIndex).isTrue();
        env.execute("test-partial-index");

        List<IndexFileMeta> vectorEntries = getVectorIndexFiles(table);
        assertThat(vectorEntries).isNotEmpty();
        // Only the first batch (50 rows) should be indexed
        long totalRowCount = vectorEntries.stream().mapToLong(IndexFileMeta::rowCount).sum();
        assertThat(totalRowCount).isEqualTo(50L);
    }

    @Test
    public void testDeletedIndexEntriesDefault() throws Catalog.TableNotExistException {
        sql(
                "CREATE TABLE T_DEL_DEFAULT (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        FileStoreTable table = paimonTable("T_DEL_DEFAULT");
        GenericGlobalIndexBuilder builder = new GenericGlobalIndexBuilder(table);
        assertThat(builder.deletedIndexEntries()).isEmpty();
    }

    @Test
    public void testDeletedIndexEntriesAtomicCommit() throws Exception {
        sql(
                "CREATE TABLE T_DEL_ATOMIC (id INT, v ARRAY<FLOAT>) WITH ("
                        + "'bucket' = '-1', "
                        + "'row-tracking.enabled' = 'true', "
                        + "'data-evolution.enabled' = 'true', "
                        + "'lumina.index.dimension' = '3', "
                        + "'lumina.distance.metric' = 'l2'"
                        + ")");

        sql("INSERT INTO T_DEL_ATOMIC VALUES " + vectorValues(0, 100, 3));

        // First build: create initial index
        sql(
                "CALL sys.create_global_index("
                        + "`table` => 'default.T_DEL_ATOMIC', "
                        + "index_column => 'v', "
                        + "index_type => '"
                        + INDEX_TYPE
                        + "')");

        FileStoreTable table = paimonTable("T_DEL_ATOMIC");
        List<IndexManifestEntry> oldEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(e -> INDEX_TYPE.equals(e.indexFile().indexType()))
                        .collect(Collectors.toList());
        assertThat(oldEntries).isNotEmpty();

        List<String> oldFileNames =
                oldEntries.stream().map(e -> e.indexFile().fileName()).collect(Collectors.toList());

        // Second build: use a custom builder that reports old entries as deletedIndexEntries
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        boolean hasIndex =
                GenericIndexTopoBuilder.buildIndex(
                        env,
                        () ->
                                new GenericGlobalIndexBuilder(table) {
                                    @Override
                                    public List<IndexManifestEntry> deletedIndexEntries() {
                                        return oldEntries;
                                    }
                                },
                        table,
                        "v",
                        INDEX_TYPE,
                        null,
                        new Options(table.options()));
        assertThat(hasIndex).isTrue();
        env.execute("test-rebuild-index");

        // After the atomic commit, old index files should be deleted and new ones created
        List<IndexManifestEntry> newEntries =
                table.store().newIndexFileHandler().scanEntries().stream()
                        .filter(e -> INDEX_TYPE.equals(e.indexFile().indexType()))
                        .collect(Collectors.toList());

        assertThat(newEntries).isNotEmpty();
        long totalRowCount = newEntries.stream().mapToLong(e -> e.indexFile().rowCount()).sum();
        assertThat(totalRowCount).isEqualTo(100L);

        // New files should be different from old files (old ones were deleted)
        List<String> newFileNames =
                newEntries.stream().map(e -> e.indexFile().fileName()).collect(Collectors.toList());
        assertThat(newFileNames).doesNotContainAnyElementsOf(oldFileNames);
    }

    // -- Helpers --

    private List<IndexFileMeta> getVectorIndexFiles(FileStoreTable table) {
        return table.store().newIndexFileHandler().scanEntries().stream()
                .map(IndexManifestEntry::indexFile)
                .filter(f -> INDEX_TYPE.equals(f.indexType()))
                .collect(Collectors.toList());
    }

    private static String vectorValues(int startId, int count, int dim) {
        return IntStream.range(startId, startId + count)
                .mapToObj(
                        i -> {
                            StringBuilder sb = new StringBuilder();
                            sb.append("(").append(i).append(", ARRAY[");
                            for (int d = 0; d < dim; d++) {
                                if (d > 0) {
                                    sb.append(", ");
                                }
                                sb.append("CAST(").append(i + d).append(".0 AS FLOAT)");
                            }
                            sb.append("])");
                            return sb.toString();
                        })
                .collect(Collectors.joining(","));
    }
}
