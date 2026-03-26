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

package org.apache.paimon.separated;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.CLUSTERING_COLUMNS;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.CoreOptions.PK_CLUSTERING_OVERRIDE;
import static org.apache.paimon.CoreOptions.SORT_SPILL_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;

class ClusteringTableTest {

    @TempDir public java.nio.file.Path tempPath;

    private Catalog catalog;
    private Table table;
    private IOManager ioManager;

    @BeforeEach
    public void beforeEach() throws Exception {
        Path warehouse = new Path(tempPath.toString());
        this.catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        catalog.createDatabase("default", true);
        Identifier identifier = Identifier.create("default", "t");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .option(DELETION_VECTORS_ENABLED.key(), "true")
                        .option(BUCKET.key(), "2")
                        .option(CLUSTERING_COLUMNS.key(), "b")
                        .option(PK_CLUSTERING_OVERRIDE.key(), "true")
                        .build();
        catalog.createTable(identifier, schema, false);
        this.table = catalog.getTable(identifier);
        this.ioManager = IOManager.create(tempPath.toString());
    }

    @Test
    public void testSameKeysMultipleTimesAcrossCommits() throws Exception {
        // Write same keys multiple times across different commits
        writeRows(table, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
        writeRows(table, Arrays.asList(GenericRow.of(1, 11), GenericRow.of(3, 30)));
        writeRows(table, Arrays.asList(GenericRow.of(2, 22), GenericRow.of(4, 40)));
        writeRows(table, Arrays.asList(GenericRow.of(1, 12), GenericRow.of(2, 23)));
        writeRows(table, Arrays.asList(GenericRow.of(3, 33), GenericRow.of(4, 44)));

        // Should see latest values for each key (deduplicate mode)
        assertThat(readRows(table))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 12),
                        GenericRow.of(2, 23),
                        GenericRow.of(3, 33),
                        GenericRow.of(4, 44));
    }

    @Test
    public void testNormal() throws Exception {
        List<GenericRow> input;

        input = Arrays.asList(GenericRow.of(1, 11), GenericRow.of(2, 22));
        writeRows(input);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(input);

        input = Arrays.asList(GenericRow.of(1, 111), GenericRow.of(2, 222));
        writeRows(input);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(input);

        input = Arrays.asList(GenericRow.of(1, 1111), GenericRow.of(2, 2222));
        writeRows(input);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(input);
    }

    /** Test overlapping clustering ranges from multiple commits should be merged correctly. */
    @Test
    public void testOverlappingClusteringRanges() throws Exception {
        // Commit 1: clustering values 10-20
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Commit 2: clustering values 15-25 (overlaps with commit 1)
        writeRows(Arrays.asList(GenericRow.of(3, 15), GenericRow.of(4, 25)));

        // Commit 3: clustering values 18-22 (overlaps with both)
        writeRows(Arrays.asList(GenericRow.of(5, 18), GenericRow.of(6, 22)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 15),
                        GenericRow.of(4, 25),
                        GenericRow.of(5, 18),
                        GenericRow.of(6, 22));
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test non-overlapping clustering ranges should remain separate (reduce IO amplification). */
    @Test
    public void testNonOverlappingClusteringRanges() throws Exception {
        // Commit 1: clustering values 10-20
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Commit 2: clustering values 50-60 (no overlap with commit 1)
        writeRows(Arrays.asList(GenericRow.of(3, 50), GenericRow.of(4, 60)));

        // Commit 3: clustering values 100-110 (no overlap with others)
        writeRows(Arrays.asList(GenericRow.of(5, 100), GenericRow.of(6, 110)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 50),
                        GenericRow.of(4, 60),
                        GenericRow.of(5, 100),
                        GenericRow.of(6, 110));
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test updating the same key multiple times with different clustering values. */
    @Test
    public void testMultipleUpdatesToSameKey() throws Exception {
        // Initial write
        writeRows(Arrays.asList(GenericRow.of(1, 100), GenericRow.of(2, 200)));
        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 100), GenericRow.of(2, 200));

        // Update key 1 with different clustering value
        writeRows(Arrays.asList(GenericRow.of(1, 50)));
        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 50), GenericRow.of(2, 200));

        // Update key 1 again
        writeRows(Arrays.asList(GenericRow.of(1, 150)));
        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 150), GenericRow.of(2, 200));

        // Update key 2
        writeRows(Arrays.asList(GenericRow.of(2, 75)));
        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 150), GenericRow.of(2, 75));
    }

    /** Test mixed overlapping and non-overlapping sections. */
    @Test
    public void testMixedOverlapAndNonOverlap() throws Exception {
        // Section A: 10-30
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 30)));

        // Section A extended: 20-40 (overlaps with Section A)
        writeRows(Arrays.asList(GenericRow.of(3, 20), GenericRow.of(4, 40)));

        // Section B: 100-120 (no overlap)
        writeRows(Arrays.asList(GenericRow.of(5, 100), GenericRow.of(6, 120)));

        // Section B extended: 110-130 (overlaps with Section B)
        writeRows(Arrays.asList(GenericRow.of(7, 110), GenericRow.of(8, 130)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 30),
                        GenericRow.of(3, 20),
                        GenericRow.of(4, 40),
                        GenericRow.of(5, 100),
                        GenericRow.of(6, 120),
                        GenericRow.of(7, 110),
                        GenericRow.of(8, 130));
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test large number of writes to trigger multiple compactions. */
    @Test
    public void testManyWrites() throws Exception {
        List<GenericRow> allRows = new ArrayList<>();

        // Write 10 batches with different clustering ranges
        for (int batch = 0; batch < 10; batch++) {
            List<GenericRow> batchRows = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                int pk = batch * 5 + i;
                int clusteringValue = (batch % 3) * 100 + i * 10; // Creates some overlapping ranges
                batchRows.add(GenericRow.of(pk, clusteringValue));
            }
            writeRows(batchRows);
            allRows.addAll(batchRows);
        }

        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(allRows);
    }

    /** Test updates that change clustering column causing record to move between sections. */
    @Test
    public void testUpdateChangesClusteringSection() throws Exception {
        // Initial: key 1 in section [10-20]
        writeRows(Arrays.asList(GenericRow.of(1, 15), GenericRow.of(2, 18)));

        // Update: key 1 moves to section [100-120] (different section)
        writeRows(Arrays.asList(GenericRow.of(1, 110)));

        // Key 1 should now have clustering value 110, key 2 stays at 18
        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 110), GenericRow.of(2, 18));

        // Another update: key 1 moves back to a section near key 2
        writeRows(Arrays.asList(GenericRow.of(1, 20)));

        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 20), GenericRow.of(2, 18));
    }

    /** Test single row write and read. */
    @Test
    public void testSingleRow() throws Exception {
        writeRows(Arrays.asList(GenericRow.of(1, 100)));
        assertThat(readRows()).containsExactly(GenericRow.of(1, 100));

        // Update single row
        writeRows(Arrays.asList(GenericRow.of(1, 200)));
        assertThat(readRows()).containsExactly(GenericRow.of(1, 200));
    }

    /** Test adjacent but non-overlapping ranges (boundary case). */
    @Test
    public void testAdjacentNonOverlappingRanges() throws Exception {
        // Range [10, 20]
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Range [21, 30] - adjacent but not overlapping (21 > 20)
        writeRows(Arrays.asList(GenericRow.of(3, 21), GenericRow.of(4, 30)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 21),
                        GenericRow.of(4, 30));
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test exact boundary overlap (max of one equals min of another). */
    @Test
    public void testExactBoundaryOverlap() throws Exception {
        // Range [10, 20]
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Range [20, 30] - overlaps exactly at boundary (20 >= 20)
        writeRows(Arrays.asList(GenericRow.of(3, 20), GenericRow.of(4, 30)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 20),
                        GenericRow.of(4, 30));
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test same clustering value for all records. */
    @Test
    public void testSameClusteringValue() throws Exception {
        writeRows(Arrays.asList(GenericRow.of(1, 50), GenericRow.of(2, 50)));
        writeRows(Arrays.asList(GenericRow.of(3, 50), GenericRow.of(4, 50)));
        writeRows(Arrays.asList(GenericRow.of(5, 50)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 50),
                        GenericRow.of(2, 50),
                        GenericRow.of(3, 50),
                        GenericRow.of(4, 50),
                        GenericRow.of(5, 50));
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test delete by writing a key then updating to see old value is gone. */
    @Test
    public void testDeletionVectorCorrectness() throws Exception {
        // Write initial data
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Write same keys with new values multiple times
        for (int i = 0; i < 5; i++) {
            writeRows(Arrays.asList(GenericRow.of(1, 10 + i * 10), GenericRow.of(2, 20 + i * 10)));
        }

        // Should only see the latest values
        assertThat(readRows())
                .containsExactlyInAnyOrder(GenericRow.of(1, 50), GenericRow.of(2, 60));
    }

    /**
     * Test that bootstrap correctly rebuilds the key index via bulkLoad from existing sorted files.
     *
     * <p>Each writeRows() call creates a new writer (and thus a new ClusteringCompactManager),
     * which calls {@code keyIndex.bootstrap(restoreFiles)}. The bootstrap method reads all level >
     * 0 files, sorts them externally, and bulk-loads into the LSM KV DB — bypassing the normal
     * put-per-entry path. This test verifies that the bulkLoad-based index is correct by checking
     * deduplication across multiple commits with overlapping keys.
     */
    @Test
    public void testBootstrapBulkLoadIndex() throws Exception {
        // Commit 1: write initial data → compaction produces level > 0 sorted files
        writeRows(
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 30),
                        GenericRow.of(4, 40),
                        GenericRow.of(5, 50)));

        // Commit 2: new writer bootstraps index from level > 0 files via bulkLoad,
        // then writes overlapping keys — updateIndex must find existing entries in the
        // bulkLoaded index to generate correct deletion vectors
        writeRows(
                Arrays.asList(GenericRow.of(1, 100), GenericRow.of(3, 300), GenericRow.of(5, 500)));

        // Verify dedup: keys 1,3,5 updated; keys 2,4 unchanged
        assertThat(readRows())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 100),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 300),
                        GenericRow.of(4, 40),
                        GenericRow.of(5, 500));

        // Commit 3: another bootstrap from the updated sorted files,
        // verifies bulkLoad works correctly after files have been rewritten
        writeRows(
                Arrays.asList(GenericRow.of(2, 200), GenericRow.of(4, 400), GenericRow.of(6, 600)));

        assertThat(readRows())
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 100),
                        GenericRow.of(2, 200),
                        GenericRow.of(3, 300),
                        GenericRow.of(4, 400),
                        GenericRow.of(5, 500),
                        GenericRow.of(6, 600));
    }

    // ==================== Clustering Column Filter Tests ====================

    /** Test that equality filter on clustering column skips irrelevant files in the scan plan. */
    @Test
    public void testClusteringColumnEqualityFilterSkipsFiles() throws Exception {
        // Write 3 commits with widely separated, non-overlapping b ranges
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
        writeRows(Arrays.asList(GenericRow.of(3, 100), GenericRow.of(4, 110)));
        writeRows(Arrays.asList(GenericRow.of(5, 1000), GenericRow.of(6, 1010)));

        // After compaction, expect at least 2 files with non-overlapping b ranges
        int totalFiles = countFiles(table, null);
        assertThat(totalFiles).isGreaterThanOrEqualTo(2);

        PredicateBuilder pb = new PredicateBuilder(table.rowType());

        // b = 1005 → only file(s) covering [1000, 1010] match, skip file(s) with smaller b
        assertThat(countFiles(table, pb.equal(1, 1005))).isLessThan(totalFiles);

        // b = 15 → only file(s) covering [10, 20] match, skip file(s) with larger b
        assertThat(countFiles(table, pb.equal(1, 15))).isLessThan(totalFiles);

        // b = 5000 → no file covers this value, should return 0 files
        assertThat(countFiles(table, pb.equal(1, 5000))).isEqualTo(0);
    }

    /** Test that range filter on clustering column skips irrelevant files in the scan plan. */
    @Test
    public void testClusteringColumnRangeFilterSkipsFiles() throws Exception {
        // Write 3 commits with widely separated, non-overlapping b ranges
        writeRows(Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
        writeRows(Arrays.asList(GenericRow.of(3, 100), GenericRow.of(4, 110)));
        writeRows(Arrays.asList(GenericRow.of(5, 1000), GenericRow.of(6, 1010)));

        // After compaction, expect at least 2 files with non-overlapping b ranges
        int totalFiles = countFiles(table, null);
        assertThat(totalFiles).isGreaterThanOrEqualTo(2);

        PredicateBuilder pb = new PredicateBuilder(table.rowType());

        // b > 500 → only file(s) covering [1000, 1010] match
        assertThat(countFiles(table, pb.greaterThan(1, 500))).isLessThan(totalFiles);

        // b < 50 → only file(s) covering [10, 20] match
        assertThat(countFiles(table, pb.lessThan(1, 50))).isLessThan(totalFiles);

        // 50 <= b <= 150 → only file(s) covering [100, 110] match
        Predicate rangeFilter =
                PredicateBuilder.and(pb.greaterOrEqual(1, 50), pb.lessOrEqual(1, 150));
        assertThat(countFiles(table, rangeFilter)).isLessThan(totalFiles);

        // b > 5 → all files match (all b values are > 5)
        assertThat(countFiles(table, pb.greaterThan(1, 5))).isEqualTo(totalFiles);
    }

    // ==================== First-Row Mode Tests ====================

    /** Test first-row mode keeps the first record when same key is written multiple times. */
    @Test
    public void testFirstRowBasic() throws Exception {
        Table firstRowTable = createFirstRowTable();

        // Write initial data
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 100), GenericRow.of(2, 200)));

        // Write same keys with different values - should be ignored (first-row keeps first)
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 999), GenericRow.of(2, 888)));

        // Should still see the first values
        assertThat(readRows(firstRowTable))
                .containsExactlyInAnyOrder(GenericRow.of(1, 100), GenericRow.of(2, 200));
    }

    /** Test first-row mode with multiple commits. */
    @Test
    public void testFirstRowMultipleCommits() throws Exception {
        Table firstRowTable = createFirstRowTable();

        // Commit 1: initial records
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Commit 2: new keys + duplicate keys
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 99), GenericRow.of(3, 30)));

        // Commit 3: more duplicates + new key
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(2, 88), GenericRow.of(4, 40)));

        // Key 1,2 should keep first values; key 3,4 are new
        assertThat(readRows(firstRowTable))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 30),
                        GenericRow.of(4, 40));
    }

    /** Test first-row mode with overlapping clustering ranges. */
    @Test
    public void testFirstRowOverlappingRanges() throws Exception {
        Table firstRowTable = createFirstRowTable();

        // Commit 1: clustering values 10-20
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Commit 2: overlapping range with duplicate key 1
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 15), GenericRow.of(3, 25)));

        // Key 1 should keep first value (10), key 2,3 are unique
        assertThat(readRows(firstRowTable))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10), GenericRow.of(2, 20), GenericRow.of(3, 25));
    }

    /** Test first-row mode behavior differs from deduplicate mode. */
    @Test
    public void testFirstRowVsDeduplicate() throws Exception {
        // Deduplicate mode table (default)
        writeRows(Arrays.asList(GenericRow.of(1, 100)));
        writeRows(Arrays.asList(GenericRow.of(1, 200)));
        // Deduplicate keeps last value
        assertThat(readRows()).containsExactly(GenericRow.of(1, 200));

        // First-row mode table
        Table firstRowTable = createFirstRowTable();
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 100)));
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 200)));
        // First-row keeps first value
        assertThat(readRows(firstRowTable)).containsExactly(GenericRow.of(1, 100));
    }

    /** Test first-row mode: updating clustering value should still keep the first record. */
    @Test
    public void testFirstRowUpdateChangesClusteringSection() throws Exception {
        Table firstRowTable = createFirstRowTable();

        // Initial: key 1 in section [10-20]
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 15), GenericRow.of(2, 18)));

        // Update: key 1 tries to move to section [100-120] - should be ignored
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 110)));

        // Key 1 should keep first value (15), key 2 stays at 18
        assertThat(readRows(firstRowTable))
                .containsExactlyInAnyOrder(GenericRow.of(1, 15), GenericRow.of(2, 18));

        // Another attempt to update key 1 - still ignored
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 20)));
        assertThat(readRows(firstRowTable))
                .containsExactlyInAnyOrder(GenericRow.of(1, 15), GenericRow.of(2, 18));
    }

    /** Test first-row mode DV correctness: rapid repeated writes mark new records, not old. */
    @Test
    public void testFirstRowDeletionVectorCorrectness() throws Exception {
        Table firstRowTable = createFirstRowTable();

        // Write initial data
        writeRows(firstRowTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Write same keys with new values multiple times - all should be ignored
        for (int i = 0; i < 5; i++) {
            writeRows(
                    firstRowTable,
                    Arrays.asList(GenericRow.of(1, 10 + i * 10), GenericRow.of(2, 20 + i * 10)));
        }

        // Should still see the very first values
        assertThat(readRows(firstRowTable))
                .containsExactlyInAnyOrder(GenericRow.of(1, 10), GenericRow.of(2, 20));
    }

    /** Test first-row mode with many writes to trigger compaction. */
    @Test
    public void testFirstRowManyWrites() throws Exception {
        Table firstRowTable = createFirstRowTable();

        // First commit: establish initial values for keys 0-4
        List<GenericRow> firstBatch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            firstBatch.add(GenericRow.of(i, i * 10));
        }
        writeRows(firstRowTable, firstBatch);

        // Subsequent commits: write same keys with different values
        for (int batch = 1; batch < 10; batch++) {
            List<GenericRow> batchRows = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                batchRows.add(GenericRow.of(i, i * 10 + batch * 100));
            }
            writeRows(firstRowTable, batchRows);
        }

        // Should only see the first values
        assertThat(readRows(firstRowTable)).containsExactlyInAnyOrderElementsOf(firstBatch);
    }

    // ==================== Spill Tests ====================

    /** Test first-row mode with spill: keeps first values despite many duplicate commits. */
    @Test
    public void testFirstRowSpillBasic() throws Exception {
        Table spillFirstRowTable = createFirstRowTableWithLowSpillThreshold();

        // Commit 1: initial records
        writeRows(spillFirstRowTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));

        // Commits 2-5: same keys with different values - all should be ignored
        writeRows(spillFirstRowTable, Arrays.asList(GenericRow.of(1, 99), GenericRow.of(3, 30)));
        writeRows(spillFirstRowTable, Arrays.asList(GenericRow.of(2, 88), GenericRow.of(4, 40)));
        writeRows(spillFirstRowTable, Arrays.asList(GenericRow.of(1, 77), GenericRow.of(2, 66)));
        writeRows(spillFirstRowTable, Arrays.asList(GenericRow.of(3, 55), GenericRow.of(4, 44)));

        // Key 1,2 should keep first values; key 3,4 keep their first appearance
        assertThat(readRows(spillFirstRowTable))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 30),
                        GenericRow.of(4, 40));
    }

    /** Test first-row mode with spill and many writes to stress test. */
    @Test
    public void testFirstRowSpillManyWrites() throws Exception {
        Table spillFirstRowTable = createFirstRowTableWithLowSpillThreshold();

        // First commit: establish initial values for keys 0-4
        List<GenericRow> firstBatch = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            firstBatch.add(GenericRow.of(i, i * 10));
        }
        writeRows(spillFirstRowTable, firstBatch);

        // Subsequent commits: write same keys with different values
        for (int batch = 1; batch < 10; batch++) {
            List<GenericRow> batchRows = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                batchRows.add(GenericRow.of(i, i * 10 + batch * 100));
            }
            writeRows(spillFirstRowTable, batchRows);
        }

        // Should only see the first values
        assertThat(readRows(spillFirstRowTable)).containsExactlyInAnyOrderElementsOf(firstBatch);
    }

    @Test
    public void testSameKeysMultipleTimesAcrossCommitsInSpill() throws Exception {
        Table spillTable = createTableWithLowSpillThreshold();
        // Write same keys multiple times across different commits
        writeRows(spillTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(1, 11), GenericRow.of(3, 30)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(2, 22), GenericRow.of(4, 40)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(1, 12), GenericRow.of(2, 23)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(3, 33), GenericRow.of(4, 44)));

        // Should see latest values for each key (deduplicate mode)
        assertThat(readRows(spillTable))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 12),
                        GenericRow.of(2, 23),
                        GenericRow.of(3, 33),
                        GenericRow.of(4, 44));
    }

    /**
     * Test row-based spill when merging many files. Uses a low spillThreshold to trigger spill
     * logic with fewer files.
     */
    @Test
    public void testSpillWithManyFiles() throws Exception {
        Table spillTable = createTableWithLowSpillThreshold();

        // Write many separate commits to create many files
        // Each commit creates a new file, and we want to exceed spillThreshold (set to 2)
        List<GenericRow> allRows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<GenericRow> batch =
                    Arrays.asList(
                            GenericRow.of(i * 2, i * 10), GenericRow.of(i * 2 + 1, i * 10 + 5));
            writeRows(spillTable, batch);
            allRows.addAll(batch);
        }

        // All rows should be readable after compaction with spill
        assertThat(readRows(spillTable)).containsExactlyInAnyOrderElementsOf(allRows);
    }

    /** Test spill with overlapping clustering ranges. */
    @Test
    public void testSpillWithOverlappingRanges() throws Exception {
        Table spillTable = createTableWithLowSpillThreshold();

        // Create files with overlapping clustering ranges to trigger merge
        writeRows(spillTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(3, 15), GenericRow.of(4, 25)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(5, 12), GenericRow.of(6, 22)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(7, 18), GenericRow.of(8, 28)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(9, 14), GenericRow.of(10, 24)));

        List<GenericRow> expected =
                Arrays.asList(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 15),
                        GenericRow.of(4, 25),
                        GenericRow.of(5, 12),
                        GenericRow.of(6, 22),
                        GenericRow.of(7, 18),
                        GenericRow.of(8, 28),
                        GenericRow.of(9, 14),
                        GenericRow.of(10, 24));
        assertThat(readRows(spillTable)).containsExactlyInAnyOrderElementsOf(expected);
    }

    /** Test spill with deduplication across separate compaction rounds. */
    @Test
    public void testSpillWithDeduplication() throws Exception {
        Table spillTable = createTableWithLowSpillThreshold();

        // First round: write initial data
        writeRows(spillTable, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(3, 30), GenericRow.of(4, 40)));

        // Verify initial data
        assertThat(readRows(spillTable))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 10),
                        GenericRow.of(2, 20),
                        GenericRow.of(3, 30),
                        GenericRow.of(4, 40));

        // Second round: update keys with new values
        writeRows(spillTable, Arrays.asList(GenericRow.of(1, 11), GenericRow.of(2, 22)));
        writeRows(spillTable, Arrays.asList(GenericRow.of(3, 33), GenericRow.of(4, 44)));

        // Should see latest values for each key
        assertThat(readRows(spillTable))
                .containsExactlyInAnyOrder(
                        GenericRow.of(1, 11),
                        GenericRow.of(2, 22),
                        GenericRow.of(3, 33),
                        GenericRow.of(4, 44));
    }

    // ==================== Stream Write with Frequent Compact Tests ====================

    /**
     * Test that frequent compact triggers during streaming writes don't corrupt data. This
     * exercises the {@code if (taskFuture != null) return} guard in {@code
     * ClusteringCompactManager.triggerCompaction}.
     */
    @Test
    public void testStreamWriteFrequentCompact() throws Exception {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        try (StreamTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                StreamTableCommit commit = writeBuilder.newCommit()) {
            long commitId = 0;
            for (int round = 0; round < 20; round++) {
                write.write(GenericRow.of(round, round * 10));
                // Trigger compact on every round for both buckets
                write.compact(BinaryRow.EMPTY_ROW, 0, false);
                write.compact(BinaryRow.EMPTY_ROW, 1, false);
                commit.commit(commitId, write.prepareCommit(false, commitId));
                commitId++;
            }

            // Final commit with waitCompaction to ensure all compactions finish
            commit.commit(commitId, write.prepareCommit(true, commitId));

            // All 20 distinct keys should be present
            List<GenericRow> expected = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                expected.add(GenericRow.of(i, i * 10));
            }
            assertThat(readRows()).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    /**
     * Test streaming writes with overlapping keys and frequent compaction. The {@code taskFuture !=
     * null} guard should safely skip redundant compaction triggers while still producing correct
     * deduplicated results.
     */
    @Test
    public void testStreamWriteFrequentCompactWithOverlappingKeys() throws Exception {
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        try (StreamTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                StreamTableCommit commit = writeBuilder.newCommit()) {
            long commitId = 0;
            for (int round = 0; round < 20; round++) {
                // Write overlapping keys: keys 0-4 are updated every round
                for (int i = 0; i < 5; i++) {
                    write.write(GenericRow.of(i, round * 100 + i));
                }
                write.compact(BinaryRow.EMPTY_ROW, 0, false);
                write.compact(BinaryRow.EMPTY_ROW, 1, false);
                commit.commit(commitId, write.prepareCommit(false, commitId));
                commitId++;
            }

            // Final wait for compaction
            commit.commit(commitId, write.prepareCommit(true, commitId));

            // Should see only the latest values from round 19
            assertThat(readRows())
                    .containsExactlyInAnyOrder(
                            GenericRow.of(0, 1900),
                            GenericRow.of(1, 1901),
                            GenericRow.of(2, 1902),
                            GenericRow.of(3, 1903),
                            GenericRow.of(4, 1904));
        }
    }

    private Table createFirstRowTableWithLowSpillThreshold() throws Exception {
        Identifier identifier = Identifier.create("default", "first_row_spill_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .option(DELETION_VECTORS_ENABLED.key(), "true")
                        .option(BUCKET.key(), "1")
                        .option(CLUSTERING_COLUMNS.key(), "b")
                        .option(PK_CLUSTERING_OVERRIDE.key(), "true")
                        .option(MERGE_ENGINE.key(), "first-row")
                        .option(SORT_SPILL_THRESHOLD.key(), "2")
                        .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private Table createTableWithLowSpillThreshold() throws Exception {
        Identifier identifier = Identifier.create("default", "spill_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .option(DELETION_VECTORS_ENABLED.key(), "true")
                        .option(BUCKET.key(), "1")
                        .option(CLUSTERING_COLUMNS.key(), "b")
                        .option(PK_CLUSTERING_OVERRIDE.key(), "true")
                        .option(SORT_SPILL_THRESHOLD.key(), "2") // Low threshold to trigger spill
                        .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private Table createFirstRowTable() throws Exception {
        Identifier identifier = Identifier.create("default", "first_row_table");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .option(DELETION_VECTORS_ENABLED.key(), "true")
                        .option(BUCKET.key(), "1")
                        .option(CLUSTERING_COLUMNS.key(), "b")
                        .option(PK_CLUSTERING_OVERRIDE.key(), "true")
                        .option(MERGE_ENGINE.key(), "first-row")
                        .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private void writeRows(List<GenericRow> rows) throws Exception {
        writeRows(table, rows);
    }

    private void writeRows(Table targetTable, List<GenericRow> rows) throws Exception {
        BatchWriteBuilder writeBuilder = targetTable.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<GenericRow> readRows() throws Exception {
        return readRows(table);
    }

    private List<GenericRow> readRows(Table targetTable) throws Exception {
        ReadBuilder readBuilder = targetTable.newReadBuilder();
        @SuppressWarnings("resource")
        CloseableIterator<InternalRow> iterator =
                readBuilder
                        .newRead()
                        .createReader(readBuilder.newScan().plan())
                        .toCloseableIterator();
        List<GenericRow> result = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalRow row = iterator.next();
            result.add(GenericRow.of(row.getInt(0), row.getInt(1)));
        }
        return result;
    }

    private int countFiles(Table targetTable, Predicate filter) {
        ReadBuilder readBuilder = targetTable.newReadBuilder();
        if (filter != null) {
            readBuilder.withFilter(filter);
        }
        return readBuilder.newScan().plan().splits().stream()
                .mapToInt(split -> ((DataSplit) split).dataFiles().size())
                .sum();
    }
}
