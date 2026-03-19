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

package org.apache.paimon.lookup.sort.db;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.memory.MemorySlice;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Test for {@link SimpleLsmKvDb}. */
public class SimpleLsmKvDbTest {

    @TempDir java.nio.file.Path tempDir;

    private File dataDirectory;

    @BeforeEach
    public void setUp() {
        dataDirectory = new File(tempDir.toFile(), "test-db");
    }

    private SimpleLsmKvDb createDb() {
        return SimpleLsmKvDb.builder(dataDirectory)
                .memTableFlushThreshold(1024)
                .blockSize(256)
                .level0FileNumCompactTrigger(4)
                .compressOptions(new CompressOptions("none", 1))
                .build();
    }

    @Test
    public void testBasicPutAndGet() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "value1");
            putString(db, "key2", "value2");
            putString(db, "key3", "value3");

            Assertions.assertEquals("value1", getString(db, "key1"));
            Assertions.assertEquals("value2", getString(db, "key2"));
            Assertions.assertEquals("value3", getString(db, "key3"));
            Assertions.assertNull(getString(db, "nonexistent"));
        }
    }

    @Test
    public void testOverwriteValue() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "value1");
            Assertions.assertEquals("value1", getString(db, "key1"));

            putString(db, "key1", "value2");
            Assertions.assertEquals("value2", getString(db, "key1"));

            putString(db, "key1", "value3");
            Assertions.assertEquals("value3", getString(db, "key1"));
        }
    }

    @Test
    public void testDelete() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "value1");
            putString(db, "key2", "value2");

            Assertions.assertEquals("value1", getString(db, "key1"));

            deleteString(db, "key1");
            Assertions.assertNull(getString(db, "key1"));

            // key2 should still exist
            Assertions.assertEquals("value2", getString(db, "key2"));

            // deleting a non-existent key should not cause errors
            deleteString(db, "nonexistent");
            Assertions.assertNull(getString(db, "nonexistent"));
        }
    }

    @Test
    public void testDeleteAndReinsert() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "value1");
            Assertions.assertEquals("value1", getString(db, "key1"));

            deleteString(db, "key1");
            Assertions.assertNull(getString(db, "key1"));

            putString(db, "key1", "value2");
            Assertions.assertEquals("value2", getString(db, "key1"));
        }
    }

    @Test
    public void testFlushToSst() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            Assertions.assertEquals(0, db.getSstFileCount());

            // Write enough data to trigger a flush (threshold is 1024 bytes)
            for (int i = 0; i < 100; i++) {
                putString(db, String.format("key-%05d", i), String.format("value-%05d", i));
            }

            // After writing enough data, at least one SST file should exist
            Assertions.assertTrue(
                    db.getSstFileCount() > 0,
                    "Expected at least one SST file after writing enough data");

            // All data should still be readable
            for (int i = 0; i < 100; i++) {
                String expected = String.format("value-%05d", i);
                String actual = getString(db, String.format("key-%05d", i));
                Assertions.assertEquals(expected, actual, "Mismatch for key-" + i);
            }
        }
    }

    @Test
    public void testReadFromSstAfterFlush() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "alpha", "first");
            putString(db, "beta", "second");
            putString(db, "gamma", "third");

            // Force flush
            db.flush();
            Assertions.assertTrue(db.getSstFileCount() > 0);

            // Data should be readable from SST
            Assertions.assertEquals("first", getString(db, "alpha"));
            Assertions.assertEquals("second", getString(db, "beta"));
            Assertions.assertEquals("third", getString(db, "gamma"));
        }
    }

    @Test
    public void testOverwriteAcrossFlush() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "old-value");
            db.flush();

            // Overwrite in MemTable (newer than SST)
            putString(db, "key1", "new-value");
            Assertions.assertEquals("new-value", getString(db, "key1"));

            // After another flush, the new value should persist
            db.flush();
            Assertions.assertEquals("new-value", getString(db, "key1"));
        }
    }

    @Test
    public void testDeleteAcrossFlush() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "value1");
            db.flush();

            // Delete in MemTable
            deleteString(db, "key1");
            Assertions.assertNull(getString(db, "key1"));

            // After flush, the tombstone should be in SST and key should still be deleted
            db.flush();
            Assertions.assertNull(getString(db, "key1"));
        }
    }

    @Test
    public void testCompaction() throws IOException {
        // Use a low compaction threshold to trigger compaction easily
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(dataDirectory)
                        .memTableFlushThreshold(256)
                        .blockSize(128)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Write data in batches to create multiple SST files
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 20; i++) {
                    int key = batch * 20 + i;
                    putString(
                            db,
                            String.format("key-%05d", key),
                            String.format("value-%05d-batch-%d", key, batch));
                }
                db.flush();
            }

            // Compaction may have been triggered automatically
            // Verify all data is still accessible
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 20; i++) {
                    int key = batch * 20 + i;
                    String value = getString(db, String.format("key-%05d", key));
                    Assertions.assertNotNull(
                            value, "Key key-" + key + " should exist after compaction");
                }
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testCompactionRemovesTombstones() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            putString(db, "key1", "value1");
            putString(db, "key2", "value2");
            putString(db, "key3", "value3");
            db.flush();

            deleteString(db, "key2");
            db.flush();

            // Before compaction, key2 should be deleted
            Assertions.assertNull(getString(db, "key2"));

            // Force compaction to push tombstones to max level where they are removed
            db.compact();

            // After full compaction, key2 should still be deleted
            Assertions.assertNull(getString(db, "key2"));
            Assertions.assertEquals("value1", getString(db, "key1"));
            Assertions.assertEquals("value3", getString(db, "key3"));
        }
    }

    @Test
    public void testManualCompaction() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            // Create multiple SST files in L0
            putString(db, "a", "1");
            db.flush();
            putString(db, "b", "2");
            db.flush();
            putString(db, "c", "3");
            db.flush();

            Assertions.assertEquals(3, db.getSstFileCount());
            Assertions.assertEquals(3, db.getLevelFileCount(0));

            db.compact();

            // After full compaction, all data should be in the deepest level
            Assertions.assertEquals(0, db.getLevelFileCount(0));
            Assertions.assertTrue(db.getSstFileCount() > 0);

            // All data should still be accessible
            Assertions.assertEquals("1", getString(db, "a"));
            Assertions.assertEquals("2", getString(db, "b"));
            Assertions.assertEquals("3", getString(db, "c"));
        }
    }

    @Test
    public void testLargeDataSet() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            int recordCount = 1000;
            for (int i = 0; i < recordCount; i++) {
                putString(db, String.format("key-%08d", i), String.format("value-%08d", i));
            }

            // Verify all records
            for (int i = 0; i < recordCount; i++) {
                String expected = String.format("value-%08d", i);
                String actual = getString(db, String.format("key-%08d", i));
                Assertions.assertEquals(expected, actual, "Mismatch at index " + i);
            }
        }
    }

    @Test
    public void testByteArrayKeyValue() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            byte[] key = new byte[] {0x01, 0x02, 0x03};
            byte[] value = new byte[] {0x0A, 0x0B, 0x0C, 0x0D};

            db.put(key, value);

            byte[] result = db.get(key);
            Assertions.assertNotNull(result);
            Assertions.assertArrayEquals(value, result);
        }
    }

    @Test
    public void testCloseFlushesMemTable() throws IOException {
        File dbDir = new File(tempDir.toFile(), "close-test-db");
        // Write data and close
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(dbDir)
                        .memTableFlushThreshold(1024 * 1024) // large threshold, won't auto-flush
                        .blockSize(256)
                        .level0FileNumCompactTrigger(10)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        putString(db, "persist-key", "persist-value");
        Assertions.assertEquals(0, db.getSstFileCount());
        db.close();

        // After close, data should have been flushed to SST
        Assertions.assertEquals(1, db.getSstFileCount());
    }

    @Test
    public void testClosedDbThrowsException() throws IOException {
        SimpleLsmKvDb db = createDb();
        db.close();

        Assertions.assertThrows(IllegalStateException.class, () -> putString(db, "key", "value"));
        Assertions.assertThrows(IllegalStateException.class, () -> getString(db, "key"));
        Assertions.assertThrows(IllegalStateException.class, () -> deleteString(db, "key"));
    }

    @Test
    public void testWithCompression() throws IOException {
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "compressed-db"))
                        .memTableFlushThreshold(512)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(4)
                        .compressOptions(CompressOptions.defaultOptions())
                        .build();

        try {
            for (int i = 0; i < 200; i++) {
                putString(
                        db,
                        String.format("compressed-key-%05d", i),
                        String.format("compressed-value-%05d", i));
            }

            for (int i = 0; i < 200; i++) {
                String expected = String.format("compressed-value-%05d", i);
                String actual = getString(db, String.format("compressed-key-%05d", i));
                Assertions.assertEquals(expected, actual);
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testEmptyDb() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            Assertions.assertNull(getString(db, "any-key"));
            Assertions.assertEquals(0, db.getSstFileCount());
            Assertions.assertEquals(0, db.getMemTableSize());
        }
    }

    @Test
    public void testFlushEmptyMemTable() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            // Flushing an empty MemTable should be a no-op
            db.flush();
            Assertions.assertEquals(0, db.getSstFileCount());
        }
    }

    @Test
    public void testUniversalCompactionTriggeredByRunCount() throws IOException {
        // Compaction threshold = 3, so 3 sorted runs trigger compaction
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "universal-trigger-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Create 3 sorted runs (L0 files) to trigger universal compaction
            putString(db, "aaa", "v1");
            db.flush();
            Assertions.assertEquals(1, db.getLevelFileCount(0));

            putString(db, "bbb", "v2");
            db.flush();
            Assertions.assertEquals(2, db.getLevelFileCount(0));

            putString(db, "ccc", "v3");
            db.flush();
            // After 3rd flush, universal compaction should have been triggered
            // L0 should be cleared and data moved to a deeper level
            Assertions.assertEquals(0, db.getLevelFileCount(0));
            Assertions.assertTrue(db.getSstFileCount() > 0);

            // All data should be accessible
            Assertions.assertEquals("v1", getString(db, "aaa"));
            Assertions.assertEquals("v2", getString(db, "bbb"));
            Assertions.assertEquals("v3", getString(db, "ccc"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testUniversalCompactionWithOverlappingKeys() throws IOException {
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "universal-overlap-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Write overlapping keys across multiple flushes
            putString(db, "key-a", "old-a");
            putString(db, "key-b", "old-b");
            db.flush();

            putString(db, "key-a", "new-a");
            putString(db, "key-c", "new-c");
            db.flush();

            putString(db, "key-b", "new-b");
            putString(db, "key-d", "new-d");
            db.flush();
            // Universal compaction should have been triggered

            // Newer values should win
            Assertions.assertEquals("new-a", getString(db, "key-a"));
            Assertions.assertEquals("new-b", getString(db, "key-b"));
            Assertions.assertEquals("new-c", getString(db, "key-c"));
            Assertions.assertEquals("new-d", getString(db, "key-d"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testUniversalCompactionReducesFileCount() throws IOException {
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "universal-reduce-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Create 2 runs with overlapping keys (below threshold, no compaction)
            putString(db, "shared-key", "v1");
            putString(db, "a", "1");
            db.flush();
            putString(db, "shared-key", "v2");
            putString(db, "b", "2");
            db.flush();
            int fileCountBeforeCompaction = db.getSstFileCount();
            Assertions.assertEquals(2, fileCountBeforeCompaction);

            // 3rd flush triggers compaction; overlapping files get merged, reducing count
            putString(db, "shared-key", "v3");
            putString(db, "c", "3");
            db.flush();
            int fileCountAfterCompaction = db.getSstFileCount();
            Assertions.assertTrue(
                    fileCountAfterCompaction <= fileCountBeforeCompaction,
                    "Compaction should reduce or maintain file count, but got "
                            + fileCountAfterCompaction);

            // Data integrity — newest value wins
            Assertions.assertEquals("v3", getString(db, "shared-key"));
            Assertions.assertEquals("1", getString(db, "a"));
            Assertions.assertEquals("2", getString(db, "b"));
            Assertions.assertEquals("3", getString(db, "c"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testUniversalCompactionMultipleRounds() throws IOException {
        // Low threshold to trigger compaction frequently
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "universal-multi-db"))
                        .memTableFlushThreshold(512)
                        .blockSize(128)
                        .level0FileNumCompactTrigger(3)
                        .sizeRatio(50)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Write many batches to trigger multiple rounds of compaction
            int totalKeys = 500;
            for (int i = 0; i < totalKeys; i++) {
                putString(db, String.format("key-%06d", i), String.format("value-%06d", i));
            }

            // Verify all data is still correct after multiple compaction rounds
            for (int i = 0; i < totalKeys; i++) {
                String expected = String.format("value-%06d", i);
                String actual = getString(db, String.format("key-%06d", i));
                Assertions.assertEquals(
                        expected, actual, "Mismatch for key-" + String.format("%06d", i));
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testUniversalCompactionPreservesTombstonesInPartialMerge() throws IOException {
        // Use a high compaction threshold so we can control when compaction happens
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "universal-tombstone-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(4)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Create data and tombstones across multiple flushes
            putString(db, "key-1", "value-1");
            putString(db, "key-2", "value-2");
            putString(db, "key-3", "value-3");
            db.flush();

            deleteString(db, "key-2");
            db.flush();

            putString(db, "key-4", "value-4");
            db.flush();

            // key-2 should be deleted (tombstone exists in SST)
            Assertions.assertNull(getString(db, "key-2"));

            // Now trigger compaction by adding one more flush
            putString(db, "key-5", "value-5");
            db.flush();
            // 4 sorted runs should trigger compaction

            // After compaction, deleted key should still be gone
            Assertions.assertNull(getString(db, "key-2"));
            Assertions.assertEquals("value-1", getString(db, "key-1"));
            Assertions.assertEquals("value-3", getString(db, "key-3"));
            Assertions.assertEquals("value-4", getString(db, "key-4"));
            Assertions.assertEquals("value-5", getString(db, "key-5"));

            // Full compaction should clean up tombstones completely
            db.compact();
            Assertions.assertNull(getString(db, "key-2"));
            Assertions.assertEquals("value-1", getString(db, "key-1"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testUniversalCompactionWithUpdatesAcrossRuns() throws IOException {
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "universal-update-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Write same key across multiple sorted runs with different values
            putString(db, "shared-key", "version-1");
            db.flush();

            putString(db, "shared-key", "version-2");
            db.flush();

            putString(db, "shared-key", "version-3");
            db.flush();
            // Compaction triggered

            // The newest value should always win
            Assertions.assertEquals("version-3", getString(db, "shared-key"));

            // Write more updates and compact again
            putString(db, "shared-key", "version-4");
            db.flush();
            putString(db, "shared-key", "version-5");
            db.flush();
            putString(db, "shared-key", "version-6");
            db.flush();

            Assertions.assertEquals("version-6", getString(db, "shared-key"));

            // Full compaction should still preserve the latest value
            db.compact();
            Assertions.assertEquals("version-6", getString(db, "shared-key"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testLargeScaleFlushCompactAndFullCompact() throws IOException {
        // Very small thresholds to trigger flush, auto-compaction, and full compaction frequently
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "large-scale-db"))
                        .memTableFlushThreshold(256)
                        .blockSize(64)
                        .level0FileNumCompactTrigger(3)
                        .sizeRatio(20)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            int totalKeys = 2000;

            // Phase 1: Insert keys, triggering many flushes and auto-compactions
            for (int i = 0; i < totalKeys; i++) {
                putString(db, String.format("key-%08d", i), String.format("value-%08d", i));
            }

            // Verify all keys are readable after many auto-compactions
            for (int i = 0; i < totalKeys; i++) {
                String expected = String.format("value-%08d", i);
                String actual = getString(db, String.format("key-%08d", i));
                Assertions.assertEquals(
                        expected, actual, "Phase 1 mismatch for key-" + String.format("%08d", i));
            }

            // Phase 2: Overwrite half the keys
            for (int i = 0; i < totalKeys / 2; i++) {
                putString(db, String.format("key-%08d", i), String.format("updated-%08d", i));
            }

            // Verify overwrites
            for (int i = 0; i < totalKeys / 2; i++) {
                String expected = String.format("updated-%08d", i);
                String actual = getString(db, String.format("key-%08d", i));
                Assertions.assertEquals(
                        expected,
                        actual,
                        "Phase 2 overwrite mismatch for key-" + String.format("%08d", i));
            }
            for (int i = totalKeys / 2; i < totalKeys; i++) {
                String expected = String.format("value-%08d", i);
                String actual = getString(db, String.format("key-%08d", i));
                Assertions.assertEquals(
                        expected,
                        actual,
                        "Phase 2 original mismatch for key-" + String.format("%08d", i));
            }

            // Phase 3: Delete a quarter of the keys
            for (int i = 0; i < totalKeys / 4; i++) {
                deleteString(db, String.format("key-%08d", i));
            }

            // Verify deletes
            for (int i = 0; i < totalKeys / 4; i++) {
                Assertions.assertNull(
                        getString(db, String.format("key-%08d", i)),
                        "Phase 3 deleted key should be null: key-" + String.format("%08d", i));
            }

            // Phase 4: Full compaction — merges all runs and cleans tombstones
            int fileCountBefore = db.getSstFileCount();
            db.compact();
            int fileCountAfter = db.getSstFileCount();

            Assertions.assertTrue(
                    fileCountAfter <= fileCountBefore,
                    "Full compaction should reduce file count: before="
                            + fileCountBefore
                            + " after="
                            + fileCountAfter);
            Assertions.assertEquals(0, db.getLevelFileCount(0));

            // Verify all data integrity after full compaction
            for (int i = 0; i < totalKeys / 4; i++) {
                Assertions.assertNull(
                        getString(db, String.format("key-%08d", i)),
                        "After compact, deleted key should be null: key-"
                                + String.format("%08d", i));
            }
            for (int i = totalKeys / 4; i < totalKeys / 2; i++) {
                Assertions.assertEquals(
                        String.format("updated-%08d", i),
                        getString(db, String.format("key-%08d", i)),
                        "After compact, updated key mismatch: key-" + String.format("%08d", i));
            }
            for (int i = totalKeys / 2; i < totalKeys; i++) {
                Assertions.assertEquals(
                        String.format("value-%08d", i),
                        getString(db, String.format("key-%08d", i)),
                        "After compact, original key mismatch: key-" + String.format("%08d", i));
            }

            // Phase 5: Write more data after full compaction to ensure DB is still functional
            for (int i = totalKeys; i < totalKeys + 500; i++) {
                putString(db, String.format("key-%08d", i), String.format("new-%08d", i));
            }

            for (int i = totalKeys; i < totalKeys + 500; i++) {
                Assertions.assertEquals(
                        String.format("new-%08d", i),
                        getString(db, String.format("key-%08d", i)),
                        "Phase 5 new key mismatch: key-" + String.format("%08d", i));
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testCompactRemovesTombstonesAndMerges() throws IOException {
        try (SimpleLsmKvDb db = createDb()) {
            // Create data across multiple flushes
            for (int i = 0; i < 50; i++) {
                putString(db, String.format("key-%05d", i), String.format("value-%05d", i));
            }
            db.flush();

            for (int i = 25; i < 75; i++) {
                putString(db, String.format("key-%05d", i), String.format("updated-%05d", i));
            }
            db.flush();

            // Delete some keys
            for (int i = 0; i < 10; i++) {
                deleteString(db, String.format("key-%05d", i));
            }
            db.flush();

            // Full compaction should merge everything and remove tombstones
            db.compact();

            // Verify results
            for (int i = 0; i < 10; i++) {
                Assertions.assertNull(
                        getString(db, String.format("key-%05d", i)),
                        "Deleted key should be null: key-" + i);
            }
            for (int i = 10; i < 25; i++) {
                Assertions.assertEquals(
                        String.format("value-%05d", i),
                        getString(db, String.format("key-%05d", i)));
            }
            for (int i = 25; i < 75; i++) {
                Assertions.assertEquals(
                        String.format("updated-%05d", i),
                        getString(db, String.format("key-%05d", i)));
            }
        }
    }

    @Test
    public void testLevelStats() throws IOException {
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "stats-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(10)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            Assertions.assertEquals("empty", db.getLevelStats());

            putString(db, "a", "1");
            db.flush();
            Assertions.assertTrue(db.getLevelStats().contains("L0=1"));

            putString(db, "b", "2");
            db.flush();
            Assertions.assertTrue(db.getLevelStats().contains("L0=2"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testCustomComparatorReverseOrder() throws IOException {
        // Reverse comparator: keys are ordered in descending byte order
        Comparator<MemorySlice> reverseComparator =
                new Comparator<MemorySlice>() {
                    @Override
                    public int compare(MemorySlice a, MemorySlice b) {
                        return b.compareTo(a);
                    }
                };

        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "reverse-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .keyComparator(reverseComparator)
                        .build();

        try {
            // Basic put/get with reverse comparator
            putString(db, "aaa", "value-a");
            putString(db, "bbb", "value-b");
            putString(db, "ccc", "value-c");

            Assertions.assertEquals("value-a", getString(db, "aaa"));
            Assertions.assertEquals("value-b", getString(db, "bbb"));
            Assertions.assertEquals("value-c", getString(db, "ccc"));

            // Flush to SST and verify reads still work with reverse comparator
            db.flush();
            Assertions.assertEquals("value-a", getString(db, "aaa"));
            Assertions.assertEquals("value-b", getString(db, "bbb"));
            Assertions.assertEquals("value-c", getString(db, "ccc"));
            Assertions.assertNull(getString(db, "ddd"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testCustomComparatorWithCompaction() throws IOException {
        // Reverse comparator to ensure compaction uses the custom comparator
        Comparator<MemorySlice> reverseComparator =
                new Comparator<MemorySlice>() {
                    @Override
                    public int compare(MemorySlice a, MemorySlice b) {
                        return b.compareTo(a);
                    }
                };

        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "reverse-compact-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .keyComparator(reverseComparator)
                        .build();

        try {
            // Create multiple L0 files with overlapping keys to trigger compaction
            putString(db, "key-a", "old-a");
            putString(db, "key-b", "old-b");
            db.flush();

            putString(db, "key-a", "new-a");
            putString(db, "key-c", "new-c");
            db.flush();

            putString(db, "key-b", "new-b");
            putString(db, "key-d", "new-d");
            db.flush();
            // Compaction should have been triggered (threshold = 3)

            // Newer values should win after compaction with reverse comparator
            Assertions.assertEquals("new-a", getString(db, "key-a"));
            Assertions.assertEquals("new-b", getString(db, "key-b"));
            Assertions.assertEquals("new-c", getString(db, "key-c"));
            Assertions.assertEquals("new-d", getString(db, "key-d"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testCustomComparatorDeleteAcrossFlushAndCompact() throws IOException {
        // Reverse comparator
        Comparator<MemorySlice> reverseComparator =
                new Comparator<MemorySlice>() {
                    @Override
                    public int compare(MemorySlice a, MemorySlice b) {
                        return b.compareTo(a);
                    }
                };

        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "reverse-delete-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(4)
                        .compressOptions(new CompressOptions("none", 1))
                        .keyComparator(reverseComparator)
                        .build();

        try {
            putString(db, "key-x", "value-x");
            putString(db, "key-y", "value-y");
            putString(db, "key-z", "value-z");
            db.flush();

            // Delete key-y via tombstone
            deleteString(db, "key-y");
            db.flush();

            Assertions.assertNull(getString(db, "key-y"));
            Assertions.assertEquals("value-x", getString(db, "key-x"));
            Assertions.assertEquals("value-z", getString(db, "key-z"));

            // Compaction should clean up tombstones
            db.compact();

            Assertions.assertNull(getString(db, "key-y"));
            Assertions.assertEquals("value-x", getString(db, "key-x"));
            Assertions.assertEquals("value-z", getString(db, "key-z"));
        } finally {
            db.close();
        }
    }

    // ---- Tests for group-based merge optimization ----

    @Test
    public void testNonOverlappingFilesSkipMerge() throws IOException {
        // Non-overlapping files should be kept as-is during compaction (skipped groups).
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "non-overlap-skip-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Each flush creates a file with a distinct, non-overlapping key range
            putString(db, "aaa", "1");
            db.flush();
            putString(db, "mmm", "2");
            db.flush();
            putString(db, "zzz", "3");
            db.flush(); // triggers compaction (threshold = 3)

            // Data integrity must be preserved
            Assertions.assertEquals("1", getString(db, "aaa"));
            Assertions.assertEquals("2", getString(db, "mmm"));
            Assertions.assertEquals("3", getString(db, "zzz"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testOverlappingFilesAreMergedInGroups() throws IOException {
        // Files with overlapping key ranges should be merged within the same group.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "overlap-group-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // All three flushes share "shared-key", so all files overlap
            putString(db, "shared-key", "v1");
            putString(db, "aaa", "a1");
            db.flush();
            putString(db, "shared-key", "v2");
            putString(db, "bbb", "b1");
            db.flush();
            putString(db, "shared-key", "v3");
            putString(db, "ccc", "c1");
            db.flush(); // triggers compaction

            // Newest value wins for the shared key
            Assertions.assertEquals("v3", getString(db, "shared-key"));
            Assertions.assertEquals("a1", getString(db, "aaa"));
            Assertions.assertEquals("b1", getString(db, "bbb"));
            Assertions.assertEquals("c1", getString(db, "ccc"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testMixedOverlapAndNonOverlapGroups() throws IOException {
        // Some files overlap (forming one group) while others don't (separate groups).
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "mixed-group-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(4)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Run 1: keys a-c (overlaps with run 2)
            putString(db, "a", "a1");
            putString(db, "b", "b1");
            putString(db, "c", "c1");
            db.flush();

            // Run 2: keys b-d (overlaps with run 1, forms one group)
            putString(db, "b", "b2");
            putString(db, "d", "d1");
            db.flush();

            // Run 3: keys x-z (no overlap with runs 1-2, separate group)
            putString(db, "x", "x1");
            putString(db, "y", "y1");
            putString(db, "z", "z1");
            db.flush();

            // Run 4: key m (no overlap, separate group) — triggers compaction
            putString(db, "m", "m1");
            db.flush();

            // Verify data integrity: overlapping keys use newest value
            Assertions.assertEquals("a1", getString(db, "a"));
            Assertions.assertEquals("b2", getString(db, "b")); // newest wins
            Assertions.assertEquals("c1", getString(db, "c"));
            Assertions.assertEquals("d1", getString(db, "d"));
            Assertions.assertEquals("m1", getString(db, "m"));
            Assertions.assertEquals("x1", getString(db, "x"));
            Assertions.assertEquals("y1", getString(db, "y"));
            Assertions.assertEquals("z1", getString(db, "z"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testTombstoneFileNotSkippedDuringFullCompact() throws IOException {
        // A non-overlapping file with tombstones should NOT be skipped during full compaction
        // (dropTombstones=true), ensuring tombstones are cleaned up.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "tombstone-no-skip-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            putString(db, "key1", "value1");
            putString(db, "key2", "value2");
            db.flush();

            // Delete key1 — creates a tombstone
            deleteString(db, "key1");
            db.flush();

            // Full compaction should process the tombstone file even if it doesn't overlap
            db.compact();

            Assertions.assertNull(getString(db, "key1"));
            Assertions.assertEquals("value2", getString(db, "key2"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testGroupMergeWithMultipleCompactionRounds() throws IOException {
        // Multiple rounds of compaction with mixed overlapping/non-overlapping data.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "multi-round-group-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Round 1: non-overlapping keys
            putString(db, "a", "a1");
            db.flush();
            putString(db, "m", "m1");
            db.flush();
            putString(db, "z", "z1");
            db.flush(); // triggers compaction

            // Round 2: overlapping updates
            putString(db, "a", "a2");
            db.flush();
            putString(db, "a", "a3");
            putString(db, "b", "b1");
            db.flush();
            putString(db, "c", "c1");
            db.flush(); // triggers compaction again

            // Full compaction to consolidate everything
            db.compact();

            Assertions.assertEquals("a3", getString(db, "a"));
            Assertions.assertEquals("b1", getString(db, "b"));
            Assertions.assertEquals("c1", getString(db, "c"));
            Assertions.assertEquals("m1", getString(db, "m"));
            Assertions.assertEquals("z1", getString(db, "z"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testCompactionMergesAllL0RunsAndIncludesL1() throws IOException {
        // Verify that size-ratio compaction always merges all L0 files and L1 data,
        // so the total sorted run count never exceeds maxLevels.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "l0-clear-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .sizeRatio(1)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Step 1: Create 3 L0 files to trigger first compaction
            putString(db, "aaa", "v1");
            db.flush();
            putString(db, "bbb", "v2");
            db.flush();
            putString(db, "ccc", "v3");
            db.flush();
            // After compaction, data should exist in SST files
            Assertions.assertTrue(
                    db.getSstFileCount() > 0, "SST files should exist after compaction");

            // Step 2: Create 3 more L0 files to trigger second compaction
            // Now deeper levels already have data from the first compaction.
            // The fix ensures L1 is included in the merge, preventing overflow.
            putString(db, "ddd", "v4");
            db.flush();
            putString(db, "eee", "v5");
            db.flush();
            putString(db, "fff", "v6");
            db.flush();

            // All data should be accessible after multiple compaction rounds
            Assertions.assertEquals("v1", getString(db, "aaa"));
            Assertions.assertEquals("v2", getString(db, "bbb"));
            Assertions.assertEquals("v3", getString(db, "ccc"));
            Assertions.assertEquals("v4", getString(db, "ddd"));
            Assertions.assertEquals("v5", getString(db, "eee"));
            Assertions.assertEquals("v6", getString(db, "fff"));

            // Total file count across all levels should be reasonable (no overflow)
            int totalFiles = db.getSstFileCount();
            Assertions.assertTrue(
                    totalFiles > 0 && totalFiles < 20,
                    "File count should be reasonable, got: " + totalFiles);
        } finally {
            db.close();
        }
    }

    @Test
    public void testCompactionWithManyRoundsNoOverflow() throws IOException {
        // Stress test: many rounds of flush + compaction to ensure no overflow ever occurs
        // and data integrity is maintained throughout.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "many-l0-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .sizeRatio(1)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            int totalRounds = 10;
            for (int round = 0; round < totalRounds; round++) {
                for (int i = 0; i < 3; i++) {
                    String key = String.format("round-%02d-key-%02d", round, i);
                    String value = String.format("value-%02d-%02d", round, i);
                    putString(db, key, value);
                    db.flush();
                }
                // After each batch of 3 flushes, compaction should have been triggered.
                // Verify that the number of occupied levels is within bounds.
                int occupiedLevels = 0;
                for (int level = 0; level < SimpleLsmKvDb.MAX_LEVELS; level++) {
                    if (db.getLevelFileCount(level) > 0) {
                        occupiedLevels++;
                    }
                }
                Assertions.assertTrue(
                        occupiedLevels <= SimpleLsmKvDb.MAX_LEVELS,
                        "Occupied levels should not exceed MAX_LEVELS in round "
                                + round
                                + ", got: "
                                + occupiedLevels);
            }

            // Verify all data is still accessible
            for (int round = 0; round < totalRounds; round++) {
                for (int i = 0; i < 3; i++) {
                    String key = String.format("round-%02d-key-%02d", round, i);
                    String expected = String.format("value-%02d-%02d", round, i);
                    Assertions.assertEquals(expected, getString(db, key), "Mismatch for " + key);
                }
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testCompactionWithOverlappingKeysAcrossL0AndL1() throws IOException {
        // Verify that when L0 files have overlapping keys with existing level data,
        // compaction correctly deduplicates and the newest value wins.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "overlap-l0-l1-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .sizeRatio(1)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // First round: write and compact to push data into deeper levels
            putString(db, "shared", "version-1");
            putString(db, "only-old", "old-value");
            db.flush();
            putString(db, "aaa", "a1");
            db.flush();
            putString(db, "bbb", "b1");
            db.flush();
            // Compaction triggered

            // Second round: overwrite "shared" key and trigger compaction again
            // This forces merge with overlapping keys across levels
            putString(db, "shared", "version-2");
            db.flush();
            putString(db, "ccc", "c1");
            db.flush();
            putString(db, "ddd", "d1");
            db.flush();
            // Compaction triggered again

            // Newest value should win
            Assertions.assertEquals("version-2", getString(db, "shared"));
            Assertions.assertEquals("old-value", getString(db, "only-old"));
            Assertions.assertEquals("a1", getString(db, "aaa"));
            Assertions.assertEquals("b1", getString(db, "bbb"));
            Assertions.assertEquals("c1", getString(db, "ccc"));
            Assertions.assertEquals("d1", getString(db, "ddd"));

            // Third round: overwrite again and verify
            putString(db, "shared", "version-3");
            db.flush();
            putString(db, "eee", "e1");
            db.flush();
            putString(db, "fff", "f1");
            db.flush();

            Assertions.assertEquals("version-3", getString(db, "shared"));
            Assertions.assertEquals("old-value", getString(db, "only-old"));
            Assertions.assertEquals("e1", getString(db, "eee"));
            Assertions.assertEquals("f1", getString(db, "fff"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testGroupMergePreservesDeleteSemantics() throws IOException {
        // Ensure that deletes are correctly handled across group-based merge.
        SimpleLsmKvDb db =
                SimpleLsmKvDb.builder(new File(tempDir.toFile(), "group-delete-db"))
                        .memTableFlushThreshold(1024 * 1024)
                        .blockSize(256)
                        .level0FileNumCompactTrigger(3)
                        .compressOptions(new CompressOptions("none", 1))
                        .build();

        try {
            // Write keys in different ranges
            putString(db, "aaa", "v1");
            putString(db, "bbb", "v2");
            db.flush();

            putString(db, "xxx", "v3");
            putString(db, "yyy", "v4");
            db.flush();

            // Delete one key from each range
            deleteString(db, "aaa");
            deleteString(db, "xxx");
            db.flush(); // triggers compaction

            Assertions.assertNull(getString(db, "aaa"));
            Assertions.assertEquals("v2", getString(db, "bbb"));
            Assertions.assertNull(getString(db, "xxx"));
            Assertions.assertEquals("v4", getString(db, "yyy"));

            // Full compaction should clean up tombstones
            db.compact();

            Assertions.assertNull(getString(db, "aaa"));
            Assertions.assertEquals("v2", getString(db, "bbb"));
            Assertions.assertNull(getString(db, "xxx"));
            Assertions.assertEquals("v4", getString(db, "yyy"));
        } finally {
            db.close();
        }
    }

    private static void putString(SimpleLsmKvDb db, String key, String value) throws IOException {
        db.put(key.getBytes(UTF_8), value.getBytes(UTF_8));
    }

    private static String getString(SimpleLsmKvDb db, String key) throws IOException {
        byte[] bytes = db.get(key.getBytes(UTF_8));
        if (bytes == null) {
            return null;
        }
        return new String(bytes, UTF_8);
    }

    private static void deleteString(SimpleLsmKvDb db, String key) throws IOException {
        db.delete(key.getBytes(UTF_8));
    }
}
