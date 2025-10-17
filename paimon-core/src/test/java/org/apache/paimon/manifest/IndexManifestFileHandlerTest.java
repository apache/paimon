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

package org.apache.paimon.manifest;

import org.apache.paimon.Snapshot;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.IndexFileMetaSerializerTest.randomDeletionVectorIndexFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for IndexManifestFileHandler. */
public class IndexManifestFileHandlerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testUnawareMode() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                null,
                                null)
                        .create();
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry entry1 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 0, randomDeletionVectorIndexFile());
        String indexManifestFile1 = indexManifestFileHandler.write(null, Arrays.asList(entry1));

        IndexManifestEntry entry2 = entry1.toDeleteEntry();
        IndexManifestEntry entry3 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 0, randomDeletionVectorIndexFile());
        String indexManifestFile2 =
                indexManifestFileHandler.write(indexManifestFile1, Arrays.asList(entry2, entry3));

        List<IndexManifestEntry> entries = indexManifestFile.read(indexManifestFile2);
        assertThat(entries.size()).isEqualTo(1);
        assertThat(entries.contains(entry1)).isFalse();
        assertThat(entries.contains(entry2)).isFalse();
        assertThat(entries.contains(entry3)).isTrue();
    }

    @Test
    public void testHashFixedBucket() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                null,
                                null)
                        .create();
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.HASH_FIXED);

        IndexManifestEntry entry1 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 0, randomDeletionVectorIndexFile());
        IndexManifestEntry entry2 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 1, randomDeletionVectorIndexFile());
        String indexManifestFile1 =
                indexManifestFileHandler.write(null, Arrays.asList(entry1, entry2));

        IndexManifestEntry entry3 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 1, randomDeletionVectorIndexFile());
        IndexManifestEntry entry4 =
                new IndexManifestEntry(
                        FileKind.ADD, BinaryRow.EMPTY_ROW, 2, randomDeletionVectorIndexFile());
        String indexManifestFile2 =
                indexManifestFileHandler.write(indexManifestFile1, Arrays.asList(entry3, entry4));

        List<IndexManifestEntry> entries = indexManifestFile.read(indexManifestFile2);
        assertThat(entries.size()).isEqualTo(3);
        assertThat(entries.contains(entry1)).isTrue();
        assertThat(entries.contains(entry2)).isFalse();
        assertThat(entries.contains(entry3)).isTrue();
        assertThat(entries.contains(entry4)).isTrue();
    }

    @Test
    public void testDVMetaCache() {
        TestFileStore fileStore = keyValueFileStore();

        // Setup cache and index-manifest file
        MemorySize manifestMaxMemory = MemorySize.ofMebiBytes(128);
        long manifestCacheThreshold = MemorySize.ofMebiBytes(1).getBytes();
        SegmentsCache<Path> manifestCache =
                SegmentsCache.create(manifestMaxMemory, manifestCacheThreshold);
        DVMetaCache dvMetaCache = new DVMetaCache(1000000);

        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                manifestCache,
                                dvMetaCache)
                        .create();

        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.HASH_FIXED);

        BinaryRow partition1 = partition(1);
        BinaryRow partition2 = partition(2);

        IndexManifestEntry entry1 =
                new IndexManifestEntry(
                        FileKind.ADD,
                        partition1,
                        0,
                        deletionVectorIndexFile("data1.parquet", "data2.parquet"));
        IndexManifestEntry entry2 =
                new IndexManifestEntry(
                        FileKind.ADD, partition1, 1, deletionVectorIndexFile("data3.parquet"));
        IndexManifestEntry entry3 =
                new IndexManifestEntry(
                        FileKind.ADD,
                        partition2,
                        0,
                        deletionVectorIndexFile("data4.parquet", "data5.parquet"));

        String indexManifestFileName =
                indexManifestFileHandler.write(null, Arrays.asList(entry1, entry2, entry3));

        // Create IndexFileHandler with cache enabled
        IndexFileHandler indexFileHandler =
                new IndexFileHandler(
                        fileStore.fileIO(),
                        fileStore.snapshotManager(),
                        indexManifestFile,
                        new IndexFilePathFactories(fileStore.pathFactory()),
                        MemorySize.ofMebiBytes(2),
                        false,
                        true);

        CacheMetrics cacheMetrics = new CacheMetrics();
        indexFileHandler.withCacheMetrics(cacheMetrics);

        Snapshot snapshot = snapshot(indexManifestFileName);

        // Test 1: First access should miss cache
        Map<String, DeletionFile> deletionFiles1 =
                indexFileHandler.scanDVIndexWithCache(snapshot, partition1, 0);
        assertThat(deletionFiles1).isNotNull();
        assertThat(cacheMetrics.getMissedObject().get()).isEqualTo(1);
        assertThat(cacheMetrics.getHitObject().get()).isEqualTo(0);

        // Test 2: Second access to same partition/bucket should hit cache
        Map<String, DeletionFile> deletionFiles2 =
                indexFileHandler.scanDVIndexWithCache(snapshot, partition1, 0);
        assertThat(deletionFiles2).isNotNull();
        assertThat(deletionFiles1).isEqualTo(deletionFiles2);
        assertThat(cacheMetrics.getHitObject().get()).isEqualTo(1);

        // Test 3: Access different bucket in same partition should hit cache
        Map<String, DeletionFile> deletionFiles3 =
                indexFileHandler.scanDVIndexWithCache(snapshot, partition1, 1);
        assertThat(deletionFiles3).isNotNull();
        assertThat(cacheMetrics.getHitObject().get()).isEqualTo(2);
        assertThat(cacheMetrics.getMissedObject().get()).isEqualTo(1); // Still only 1 miss

        // Test 4: Access different partition should miss cache
        Map<String, DeletionFile> deletionFiles4 =
                indexFileHandler.scanDVIndexWithCache(snapshot, partition2, 0);
        assertThat(deletionFiles4).isNotNull();
        assertThat(cacheMetrics.getMissedObject().get()).isEqualTo(2); // Now 2 misses total

        // Test 5: Verify cache consistency with non-cache method
        Set<Pair<BinaryRow, Integer>> partitionBuckets = new HashSet<>();
        partitionBuckets.add(Pair.of(partition1, 0));
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> nonCacheResult =
                indexFileHandler.scanDVIndex(snapshot, partitionBuckets);
        Map<String, DeletionFile> nonCacheDeletionFiles =
                nonCacheResult.get(Pair.of(partition1, 0));

        assertThat(deletionFiles1).isEqualTo(nonCacheDeletionFiles);

        // Test 6: Verify extractDeletionFileByMeta works correctly
        List<IndexManifestEntry> entries = indexManifestFile.read(indexManifestFileName);
        IndexManifestEntry targetEntry =
                entries.stream()
                        .filter(e -> e.partition().equals(partition1) && e.bucket() == 0)
                        .findFirst()
                        .orElse(null);
        assertThat(targetEntry).isNotNull();

        Map<String, DeletionFile> extractedFiles =
                indexFileHandler.extractDeletionFileByMeta(partition1, 0, targetEntry.indexFile());
        assertThat(extractedFiles).isNotNull();
        assertThat(extractedFiles.size()).isGreaterThan(0);
    }

    // ============================ Test utils ===================================

    private BinaryRow partition(int partitionValue) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, partitionValue);
        writer.complete();
        return partition;
    }

    private IndexFileMeta deletionVectorIndexFile(String... dataFileNames) {
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        int offset = 0;
        for (String dataFileName : dataFileNames) {
            dvRanges.put(
                    dataFileName,
                    new DeletionVectorMeta(
                            dataFileName, offset, 100 + offset, (long) (10 + offset)));
            offset += 150;
        }
        return new IndexFileMeta(
                DELETION_VECTORS_INDEX,
                "dv_index_" + UUID.randomUUID().toString(),
                1024,
                512,
                dvRanges,
                null);
    }

    private Snapshot snapshot(String indexManifestFileName) {
        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 1,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"baseManifestListSize\" : 0,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"deltaManifestListSize\" : 0,\n"
                        + "  \"changelogManifestListSize\" : 0,\n"
                        + "  \"indexManifest\" : \""
                        + indexManifestFileName
                        + "\",\n"
                        + "  \"commitUser\" : \"test\",\n"
                        + "  \"commitIdentifier\" : 1,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : "
                        + System.currentTimeMillis()
                        + ",\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null\n"
                        + "}";
        return Snapshot.fromJson(json);
    }

    private TestFileStore keyValueFileStore() {
        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        2, // 2 buckets for testing
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        null)
                .build();
    }
}
