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
                        dvMetaCache,
                        false);

        Map<String, DeletionFile> expectedPartition1Bucket0Files =
                indexFileHandler.extractDeletionFileByMeta(partition1, 0, entry1.indexFile());
        Map<String, DeletionFile> expectedPartition1Bucket1Files =
                indexFileHandler.extractDeletionFileByMeta(partition1, 1, entry2.indexFile());
        Map<String, DeletionFile> expectedPartition2Bucket0Files =
                indexFileHandler.extractDeletionFileByMeta(partition2, 0, entry3.indexFile());

        CacheMetrics cacheMetrics = new CacheMetrics();
        indexFileHandler.withCacheMetrics(cacheMetrics);

        Snapshot snapshot = snapshot(indexManifestFileName);

        // Test 1: First access should miss cache
        Set<Pair<BinaryRow, Integer>> partitionBuckets = new HashSet<>();
        partitionBuckets.add(Pair.of(partition1, 0));
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFiles1 =
                indexFileHandler.scanDVIndex(snapshot, partitionBuckets);
        assertThat(deletionFiles1).isNotNull();
        assertThat(deletionFiles1.containsKey(Pair.of(partition1, 0))).isTrue();
        assertThat(cacheMetrics.getMissedObject().get()).isEqualTo(1);
        assertThat(cacheMetrics.getHitObject().get()).isEqualTo(0);
        Map<String, DeletionFile> actualPartition1Bucket0Files =
                deletionFiles1.get(Pair.of(partition1, 0));
        assertThat(actualPartition1Bucket0Files).isEqualTo(expectedPartition1Bucket0Files);

        // Test 2: Second access to same partition/bucket should hit cache
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFiles2 =
                indexFileHandler.scanDVIndex(snapshot, partitionBuckets);
        assertThat(deletionFiles2).isNotNull();
        assertThat(deletionFiles1).isEqualTo(deletionFiles2);
        assertThat(cacheMetrics.getHitObject().get()).isEqualTo(1);
        Map<String, DeletionFile> cachedPartition1Bucket0Files =
                deletionFiles2.get(Pair.of(partition1, 0));
        assertThat(cachedPartition1Bucket0Files).isEqualTo(expectedPartition1Bucket0Files);

        // Test 3: Access different bucket in same partition should hit cache
        partitionBuckets.clear();
        partitionBuckets.add(Pair.of(partition1, 1));
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFiles3 =
                indexFileHandler.scanDVIndex(snapshot, partitionBuckets);
        assertThat(deletionFiles3).isNotNull();
        assertThat(cacheMetrics.getHitObject().get()).isEqualTo(2);
        assertThat(cacheMetrics.getMissedObject().get()).isEqualTo(1);
        Map<String, DeletionFile> actualPartition1Bucket1Files =
                deletionFiles3.get(Pair.of(partition1, 1));
        assertThat(actualPartition1Bucket1Files).isEqualTo(expectedPartition1Bucket1Files);

        // Test 4: Access different partition should miss cache
        partitionBuckets.clear();
        partitionBuckets.add(Pair.of(partition2, 0));
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFiles4 =
                indexFileHandler.scanDVIndex(snapshot, partitionBuckets);
        assertThat(deletionFiles4).isNotNull();
        assertThat(cacheMetrics.getMissedObject().get()).isEqualTo(2); // Now 2 misses total
        Map<String, DeletionFile> actualPartition2Bucket0Files =
                deletionFiles4.get(Pair.of(partition2, 0));
        assertThat(actualPartition2Bucket0Files).isEqualTo(expectedPartition2Bucket0Files);

        // Test 5: Test non-cache path by requesting multiple partition buckets
        partitionBuckets.clear();
        partitionBuckets.add(Pair.of(partition1, 0));
        partitionBuckets.add(Pair.of(partition2, 0)); // Multiple buckets to avoid cache path
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFiles5 =
                indexFileHandler.scanDVIndex(snapshot, partitionBuckets);
        assertThat(deletionFiles5).isNotNull();
        assertThat(deletionFiles5.containsKey(Pair.of(partition1, 0))).isTrue();
        assertThat(deletionFiles5.containsKey(Pair.of(partition2, 0))).isTrue();

        Map<String, DeletionFile> nonCachePartition1Bucket0Files =
                deletionFiles5.get(Pair.of(partition1, 0));
        Map<String, DeletionFile> nonCachePartition2Bucket0Files =
                deletionFiles5.get(Pair.of(partition2, 0));
        assertThat(nonCachePartition1Bucket0Files).isEqualTo(expectedPartition1Bucket0Files);
        assertThat(nonCachePartition2Bucket0Files).isEqualTo(expectedPartition2Bucket0Files);
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
