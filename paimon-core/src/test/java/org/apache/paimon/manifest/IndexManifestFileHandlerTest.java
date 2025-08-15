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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IndexFilePathFactories;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.SnapshotTest.newSnapshotManager;
import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
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

    /**
     * Checks if the given List of IndexFileMeta and Map of String to DeletionFile are equivalent.
     * Two collections are considered equivalent if they contain the same deletion vector
     * information for the same data files.
     *
     * @param indexFileMetas List of IndexFileMeta containing deletion vector metadata
     * @param deletionFiles Map from data file name to DeletionFile
     * @return true if the two collections are equivalent, false otherwise
     */
    public boolean areEquivalent(
            BinaryRow partition,
            Integer bucket,
            IndexFileHandler indexFileHandler,
            List<IndexFileMeta> indexFileMetas,
            Map<String, DeletionFile> deletionFiles) {
        if (indexFileMetas == null && deletionFiles == null) {
            return true;
        }
        if (indexFileMetas == null && deletionFiles != null) {
            return false;
        }
        Map<String, DeletionFile> extractedMetas = new HashMap<>();
        for (IndexFileMeta indexFileMeta : indexFileMetas) {
            LinkedHashMap<String, DeletionVectorMeta> deletionVectorMetas =
                    indexFileMeta.dvRanges();
            if (deletionVectorMetas == null) {
                continue;
            }
            deletionVectorMetas.forEach(
                    (dataFileName, meta) -> {
                        extractedMetas.put(
                                dataFileName,
                                new DeletionFile(
                                        indexFileHandler
                                                .dvIndex(partition, bucket)
                                                .path(indexFileMeta)
                                                .toString(),
                                        meta.offset(),
                                        meta.length(),
                                        meta.cardinality()));
                    });
        }

        if (deletionFiles == null) {
            if (extractedMetas.size() == 0) {
                return true;
            } else {
                return false;
            }
        }

        if (extractedMetas.size() != deletionFiles.size()) {
            return false;
        }
        // check extractedMetas is the same as deletionFiles
        for (Map.Entry<String, DeletionFile> entry : deletionFiles.entrySet()) {
            if (!extractedMetas.containsKey(entry.getKey())) {
                return false;
            }
            if (!Objects.equals(extractedMetas.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    @Test
    // TODO: generate a random index manifest file instead of reading a local file
    public void testReadLocalIndexManifestFile() {
        MemorySize manifestMaxMemory = MemorySize.ofMebiBytes(128);
        long manifestCacheThreshold = MemorySize.ofMebiBytes(1).getBytes();
        SegmentsCache<Path> manifestCache =
                SegmentsCache.create(manifestMaxMemory, manifestCacheThreshold);
        DVMetaCache dvMetaCache = new DVMetaCache(1000000);
        Path path = new Path("/Users/langu/repos/scripts/paimon");
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGCY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null,
                        null,
                        false);
        FileIO localFileIO = LocalFileIO.create();
        //        FileIO fileIO = FileIOFinder.find(path);
        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                localFileIO,
                                FileFormat.fromIdentifier("avro", new Options()),
                                "zstd",
                                pathFactory,
                                manifestCache,
                                dvMetaCache)
                        .create();

        SnapshotManager snapshotManager =
                newSnapshotManager(LocalFileIO.create(), new Path(tempDir.toString()));
        // 1. scan local file to get partition and buckets
        List<IndexManifestEntry> entries =
                indexManifestFile.read("index-manifest-1e28f20e-a437-4673-b91e-ab74d08eb5c8-0");
        //        for (IndexManifestEntry entry : entries) {
        //            System.out.println(entry);
        //        }
        Map<BinaryRow, Map<Integer, List<IndexManifestEntry>>> grouped = new LinkedHashMap<>();
        for (IndexManifestEntry entry : entries) {
            grouped.computeIfAbsent(entry.partition(), k -> new LinkedHashMap<>())
                    .computeIfAbsent(entry.bucket(), k -> new ArrayList<>())
                    .add(entry);
        }
        Set<Pair<BinaryRow, Integer>> partitionBuckets =
                grouped.entrySet().stream()
                        .flatMap(
                                e ->
                                        e.getValue().keySet().stream()
                                                .map(bucket -> Pair.of(e.getKey(), bucket)))
                        .collect(Collectors.toSet());

        IndexFileHandler indexFileHandler =
                new IndexFileHandler(
                        localFileIO,
                        snapshotManager,
                        indexManifestFile,
                        new IndexFilePathFactories(pathFactory),
                        MemorySize.ofMebiBytes(2),
                        false,
                        true);
        CacheMetrics cacheMetrics = new CacheMetrics();
        indexFileHandler.withCacheMetrics(cacheMetrics);

        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 5,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"baseManifestListSize\" : 6,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"deltaManifestListSize\" : 8,\n"
                        + "  \"changelogManifestListSize\" : 10,\n"
                        + "  \"indexManifest\" : \"index-manifest-1e28f20e-a437-4673-b91e-ab74d08eb5c8-0\",\n"
                        + "  \"commitUser\" : null,\n"
                        + "  \"commitIdentifier\" : 0,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1234,\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null,\n"
                        + "  \"unknownKey\" : 22222\n"
                        + "}";
        Snapshot snapshot = Snapshot.fromJson(json);
        // 2. warmup dv meta cache for each partition
        Set<BinaryRow> partitions =
                partitionBuckets.stream().map(Pair::getLeft).collect(Collectors.toSet());
        for (BinaryRow partition : partitions) {
            Map<String, DeletionFile> deletionFiles =
                    indexFileHandler.scanDVIndexWithCache(snapshot, partition, 0);
            assertThat(deletionFiles).isNotNull();
        }

        assertThat(cacheMetrics.getMissedObject().get() == partitions.size()).isTrue();

        // 3. get index file metas
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> indexFileMetas =
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partitions);

        int maxCheckCount = Integer.min(partitionBuckets.size(), 100);
        int i = 0;
        for (Pair<BinaryRow, Integer> partitionBucket : partitionBuckets) {
            Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> res =
                    indexFileHandler.scanDVIndex(snapshot, Collections.singleton(partitionBucket));
            assertThat(res).isNotNull();
            Map<String, DeletionFile> deletionFiles1 = res.get(partitionBucket);
            Map<String, DeletionFile> deletionFiles2 =
                    indexFileHandler.scanDVIndexWithCache(
                            snapshot, partitionBucket.getLeft(), partitionBucket.getRight());
            // check scanDVIndex and scanDVIndexWithCache are equivalent
            if (deletionFiles1 == null) {
                assertThat(deletionFiles2).isEmpty();
            } else {
                assertThat(deletionFiles1.equals(deletionFiles2)).isTrue();
            }
            // check ScanDVIndex and scan are equivalent
            assertThat(
                            areEquivalent(
                                    partitionBucket.getLeft(),
                                    partitionBucket.getRight(),
                                    indexFileHandler,
                                    indexFileMetas.get(partitionBucket),
                                    deletionFiles1))
                    .isTrue();
            if (++i == maxCheckCount) {
                break;
            }
        }
        System.out.println(maxCheckCount);
        System.out.println(cacheMetrics.getHitObject().get());
        assertThat(cacheMetrics.getHitObject().get() == maxCheckCount).isTrue();
    }
}
