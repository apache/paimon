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

import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.BucketMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.paimon.index.IndexFileMetaSerializerTest.randomDeletionVectorIndexFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testGlobalIndexOverlappingRangeRejectedWhenPreviousFileKept() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 50, 149, 1);
        assertThatThrownBy(
                        () ->
                                indexManifestFileHandler.write(
                                        manifestFileName, Arrays.asList(added)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("prev-index")
                .hasMessageContaining("new-index")
                .hasMessageContaining("overlapping row range");
    }

    @Test
    public void testGlobalIndexOverlappingRangeAllowedAfterDelete() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 50, 149, 1);
        String newManifestFileName =
                indexManifestFileHandler.write(
                        manifestFileName, Arrays.asList(previous.toDeleteEntry(), added));

        List<IndexManifestEntry> entries = indexManifestFile.read(newManifestFileName);
        assertThat(entries).containsExactly(added);
    }

    @Test
    public void testGlobalIndexOverlappingRangeAllowedForDifferentFieldId() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 50, 149, 2);
        String newManifestFileName =
                indexManifestFileHandler.write(manifestFileName, Arrays.asList(added));

        List<IndexManifestEntry> entries = indexManifestFile.read(newManifestFileName);
        assertThat(entries).containsExactlyInAnyOrder(previous, added);
    }

    @Test
    public void testGlobalIndexNonOverlappingRangeAllowedForSameFieldId() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        IndexManifestFile indexManifestFile = createIndexManifestFile(fileStore);
        IndexManifestFileHandler indexManifestFileHandler =
                new IndexManifestFileHandler(indexManifestFile, BucketMode.BUCKET_UNAWARE);

        IndexManifestEntry previous = globalIndexEntry("prev-index", 0, 99, 1);
        String manifestFileName = indexManifestFileHandler.write(null, Arrays.asList(previous));

        IndexManifestEntry added = globalIndexEntry("new-index", 100, 199, 1);
        String newManifestFileName =
                indexManifestFileHandler.write(manifestFileName, Arrays.asList(added));

        List<IndexManifestEntry> entries = indexManifestFile.read(newManifestFileName);
        assertThat(entries).containsExactlyInAnyOrder(previous, added);
    }

    private IndexManifestFile createIndexManifestFile(TestAppendFileStore fileStore) {
        return new IndexManifestFile.Factory(
                        fileStore.fileIO(),
                        FileFormat.manifestFormat(fileStore.options()),
                        "zstd",
                        fileStore.pathFactory(),
                        null)
                .create();
    }

    private IndexManifestEntry globalIndexEntry(
            String fileName, long rowRangeStart, long rowRangeEnd, int indexFieldId) {
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        "btree",
                        fileName,
                        1L,
                        rowRangeEnd - rowRangeStart + 1,
                        new GlobalIndexMeta(rowRangeStart, rowRangeEnd, indexFieldId, null, null),
                        null));
    }
}
