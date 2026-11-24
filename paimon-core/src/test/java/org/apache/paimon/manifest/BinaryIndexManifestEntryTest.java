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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;

/** Tests for {@link BinaryIndexManifestEntry}. */
class BinaryIndexManifestEntryTest extends TableTestBase {

    @Test
    void testProjectedScanAndReusableEntry() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        IndexManifestFile indexManifestFile = table.store().indexManifestFileFactory().create();

        BinaryRow firstPartition = partition(1);
        BinaryRow secondPartition = partition(2);
        IndexManifestEntry add =
                entry(
                        FileKind.ADD,
                        firstPartition,
                        3,
                        "btree",
                        new GlobalIndexMeta(10, 19, 1, new int[] {2}, null));
        IndexManifestEntry delete =
                entry(FileKind.DELETE, secondPartition, 4, "deletion-vector", null);
        String fileName = indexManifestFile.writeWithoutRolling(Arrays.asList(add, delete));

        try (CloseableIterator<BinaryIndexManifestEntry> entries =
                indexManifestFile.scan(
                        fileName, BinaryIndexManifestEntry.GLOBAL_INDEX_PROJECTION)) {
            assertThat(entries.hasNext()).isTrue();
            BinaryIndexManifestEntry first = entries.next();
            assertThat(first.isAdd()).isTrue();
            assertThat(first.isDelete()).isFalse();
            assertThat(deserializeBinaryRow(first.partitionBytes())).isEqualTo(firstPartition);
            assertThat(first.bucket()).isEqualTo(3);
            assertThat(first.indexType().toString()).isEqualTo("btree");
            assertThat(first.hasGlobalIndexMeta()).isTrue();
            assertThat(first.rowRangeStart()).isEqualTo(10);
            assertThat(first.rowRangeEnd()).isEqualTo(19);
            assertThat(first.indexFieldId()).isEqualTo(1);
            assertThat(first.hasExtraFields()).isTrue();

            assertThat(entries.hasNext()).isTrue();
            assertThatThrownBy(first::bucket)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not backed by a row");

            BinaryIndexManifestEntry second = entries.next();
            assertThat(second).isSameAs(first);
            assertThat(second.isAdd()).isFalse();
            assertThat(second.isDelete()).isTrue();
            assertThat(deserializeBinaryRow(second.partitionBytes())).isEqualTo(secondPartition);
            assertThat(second.bucket()).isEqualTo(4);
            assertThat(second.indexType().toString()).isEqualTo("deletion-vector");
            assertThat(second.hasGlobalIndexMeta()).isFalse();
            assertThatThrownBy(second::rowRangeStart)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not present");

            assertThat(entries.hasNext()).isFalse();
            assertThatThrownBy(second::bucket)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not backed by a row");
        }
    }

    @Test
    void testCustomProjectionAndOrdering() {
        RowType manifestType = IndexManifestEntry.MANIFEST_ROW_TYPE;
        BinaryIndexManifestEntry entry =
                BinaryIndexManifestEntry.Projection.create(
                                new RowType(
                                        false,
                                        Arrays.asList(
                                                manifestType.getField(
                                                        IndexManifestEntry.INDEX_TYPE),
                                                manifestType.getField(IndexManifestEntry.BUCKET),
                                                manifestType.getField(IndexManifestEntry.KIND))))
                        .createEntry()
                        .replace(
                                GenericRow.of(
                                        BinaryString.fromString("btree"),
                                        3,
                                        FileKind.ADD.toByteValue()));

        assertThat(entry.indexType().toString()).isEqualTo("btree");
        assertThat(entry.bucket()).isEqualTo(3);
        assertThat(entry.isAdd()).isTrue();
        assertThatThrownBy(entry::partitionBytes)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(IndexManifestEntry.PARTITION);
        assertThatThrownBy(entry::hasGlobalIndexMeta)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(IndexManifestEntry.GLOBAL_INDEX);
        assertThatThrownBy(entry::fileName)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(IndexManifestEntry.FILE_NAME);
        assertThatThrownBy(entry::externalPath)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(IndexManifestEntry.EXTERNAL_PATH);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFileProjectionsBypassCache(boolean warmCache) throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        SegmentsCache<Path> cache =
                spy(new SegmentsCache<>(4096, MemorySize.ofMebiBytes(8), Long.MAX_VALUE));
        IndexManifestFile manifest =
                new IndexManifestFile.Factory(
                                table.fileIO(),
                                FileFormat.manifestFormat(table.coreOptions()),
                                "zstd",
                                table.store().pathFactory(),
                                cache)
                        .create();
        IndexManifestEntry local = entry(FileKind.ADD, partition(1), 3, "hash", null);
        String externalPath = "file:///external/index-file";
        IndexManifestEntry external =
                new IndexManifestEntry(
                        FileKind.ADD,
                        partition(2),
                        4,
                        new IndexFileMeta(
                                "btree",
                                "external-index-file",
                                100,
                                10,
                                new GlobalIndexMeta(0, 9, 1, null, new byte[1024]),
                                externalPath));
        String fileName = manifest.writeWithoutRolling(Arrays.asList(local, external));
        if (warmCache) {
            assertThat(manifest.read(fileName)).containsExactly(local, external);
            assertThat(cache.getIfPresents(manifest.indexManifestFilePath(fileName))).isNotNull();
        }
        clearInvocations(cache);

        try (CloseableIterator<BinaryIndexManifestEntry> entries =
                manifest.scan(fileName, BinaryIndexManifestEntry.FILE_PATH_PROJECTION)) {
            BinaryIndexManifestEntry first = entries.next();
            assertThat(first.fileName()).isEqualTo(local.indexFile().fileName());
            assertThat(first.externalPath()).isNull();
            assertThat(deserializeBinaryRow(first.partitionBytes())).isEqualTo(local.partition());
            assertThat(first.bucket()).isEqualTo(local.bucket());
            assertThatThrownBy(first::hasGlobalIndexMeta)
                    .isInstanceOf(UnsupportedOperationException.class);

            BinaryIndexManifestEntry second = entries.next();
            assertThat(second).isSameAs(first);
            assertThat(second.fileName()).isEqualTo(external.indexFile().fileName());
            assertThat(second.externalPath()).isEqualTo(externalPath);
            assertThat(deserializeBinaryRow(second.partitionBytes()))
                    .isEqualTo(external.partition());
            assertThat(second.bucket()).isEqualTo(external.bucket());
            assertThat(entries.hasNext()).isFalse();
        }
        try (CloseableIterator<BinaryIndexManifestEntry> entries =
                manifest.scan(fileName, BinaryIndexManifestEntry.FILE_NAME_PROJECTION)) {
            assertThat(entries.next().fileName()).isEqualTo(local.indexFile().fileName());
            BinaryIndexManifestEntry second = entries.next();
            assertThat(second.fileName()).isEqualTo(external.indexFile().fileName());
            assertThatThrownBy(second::partitionBytes)
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(second::externalPath)
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThat(entries.hasNext()).isFalse();
        }
        verifyNoInteractions(cache);
    }

    private static IndexManifestEntry entry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            String indexType,
            GlobalIndexMeta globalIndexMeta) {
        return new IndexManifestEntry(
                kind,
                partition,
                bucket,
                new IndexFileMeta(indexType, "index-file", 100, 10, globalIndexMeta, null));
    }

    private static BinaryRow partition(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }
}
