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
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
