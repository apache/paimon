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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BinaryManifestEntry}. */
public class BinaryManifestEntryTest {

    @Test
    void testImplementsProjectedManifestEntry() {
        BinaryRow partition = BinaryRow.EMPTY_ROW;
        BinaryManifestEntry entry =
                projection(
                                true,
                                DataFileMeta.FILE_NAME,
                                DataFileMeta.ROW_COUNT,
                                DataFileMeta.LEVEL,
                                DataFileMeta.EXTRA_FILES,
                                DataFileMeta.EMBEDDED_FILE_INDEX,
                                DataFileMeta.EXTERNAL_PATH,
                                DataFileMeta.FIRST_ROW_ID)
                        .createEntry()
                        .replace(
                                GenericRow.of(
                                        FileKind.ADD.toByteValue(),
                                        serializeBinaryRow(partition),
                                        3,
                                        GenericRow.of(
                                                BinaryString.fromString("data.parquet"),
                                                7L,
                                                2,
                                                new GenericArray(
                                                        new Object[] {
                                                            BinaryString.fromString("extra-1"),
                                                            BinaryString.fromString("extra-2")
                                                        }),
                                                new byte[] {1, 2},
                                                BinaryString.fromString("external/data.parquet"),
                                                11L)));

        ManifestEntry manifestEntry = entry;
        assertThat(manifestEntry.kind()).isEqualTo(FileKind.ADD);
        assertThat(manifestEntry.partition()).isEqualTo(partition);
        assertThat(manifestEntry.bucket()).isEqualTo(3);
        assertThat(manifestEntry.level()).isEqualTo(2);
        assertThat(manifestEntry.fileName()).isEqualTo("data.parquet");
        assertThat(manifestEntry.externalPath()).isEqualTo("external/data.parquet");
        assertThat(manifestEntry.extraFiles()).containsExactly("extra-1", "extra-2");
        assertThat(manifestEntry.rowCount()).isEqualTo(7L);
        assertThat(manifestEntry.firstRowId()).isEqualTo(11L);
        assertThat(manifestEntry.identifier().embeddedIndex).containsExactly(1, 2);
        BinaryDataFileMeta file = entry.file();
        assertThat(manifestEntry.file()).isSameAs(file);
        assertThat(file.fileName()).isEqualTo("data.parquet");
        assertThat(file.rowCount()).isEqualTo(7L);

        assertUnsupported(manifestEntry::totalBuckets, ManifestEntry.TOTAL_BUCKETS);
        assertUnsupported(manifestEntry::minKey, DataFileMeta.MIN_KEY);
        assertUnsupported(manifestEntry::maxKey, DataFileMeta.MAX_KEY);
        assertUnsupported(manifestEntry::copyWithoutStats, "copyWithoutStats()");
        assertUnsupported(
                () -> manifestEntry.assignSequenceNumber(1L, 2L),
                "assignSequenceNumber(long, long)");
        assertUnsupported(() -> manifestEntry.assignFirstRowId(1L), "assignFirstRowId(long)");
        assertUnsupported(() -> manifestEntry.upgrade(1), "upgrade(int)");
    }

    @Test
    void testMissingProjectionIsUnsupported() {
        FileEntry entry =
                projection(false, DataFileMeta.ROW_COUNT)
                        .createEntry()
                        .replace(
                                GenericRow.of(
                                        FileKind.DELETE.toByteValue(),
                                        serializeBinaryRow(BinaryRow.EMPTY_ROW),
                                        GenericRow.of(5L)));

        assertThat(entry.kind()).isEqualTo(FileKind.DELETE);
        assertThat(entry.partition()).isEqualTo(BinaryRow.EMPTY_ROW);
        assertThat(entry.rowCount()).isEqualTo(5L);
        assertUnsupported(entry::bucket, ManifestEntry.BUCKET);
        assertUnsupported(entry::fileName, DataFileMeta.FILE_NAME);
        assertUnsupported(entry::firstRowId, DataFileMeta.FIRST_ROW_ID);
        assertUnsupported(entry::identifier, ManifestEntry.BUCKET);
    }

    @Test
    void testProjectedNullIsNotUnsupported() {
        FileEntry entry =
                projection(false, DataFileMeta.FIRST_ROW_ID)
                        .createEntry()
                        .replace(
                                GenericRow.of(
                                        FileKind.ADD.toByteValue(),
                                        serializeBinaryRow(BinaryRow.EMPTY_ROW),
                                        GenericRow.of((Object) null)));

        assertThat(entry.firstRowId()).isNull();
    }

    @Test
    void testBindsArbitraryProjectedSchema() {
        BinaryRow partition = BinaryRow.EMPTY_ROW;
        BinaryRow minKey = BinaryRow.EMPTY_ROW;
        BinaryRow maxKey = BinaryRow.EMPTY_ROW.copy();
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        RowType projectedFileType =
                DataFileMeta.SCHEMA.project(
                        DataFileMeta.MAX_KEY, DataFileMeta.FILE_NAME, DataFileMeta.MIN_KEY);
        RowType projectedType =
                new RowType(
                        false,
                        java.util.Arrays.asList(
                                manifestType
                                        .getField(ManifestEntry.FILE)
                                        .newType(projectedFileType),
                                manifestType.getField(ManifestEntry.TOTAL_BUCKETS),
                                manifestType.getField(ManifestEntry.PARTITION),
                                manifestType.getField(ManifestEntry.KIND)));
        BinaryManifestEntry entry =
                BinaryManifestEntry.Projection.create(projectedType)
                        .createEntry()
                        .replace(
                                GenericRow.of(
                                        GenericRow.of(
                                                serializeBinaryRow(maxKey),
                                                BinaryString.fromString("data.parquet"),
                                                serializeBinaryRow(minKey)),
                                        8,
                                        serializeBinaryRow(partition),
                                        FileKind.ADD.toByteValue()));

        assertThat(entry.kind()).isEqualTo(FileKind.ADD);
        assertThat(entry.partition()).isEqualTo(partition);
        assertThat(entry.totalBuckets()).isEqualTo(8);
        assertThat(entry.fileName()).isEqualTo("data.parquet");
        assertThat(entry.minKey()).isEqualTo(minKey);
        assertThat(entry.maxKey()).isEqualTo(maxKey);
        assertUnsupported(entry::bucket, ManifestEntry.BUCKET);
        assertUnsupported(entry::rowCount, DataFileMeta.ROW_COUNT);
    }

    @Test
    void testReusesAndClearsBinaryViews() {
        BinaryManifestEntry entry = projection(false, DataFileMeta.FILE_NAME).createEntry();
        entry.replace(
                GenericRow.of(
                        FileKind.ADD.toByteValue(),
                        serializeBinaryRow(BinaryRow.EMPTY_ROW),
                        GenericRow.of(BinaryString.fromString("first.parquet"))));
        BinaryDataFileMeta file = entry.file();
        assertThat(file.fileName()).isEqualTo("first.parquet");

        entry.replace(
                GenericRow.of(
                        FileKind.DELETE.toByteValue(),
                        serializeBinaryRow(BinaryRow.EMPTY_ROW),
                        GenericRow.of(BinaryString.fromString("second.parquet"))));
        assertThat(entry.file()).isSameAs(file);
        assertThat(file.fileName()).isEqualTo("second.parquet");

        entry.clear();
        assertThatThrownBy(entry::file)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not backed by a row");
        assertThatThrownBy(file::fileName)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not backed by a row");
    }

    @Test
    void testDoesNotReusePartitionAndUpdatesPartitionedIdentifier() {
        BinaryManifestEntry entry =
                projection(
                                true,
                                DataFileMeta.FILE_NAME,
                                DataFileMeta.LEVEL,
                                DataFileMeta.EXTRA_FILES,
                                DataFileMeta.EMBEDDED_FILE_INDEX,
                                DataFileMeta.EXTERNAL_PATH)
                        .createEntry();
        BinaryManifestEntry.ReusableIdentifier identifier =
                new BinaryManifestEntry.ReusableIdentifier();

        entry.replace(identityRow(partition(1)));
        BinaryRow firstPartition = entry.partition();
        assertThat(firstPartition.getInt(0)).isEqualTo(1);
        assertThat(entry.partition()).isNotSameAs(firstPartition);
        identifier.replaceWithPartition(entry);
        byte[] firstIdentifier = Arrays.copyOf(identifier.bytes(), identifier.length());

        entry.replace(identityRow(partition(2)));
        assertThat(firstPartition.getInt(0)).isEqualTo(1);
        assertThat(entry.partition().getInt(0)).isEqualTo(2);
        identifier.replaceWithPartition(entry);
        assertThat(Arrays.copyOf(identifier.bytes(), identifier.length()))
                .isNotEqualTo(firstIdentifier);
    }

    @Test
    void testProjectionWithoutFile() {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        RowType projectedType =
                new RowType(
                        false,
                        java.util.Collections.singletonList(
                                manifestType.getField(ManifestEntry.KIND)));
        BinaryManifestEntry entry =
                BinaryManifestEntry.Projection.create(projectedType)
                        .createEntry()
                        .replace(GenericRow.of(FileKind.ADD.toByteValue()));

        assertThat(entry.kind()).isEqualTo(FileKind.ADD);
        assertUnsupported(entry::file, ManifestEntry.FILE);
    }

    @Test
    void testDoesNotValidateFileKindOnReplace() {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        RowType projectedType =
                new RowType(
                        false,
                        java.util.Collections.singletonList(
                                manifestType.getField(ManifestEntry.KIND)));
        BinaryManifestEntry entry =
                BinaryManifestEntry.Projection.create(projectedType)
                        .createEntry()
                        .replace(GenericRow.of((byte) 99));

        assertThat(entry.isAdd()).isFalse();
        assertThat(entry.isDelete()).isFalse();
    }

    private static BinaryManifestEntry.Projection projection(
            boolean includeBucket, String... projectedFileFields) {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        List<DataField> fields = new ArrayList<>();
        fields.add(manifestType.getField(ManifestEntry.KIND));
        fields.add(manifestType.getField(ManifestEntry.PARTITION));
        if (includeBucket) {
            fields.add(manifestType.getField(ManifestEntry.BUCKET));
        }
        fields.add(
                manifestType
                        .getField(ManifestEntry.FILE)
                        .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return BinaryManifestEntry.Projection.create(new RowType(false, fields));
    }

    private static GenericRow identityRow(BinaryRow partition) {
        return GenericRow.of(
                FileKind.ADD.toByteValue(),
                serializeBinaryRow(partition),
                3,
                GenericRow.of(
                        BinaryString.fromString("data.parquet"),
                        2,
                        new GenericArray(new Object[0]),
                        null,
                        null));
    }

    private static BinaryRow partition(int value) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, value);
        writer.complete();
        return partition;
    }

    private static void assertUnsupported(ThrowingSupplier call, String field) {
        assertThatThrownBy(call::get)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(field);
    }

    private interface ThrowingSupplier {

        Object get();
    }
}
