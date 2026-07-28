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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VersionedObjectSerializer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BinaryManifestEntry}. */
public class BinaryManifestEntryTest {

    private static final String FILE_NAME = "_FILE_NAME";
    private static final String ROW_COUNT = "_ROW_COUNT";
    private static final String MIN_KEY = "_MIN_KEY";
    private static final String MAX_KEY = "_MAX_KEY";
    private static final String LEVEL = "_LEVEL";
    private static final String EXTRA_FILES = "_EXTRA_FILES";
    private static final String EMBEDDED_FILE_INDEX = "_EMBEDDED_FILE_INDEX";
    private static final String EXTERNAL_PATH = "_EXTERNAL_PATH";
    private static final String FIRST_ROW_ID = "_FIRST_ROW_ID";

    @Test
    void testImplementsProjectedManifestEntry() {
        BinaryRow partition = BinaryRow.EMPTY_ROW;
        BinaryManifestEntry entry =
                projection(
                                true,
                                FILE_NAME,
                                ROW_COUNT,
                                LEVEL,
                                EXTRA_FILES,
                                EMBEDDED_FILE_INDEX,
                                EXTERNAL_PATH,
                                FIRST_ROW_ID)
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

        assertUnsupported(manifestEntry::totalBuckets, "_TOTAL_BUCKETS");
        assertUnsupported(manifestEntry::minKey, MIN_KEY);
        assertUnsupported(manifestEntry::maxKey, MAX_KEY);
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
                projection(false, ROW_COUNT)
                        .createEntry()
                        .replace(
                                GenericRow.of(
                                        FileKind.DELETE.toByteValue(),
                                        serializeBinaryRow(BinaryRow.EMPTY_ROW),
                                        GenericRow.of(5L)));

        assertThat(entry.kind()).isEqualTo(FileKind.DELETE);
        assertThat(entry.partition()).isEqualTo(BinaryRow.EMPTY_ROW);
        assertThat(entry.rowCount()).isEqualTo(5L);
        assertUnsupported(entry::bucket, "_BUCKET");
        assertUnsupported(entry::fileName, "_FILE_NAME");
        assertUnsupported(entry::firstRowId, "_FIRST_ROW_ID");
        assertUnsupported(entry::identifier, "_BUCKET");
    }

    @Test
    void testProjectedNullIsNotUnsupported() {
        FileEntry entry =
                projection(false, FIRST_ROW_ID)
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
        RowType manifestType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
        RowType projectedFileType = DataFileMeta.SCHEMA.project(MAX_KEY, FILE_NAME, MIN_KEY);
        RowType projectedType =
                new RowType(
                        false,
                        java.util.Arrays.asList(
                                manifestType.getField("_FILE").newType(projectedFileType),
                                manifestType.getField("_TOTAL_BUCKETS"),
                                manifestType.getField("_PARTITION"),
                                manifestType.getField("_KIND")));
        BinaryManifestEntry entry =
                BinaryManifestEntry.Projection.create(format(), projectedType)
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
        assertUnsupported(entry::bucket, "_BUCKET");
        assertUnsupported(entry::rowCount, ROW_COUNT);
    }

    @Test
    void testReusesAndClearsBinaryViews() {
        BinaryManifestEntry entry = projection(false, FILE_NAME).createEntry();
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
    void testProjectionWithoutFile() {
        RowType manifestType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
        RowType projectedType =
                new RowType(
                        false, java.util.Collections.singletonList(manifestType.getField("_KIND")));
        BinaryManifestEntry entry =
                BinaryManifestEntry.Projection.create(format(), projectedType)
                        .createEntry()
                        .replace(GenericRow.of(FileKind.ADD.toByteValue()));

        assertThat(entry.kind()).isEqualTo(FileKind.ADD);
        assertUnsupported(entry::file, "_FILE");
    }

    @Test
    void testDoesNotValidateFileKindOnReplace() {
        RowType manifestType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
        RowType projectedType =
                new RowType(
                        false, java.util.Collections.singletonList(manifestType.getField("_KIND")));
        BinaryManifestEntry entry =
                BinaryManifestEntry.Projection.create(format(), projectedType)
                        .createEntry()
                        .replace(GenericRow.of((byte) 99));

        assertThat(entry.isAdd()).isFalse();
        assertThat(entry.isDelete()).isFalse();
    }

    private static BinaryManifestEntry.Projection projection(
            boolean includeBucket, String... projectedFileFields) {
        RowType manifestType = VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
        List<DataField> fields = new ArrayList<>();
        fields.add(manifestType.getField("_KIND"));
        fields.add(manifestType.getField("_PARTITION"));
        if (includeBucket) {
            fields.add(manifestType.getField("_BUCKET"));
        }
        fields.add(
                manifestType
                        .getField("_FILE")
                        .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return BinaryManifestEntry.Projection.create(format(), new RowType(false, fields));
    }

    private static FileFormat format() {
        return FileFormat.manifestFormat(new CoreOptions(new Options()));
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
