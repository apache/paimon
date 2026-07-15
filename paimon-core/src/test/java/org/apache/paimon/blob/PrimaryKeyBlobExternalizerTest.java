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

package org.apache.paimon.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PrimaryKeyBlobExternalizer}. */
class PrimaryKeyBlobExternalizerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testRejectsNonBlobManagedField() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);

        assertThatThrownBy(
                        () ->
                                new PrimaryKeyBlobExternalizer(
                                        fileIO,
                                        RowType.of(DataTypes.INT()),
                                        Collections.singleton("f0"),
                                        pathFactory,
                                        1024L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Managed BLOB field 'f0' must be BLOB or ARRAY<BLOB>, but was INT.");
    }

    @Test
    void testRejectsUnknownManagedField() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);

        assertThatThrownBy(
                        () ->
                                new PrimaryKeyBlobExternalizer(
                                        fileIO,
                                        RowType.of(DataTypes.BLOB()),
                                        Collections.singleton("missing"),
                                        pathFactory,
                                        1024L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Managed BLOB fields do not exist in value type: [missing].");
    }

    @Test
    void testExternalizeRawBlobBeforeBuffering() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        RowType valueType = RowType.of(DataTypes.INT(), DataTypes.BLOB());
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO, valueType, Collections.singleton("f1"), pathFactory, 1024L);
        byte[] expected = "managed-blob".getBytes(StandardCharsets.UTF_8);

        InternalRow result =
                externalizer.externalize(RowKind.INSERT, GenericRow.of(1, Blob.fromData(expected)));
        Blob blob = result.getBlob(1);

        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(blob.toDescriptor().uri())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);

        externalizer.prepareCommit();

        assertThat(blob.toData()).isEqualTo(expected);
        assertThat(fileIO.exists(new Path(blob.toDescriptor().uri()))).isTrue();
    }

    @Test
    void testRematerializesBlobRefInput() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.INT(), DataTypes.BLOB()),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);
        byte[] expected = "external-blob".getBytes(StandardCharsets.UTF_8);
        Path source = new Path(tempDir.resolve("external.blob").toUri());
        try (org.apache.paimon.fs.PositionOutputStream out =
                fileIO.newOutputStream(source, false)) {
            out.write(expected);
        }
        BlobRef blobRef = (BlobRef) Blob.fromFile(fileIO, source.toString());

        Blob result =
                externalizer.externalize(RowKind.INSERT, GenericRow.of(1, blobRef)).getBlob(1);

        assertThat(result).isInstanceOf(BlobRef.class);
        assertThat(result.toDescriptor().uri())
                .isNotEqualTo(source.toString())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        externalizer.prepareCommit();
        assertThat(result.toData()).isEqualTo(expected);
    }

    @Test
    void testExternalizesOnlyDeclaredFields() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        RowType valueType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.BLOB(), DataTypes.BLOB()
                        },
                        new String[] {"id", "managed", "unmanaged"});
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO, valueType, Collections.singleton("managed"), pathFactory, 1024L);
        Blob unmanaged = Blob.fromData(new byte[] {2});

        InternalRow result =
                externalizer.externalize(
                        RowKind.INSERT, GenericRow.of(1, Blob.fromData(new byte[] {1}), unmanaged));

        assertThat(result.getBlob(1)).isInstanceOf(BlobRef.class);
        assertThat(result.getBlob(2)).isSameAs(unmanaged);
    }

    @Test
    void testRetractSkipsPayloadAndAbortDeletesPrivatePack() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.INT(), DataTypes.BLOB()),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);

        InternalRow retract =
                externalizer.externalize(
                        RowKind.DELETE, GenericRow.of(1, Blob.fromData(new byte[] {1})));
        assertThat(retract.isNullAt(1)).isTrue();
        assertThat(fileIO.listStatus(bucketPath)).isEmpty();

        Blob privateBlob =
                externalizer
                        .externalize(
                                RowKind.INSERT, GenericRow.of(2, Blob.fromData(new byte[] {2})))
                        .getBlob(1);
        Path privatePack = new Path(privateBlob.toDescriptor().uri());
        assertThat(fileIO.exists(privatePack)).isTrue();

        externalizer.abort();

        assertThat(fileIO.exists(privatePack)).isFalse();
    }

    @Test
    void testExternalizeBlobArrayElements() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.BLOB())),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);
        byte[] expected = "array-element".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second-array-element".getBytes(StandardCharsets.UTF_8);

        InternalRow result =
                externalizer.externalize(
                        RowKind.INSERT,
                        GenericRow.of(
                                1,
                                new GenericArray(
                                        new Object[] {
                                            Blob.fromData(expected), null, Blob.fromData(second)
                                        })));
        InternalArray blobs = result.getArray(1);

        assertThat(blobs.size()).isEqualTo(3);
        assertThat(blobs.getBlob(0)).isInstanceOf(BlobRef.class);
        assertThat(blobs.getBlob(0).toDescriptor().uri())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        assertThat(blobs.isNullAt(1)).isTrue();
        assertThat(blobs.getBlob(2)).isInstanceOf(BlobRef.class);

        externalizer.prepareCommit();
        assertThat(blobs.getBlob(0).toData()).isEqualTo(expected);
        assertThat(blobs.getBlob(2).toData()).isEqualTo(second);
    }

    @Test
    void testRematerializesBlobRefArrayElement() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.BLOB())),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);
        byte[] expected = "external-array-blob".getBytes(StandardCharsets.UTF_8);
        Path source = new Path(tempDir.resolve("external-array.blob").toUri());
        try (org.apache.paimon.fs.PositionOutputStream out =
                fileIO.newOutputStream(source, false)) {
            out.write(expected);
        }
        BlobRef blobRef = (BlobRef) Blob.fromFile(fileIO, source.toString());

        InternalArray result =
                externalizer
                        .externalize(
                                RowKind.INSERT,
                                GenericRow.of(
                                        1,
                                        new GenericArray(
                                                new Object[] {
                                                    Blob.fromData(new byte[] {1}), blobRef
                                                })))
                        .getArray(1);

        assertThat(result.getBlob(1)).isInstanceOf(BlobRef.class);
        assertThat(result.getBlob(1).toDescriptor().uri())
                .isNotEqualTo(source.toString())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        externalizer.prepareCommit();
        assertThat(result.getBlob(1).toData()).isEqualTo(expected);
    }

    @Test
    void testBlobArrayNullEmptyRetractAndPlaceholder() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.BLOB())),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);

        GenericRow nullArray = GenericRow.of(1, null);
        GenericRow emptyArray = GenericRow.of(2, new GenericArray(new Object[0]));
        GenericRow nullElement = GenericRow.of(3, new GenericArray(new Object[] {null}));
        assertThat(externalizer.externalize(RowKind.INSERT, nullArray)).isSameAs(nullArray);
        assertThat(externalizer.externalize(RowKind.INSERT, emptyArray)).isSameAs(emptyArray);
        assertThat(externalizer.externalize(RowKind.INSERT, nullElement)).isSameAs(nullElement);

        InternalRow retract =
                externalizer.externalize(
                        RowKind.DELETE, GenericRow.of(4, BlobArrayPlaceholder.INSTANCE));
        assertThat(retract.isNullAt(1)).isTrue();

        assertThatThrownBy(
                        () ->
                                externalizer.externalize(
                                        RowKind.INSERT,
                                        GenericRow.of(5, BlobArrayPlaceholder.INSTANCE)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("placeholder blob array");
        assertThat(fileIO.listStatus(bucketPath)).isEmpty();
    }
}
