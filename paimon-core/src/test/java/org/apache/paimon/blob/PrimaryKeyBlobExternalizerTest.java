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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobMapPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.blob.BlobFormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PrimaryKeyBlobExternalizer}. */
class PrimaryKeyBlobExternalizerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testClosesPackStreamWhenFlushFails() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        LocalFileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                            throws IOException {
                        return new PositionOutputStreamWrapper(
                                super.newOutputStream(path, overwrite)) {
                            @Override
                            public void flush() throws IOException {
                                throw new IOException("flush failure");
                            }

                            @Override
                            public void close() throws IOException {
                                closed.set(true);
                                super.close();
                            }
                        };
                    }
                };
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                newExternalizer(
                        fileIO,
                        RowType.of(DataTypes.BLOB()),
                        Collections.singleton("f0"),
                        pathFactory,
                        1024L);
        externalizer.externalize(RowKind.INSERT, GenericRow.of(Blob.fromData(new byte[] {1})));

        assertThatThrownBy(externalizer::prepareCommit)
                .isInstanceOf(IOException.class)
                .hasMessage("flush failure");
        assertThat(closed).isTrue();
    }

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
                                newExternalizer(
                                        fileIO,
                                        RowType.of(DataTypes.INT()),
                                        Collections.singleton("f0"),
                                        pathFactory,
                                        1024L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Managed BLOB field 'f0' must be BLOB, ARRAY<BLOB> or MAP<X, BLOB>, but was INT.");
    }

    @Test
    void testExternalizeBlobMapValues() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);

        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(
                                DataTypes.INT(), DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);
        byte[] first = "first-map-value".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second-map-value".getBytes(StandardCharsets.UTF_8);
        Map<Object, Object> input = new LinkedHashMap<>();
        input.put(1, Blob.fromData(first));
        input.put(2, null);
        input.put(3, Blob.fromData(second));

        InternalMap result =
                externalizer
                        .externalize(RowKind.INSERT, GenericRow.of(1, new GenericMap(input)))
                        .getMap(1);
        InternalArray keys = result.keyArray();
        InternalArray values = result.valueArray();

        assertThat(result.size()).isEqualTo(3);
        assertThat(keys.getInt(0)).isEqualTo(1);
        assertThat(keys.getInt(1)).isEqualTo(2);
        assertThat(keys.getInt(2)).isEqualTo(3);
        assertThat(values.getBlob(0)).isInstanceOf(BlobRef.class);
        assertThat(values.isNullAt(1)).isTrue();
        assertThat(values.getBlob(2)).isInstanceOf(BlobRef.class);

        externalizer.prepareCommit();
        assertThat(values.getBlob(0).toData()).isEqualTo(first);
        assertThat(values.getBlob(2).toData()).isEqualTo(second);
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
                                newExternalizer(
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
                newExternalizer(fileIO, valueType, Collections.singleton("f1"), pathFactory, 1024L);
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
                newExternalizer(
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
                newExternalizer(
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
                newExternalizer(
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
                newExternalizer(
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
                newExternalizer(
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
    void testRematerializesBlobRefMapValueWithRange() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(
                                DataTypes.INT(),
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);
        byte[] expected = "ranged-map-value".getBytes(StandardCharsets.UTF_8);
        byte[] prefix = "prefix".getBytes(StandardCharsets.UTF_8);
        byte[] suffix = "suffix".getBytes(StandardCharsets.UTF_8);
        Path source = new Path(tempDir.resolve("external-map.blob").toUri());
        try (org.apache.paimon.fs.PositionOutputStream out =
                fileIO.newOutputStream(source, false)) {
            out.write(prefix);
            out.write(expected);
            out.write(suffix);
        }
        Blob ranged = Blob.fromFile(fileIO, source.toString(), prefix.length, expected.length);
        Map<Object, Object> input = new LinkedHashMap<>();
        input.put(BinaryString.fromString("slice"), ranged);

        Blob result =
                externalizer
                        .externalize(RowKind.INSERT, GenericRow.of(1, new GenericMap(input)))
                        .getMap(1)
                        .valueArray()
                        .getBlob(0);

        assertThat(result).isInstanceOf(BlobRef.class);
        assertThat(result.toDescriptor().uri())
                .isNotEqualTo(source.toString())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        assertThat(result.toDescriptor().offset()).isPositive();
        assertThat(result.toDescriptor().length()).isEqualTo(expected.length);
        externalizer.prepareCommit();
        assertThat(result.toData()).isEqualTo(expected);
    }

    @Test
    void testDuplicateMapKeyUsesLastValueWithoutWritingOverriddenBlob() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                        Collections.singleton("f0"),
                        pathFactory,
                        1L);
        byte[] expected = "last".getBytes(StandardCharsets.UTF_8);
        InternalMap duplicateKeys =
                new InternalMap() {
                    @Override
                    public int size() {
                        return 2;
                    }

                    @Override
                    public InternalArray keyArray() {
                        return new GenericArray(new Object[] {1, 1});
                    }

                    @Override
                    public InternalArray valueArray() {
                        return new GenericArray(
                                new Object[] {
                                    Blob.fromData("overridden".getBytes(StandardCharsets.UTF_8)),
                                    Blob.fromData(expected)
                                });
                    }
                };

        InternalMap result =
                externalizer.externalize(RowKind.INSERT, GenericRow.of(duplicateKeys)).getMap(0);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.keyArray().getInt(0)).isEqualTo(1);
        assertThat(result.valueArray().getBlob(0).toData()).isEqualTo(expected);
        assertThat(fileIO.listStatus(bucketPath))
                .singleElement()
                .extracting(status -> status.getPath().getName())
                .asString()
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
    }

    @Test
    void testDuplicateMapKeyUsesLastNullWithoutWritingBlob() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                        Collections.singleton("f0"),
                        pathFactory,
                        1024L);
        InternalMap duplicateKeys =
                new InternalMap() {
                    @Override
                    public int size() {
                        return 2;
                    }

                    @Override
                    public InternalArray keyArray() {
                        return new GenericArray(new Object[] {1, 1});
                    }

                    @Override
                    public InternalArray valueArray() {
                        return new GenericArray(
                                new Object[] {
                                    Blob.fromData("overridden".getBytes(StandardCharsets.UTF_8)),
                                    null
                                });
                    }
                };

        InternalMap result =
                externalizer.externalize(RowKind.INSERT, GenericRow.of(duplicateKeys)).getMap(0);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.keyArray().getInt(0)).isEqualTo(1);
        assertThat(result.valueArray().isNullAt(0)).isTrue();
        assertThat(fileIO.listStatus(bucketPath)).isEmpty();
    }

    @Test
    void testBlobMapNullEmptyRetractAndPlaceholder() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO,
                        RowType.of(
                                DataTypes.INT(), DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                        Collections.singleton("f1"),
                        pathFactory,
                        1024L);

        GenericRow nullMap = GenericRow.of(1, null);
        GenericRow emptyMap = GenericRow.of(2, new GenericMap(new LinkedHashMap<>()));
        Map<Object, Object> nullValue = new LinkedHashMap<>();
        nullValue.put(1, null);
        GenericRow allNullMap = GenericRow.of(3, new GenericMap(nullValue));
        assertThat(externalizer.externalize(RowKind.INSERT, nullMap)).isSameAs(nullMap);
        assertThat(externalizer.externalize(RowKind.INSERT, emptyMap)).isSameAs(emptyMap);
        assertThat(externalizer.externalize(RowKind.INSERT, allNullMap)).isSameAs(allNullMap);

        InternalRow retract =
                externalizer.externalize(
                        RowKind.DELETE, GenericRow.of(4, BlobMapPlaceholder.INSTANCE));
        assertThat(retract.isNullAt(1)).isTrue();

        assertThatThrownBy(
                        () ->
                                externalizer.externalize(
                                        RowKind.INSERT,
                                        GenericRow.of(5, BlobMapPlaceholder.INSTANCE)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("placeholder blob map");
        assertThat(fileIO.listStatus(bucketPath)).isEmpty();
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
                newExternalizer(
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

    private static PrimaryKeyBlobExternalizer newExternalizer(
            LocalFileIO fileIO,
            RowType valueType,
            java.util.Set<String> managedBlobFields,
            DataFilePathFactory pathFactory,
            long targetFileSize) {
        return new PrimaryKeyBlobExternalizer(
                fileIO,
                valueType,
                managedBlobFields,
                pathFactory,
                targetFileSize,
                BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
    }
}
