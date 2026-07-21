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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobMapPlaceholder;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BlobFileFormat}. */
public class BlobFileFormatTest {

    @TempDir java.nio.file.Path tempPath;

    protected FileIO fileIO;
    protected Path file;
    protected Path parent;

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    @Test
    public void testBlobAsDescriptor() throws IOException {
        innerTest(true);
    }

    @Test
    public void testReadBlobInlineBytes() throws IOException {
        innerTest(false);
    }

    @Test
    public void testRawDescriptorReaderDoesNotOwnInputStream() throws IOException {
        BlobFileFormat format = new BlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.BLOB());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            writer.addElement(GenericRow.of(new BlobData("blob".getBytes())));
            writer.close();
        }

        TrackingLocalFileIO trackingFileIO = new TrackingLocalFileIO();
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(trackingFileIO, file, trackingFileIO.getFileSize(file));
        TrackingSeekableInputStream trackingInputStream;
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            trackingInputStream = trackingFileIO.lastInputStream;
            assertThat(trackingInputStream).isNotNull();
            assertThat(trackingInputStream.closeCount).isOne();
            assertThat(reader.readBatch().next().getBlob(0)).isInstanceOf(BlobRef.class);
            assertThat(trackingInputStream.closeCount).isOne();
        }
        assertThat(trackingInputStream.closeCount).isOne();
    }

    @Test
    public void testPublicRawDescriptorReaderDoesNotOwnInputStream() throws IOException {
        BlobFileFormat format = new BlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.BLOB());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            writer.addElement(GenericRow.of(new BlobData("blob".getBytes())));
            writer.close();
        }

        TrackingLocalFileIO trackingFileIO = new TrackingLocalFileIO();
        TrackingSeekableInputStream trackingInputStream =
                (TrackingSeekableInputStream) trackingFileIO.newInputStream(file);
        BlobFileMeta fileMeta =
                new BlobFileMeta(trackingInputStream, trackingFileIO.getFileSize(file), null);
        try (BlobFormatReader reader =
                new BlobFormatReader(
                        trackingFileIO,
                        file,
                        fileMeta,
                        trackingInputStream,
                        1,
                        0,
                        DataTypes.BLOB(),
                        true)) {
            assertThat(trackingInputStream.closeCount).isOne();
            assertThat(reader.readBatch().next().getBlob(0)).isInstanceOf(BlobRef.class);
        }
        assertThat(trackingInputStream.closeCount).isOne();
    }

    @Test
    public void testArrayDescriptorReaderOwnsInputStream() throws IOException {
        BlobFileFormat format = new BlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            writer.addElement(
                    GenericRow.of(
                            new GenericArray(new Object[] {new BlobData("blob".getBytes())})));
            writer.close();
        }

        TrackingLocalFileIO trackingFileIO = new TrackingLocalFileIO();
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(trackingFileIO, file, trackingFileIO.getFileSize(file));
        TrackingSeekableInputStream trackingInputStream;
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            trackingInputStream = trackingFileIO.lastInputStream;
            assertThat(trackingInputStream.closeCount).isZero();
            assertThat(reader.readBatch().next().getArray(0).getBlob(0))
                    .isInstanceOf(BlobRef.class);
            assertThat(trackingInputStream.closeCount).isZero();
        }
        assertThat(trackingInputStream.closeCount).isOne();
    }

    @Test
    public void testElementSerializerReadInputStreamRequirements() {
        BlobElementSerializer raw = new RawBlobElementSerializer();
        assertThat(raw.requiresReadInputStream(false)).isTrue();
        assertThat(raw.requiresReadInputStream(true)).isFalse();

        BlobElementSerializer array = new ArrayBlobElementSerializer();
        assertThat(array.requiresReadInputStream(false)).isTrue();
        assertThat(array.requiresReadInputStream(true)).isTrue();

        BlobElementSerializer map = new MapBlobElementSerializer(DataTypes.STRING());
        assertThat(map.requiresReadInputStream(false)).isTrue();
        assertThat(map.requiresReadInputStream(true)).isTrue();
    }

    @Test
    public void testReaderCreationFailureClosesInputStream() throws IOException {
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            out.write(1);
        }

        TrackingLocalFileIO trackingFileIO = new TrackingLocalFileIO();
        BlobFileFormat format = new BlobFileFormat(true, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.BLOB());
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(trackingFileIO, file, trackingFileIO.getFileSize(file));

        assertThatThrownBy(() -> readerFactory.createReader(context));
        assertThat(trackingFileIO.lastInputStream.closeCount).isOne();
    }

    @Test
    public void testArrayBlobAsDescriptor() throws IOException {
        innerTestArray(true);
    }

    @Test
    public void testReadArrayBlobInlineBytes() throws IOException {
        innerTestArray(false);
    }

    @Test
    public void testMapBlobAsDescriptor() throws IOException {
        innerTestMap(true);
    }

    @Test
    public void testReadMapBlobInlineBytes() throws IOException {
        innerTestMap(false);
    }

    @Test
    public void testWriteArrayBlobPlaceholderWithProjectedRow() throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = format.createWriterFactory(rowType).create(out, null);
            ProjectedRow projectedRow = ProjectedRow.from(new int[] {1});
            formatWriter.addElement(
                    projectedRow.replaceRow(GenericRow.of(0, BlobArrayPlaceholder.INSTANCE)));
            formatWriter.close();
        }

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<InternalRow> rows = new ArrayList<>();
        readerFactory.createReader(context).forEachRemaining(rows::add);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getArray(0)).isSameAs(BlobArrayPlaceholder.INSTANCE);
    }

    @Test
    public void testWriteMapBlobPlaceholderWithProjectedRow() throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()));

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = format.createWriterFactory(rowType).create(out, null);
            ProjectedRow projectedRow = ProjectedRow.from(new int[] {1});
            formatWriter.addElement(
                    projectedRow.replaceRow(GenericRow.of(0, BlobMapPlaceholder.INSTANCE)));
            formatWriter.close();
        }

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<InternalRow> rows = new ArrayList<>();
        readerFactory.createReader(context).forEachRemaining(rows::add);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getMap(0)).isSameAs(BlobMapPlaceholder.INSTANCE);
    }

    @Test
    public void testRejectUnsupportedArrayBlobPayloadVersion() throws IOException {
        assertMalformedArrayPayload(
                (bytes, position, length) -> bytes[position + Integer.BYTES] = 0,
                "Unsupported ARRAY<BLOB> payload version");
    }

    @Test
    public void testRejectInvalidArrayBlobElementCount() throws IOException {
        assertMalformedArrayPayload(
                (bytes, position, length) -> {
                    int countPosition = position + Integer.BYTES + 1;
                    Arrays.fill(bytes, countPosition, countPosition + Integer.BYTES, (byte) 0xff);
                },
                "Invalid ARRAY<BLOB> element count");
    }

    @Test
    public void testRejectInvalidArrayBlobIndexLength() throws IOException {
        assertMalformedArrayPayload(
                (bytes, position, length) -> {
                    int indexLengthPosition = position + length - Integer.BYTES;
                    Arrays.fill(
                            bytes,
                            indexLengthPosition,
                            indexLengthPosition + Integer.BYTES,
                            (byte) 0xff);
                },
                "Invalid ARRAY<BLOB> element index length");
    }

    @Test
    public void testRejectInvalidArrayBlobElementLength() throws IOException {
        assertMalformedArrayPayload(
                (bytes, position, length) -> bytes[position + length - Integer.BYTES - 1] = 3,
                "Invalid ARRAY<BLOB> element length");
    }

    @Test
    public void testRejectArrayBlobElementLengthMismatch() throws IOException {
        assertMalformedArrayPayload(
                (bytes, position, length) -> bytes[position + length - Integer.BYTES - 1] = 4,
                "element lengths exceed the payload data length");
    }

    @Test
    public void testRejectUnsupportedMapBlobPayloadVersion() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[position + Integer.BYTES] = 0,
                "Unsupported MAP<X, BLOB> payload version");
    }

    @Test
    public void testRejectInvalidMapBlobPayloadMagic() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[position] = 0,
                "Invalid MAP<X, BLOB> payload magic number");
    }

    @Test
    public void testRejectInvalidMapBlobEntryCount() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> {
                    int countPosition = position + Integer.BYTES + 1;
                    Arrays.fill(bytes, countPosition, countPosition + Integer.BYTES, (byte) 0xff);
                },
                "Invalid MAP<X, BLOB> entry count");
    }

    @Test
    public void testRejectInvalidMapBlobIndexLength() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> {
                    int indexLengthPosition = position + length - Integer.BYTES;
                    Arrays.fill(
                            bytes,
                            indexLengthPosition,
                            indexLengthPosition + Integer.BYTES,
                            (byte) 0xff);
                },
                "Invalid MAP<X, BLOB> value index length");
    }

    @Test
    public void testRejectInvalidMapBlobKeyIndexLength() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> {
                    int indexLengthPosition = position + length - 2 * Integer.BYTES;
                    Arrays.fill(
                            bytes,
                            indexLengthPosition,
                            indexLengthPosition + Integer.BYTES,
                            (byte) 0xff);
                },
                "Invalid MAP<X, BLOB> key index length");
    }

    @Test
    public void testRejectMapBlobIndexesOutsidePayload() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> {
                    writeLittleEndianInt(bytes, position + length - 2 * Integer.BYTES, 3);
                    writeLittleEndianInt(bytes, position + length - Integer.BYTES, 3);
                },
                "indexes exceed the payload length");
    }

    @Test
    public void testRejectMapBlobEntryCountIndexMismatch() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> {
                    int countPosition = position + Integer.BYTES + 1;
                    Arrays.fill(bytes, countPosition, countPosition + Integer.BYTES, (byte) 0);
                },
                "entry count does not match key index length");
    }

    @Test
    public void testRejectInvalidMapBlobKeyLength() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[keyIndexPosition(bytes, position, length)] = 3,
                "Invalid MAP<X, BLOB> key length");
    }

    @Test
    public void testRejectMapBlobKeyOutsidePayload() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[keyIndexPosition(bytes, position, length)] = 6,
                "key lengths exceed the payload data length");
    }

    @Test
    public void testRejectInvalidFixedWidthMapBlobKey() throws IOException {
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(7, new BlobData("a".getBytes()));
        assertMalformedMapPayload(
                RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                new GenericMap(entries),
                (bytes, position, length) -> bytes[keyIndexPosition(bytes, position, length)] = 6,
                "Invalid MAP<X, BLOB> fixed-width key length");
    }

    @Test
    public void testRejectInvalidMapBlobValueLength() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[valueIndexPosition(bytes, position, length)] = 3,
                "Invalid MAP<X, BLOB> value length");
    }

    @Test
    public void testRejectMapBlobDataLengthMismatch() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[valueIndexPosition(bytes, position, length)] = 4,
                "value lengths exceed the payload data length");
    }

    @Test
    public void testRejectTrailingMapBlobEntryData() throws IOException {
        assertMalformedMapPayload(
                (bytes, position, length) -> bytes[valueIndexPosition(bytes, position, length)] = 0,
                "key/value lengths do not match the payload data length");
    }

    @Test
    public void testDuplicateMapBlobKeyLastWinsInline() throws IOException {
        assertDuplicateMapBlobKeyLastWins(false);
    }

    @Test
    public void testDuplicateMapBlobKeyLastWinsAsDescriptor() throws IOException {
        assertDuplicateMapBlobKeyLastWins(true);
    }

    private void assertDuplicateMapBlobKeyLastWins(boolean blobAsDescriptor) throws IOException {
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(BinaryString.fromString("a"), new BlobData("first".getBytes()));
        entries.put(BinaryString.fromString("b"), new BlobData("second".getBytes()));

        BlobFileFormat format =
                new BlobFileFormat(blobAsDescriptor, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            writer.addElement(GenericRow.of(new GenericMap(entries)));
            writer.close();
        }

        int payloadPosition;
        try (SeekableInputStream input = fileIO.newInputStream(file)) {
            BlobFileMeta fileMeta = new BlobFileMeta(input, fileIO.getFileSize(file), null);
            payloadPosition = Math.toIntExact(fileMeta.blobOffset(0) + Integer.BYTES);
        }

        java.nio.file.Path localFile = Paths.get(file.toUri());
        byte[] bytes = Files.readAllBytes(localFile);
        bytes[payloadPosition + 10] = 'a';
        Files.write(localFile, bytes);

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<InternalRow> rows = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            reader.forEachRemaining(rows::add);
        }

        assertThat(rows).hasSize(1);
        GenericMap result = (GenericMap) rows.get(0).getMap(0);
        assertThat(result.size()).isOne();
        assertMapBlob(
                result.get(BinaryString.fromString("a")), blobAsDescriptor, "second".getBytes());
    }

    @Test
    public void testMapBlobSupportedKeyTypes() throws IOException {
        DataType[] keyTypes =
                new DataType[] {
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.CHAR(10),
                    DataTypes.VARCHAR(10)
                };
        Object[] keys =
                new Object[] {
                    (byte) 1,
                    (short) 2,
                    3,
                    4L,
                    BinaryString.fromString("char"),
                    BinaryString.fromString("varchar")
                };

        for (int i = 0; i < keyTypes.length; i++) {
            Path mapFile = new Path(parent, UUID.randomUUID().toString());
            RowType rowType = RowType.of(DataTypes.MAP(keyTypes[i], DataTypes.BLOB()));
            Map<Object, Object> entries = new LinkedHashMap<>();
            entries.put(keys[i], new BlobData("value".getBytes()));
            try (PositionOutputStream out = fileIO.newOutputStream(mapFile, false)) {
                FormatWriter writer =
                        new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE)
                                .createWriterFactory(rowType)
                                .create(out, null);
                writer.addElement(GenericRow.of(new GenericMap(entries)));
                writer.close();
            }

            FormatReaderFactory readerFactory =
                    new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE)
                            .createReaderFactory(null, rowType, null);
            FormatReaderContext context =
                    new FormatReaderContext(fileIO, mapFile, fileIO.getFileSize(mapFile));
            List<InternalRow> rows = new ArrayList<>();
            readerFactory.createReader(context).forEachRemaining(rows::add);
            GenericMap result = (GenericMap) rows.get(0).getMap(0);
            assertThat(result.contains(keys[i])).isTrue();
            assertThat(((Blob) result.get(keys[i])).toData()).isEqualTo("value".getBytes());
        }
    }

    @Test
    public void testRejectUnsupportedMapBlobTypes() {
        assertThatThrownBy(
                        () ->
                                BlobElementSerializerFactory.create(
                                        DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.BLOB())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported key type");
        assertThatThrownBy(
                        () ->
                                BlobElementSerializerFactory.create(
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value type must be BLOB");
        assertThatThrownBy(
                        () ->
                                BlobElementSerializerFactory.create(
                                        DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.STRING())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value type must be BLOB");
    }

    private void innerTest(boolean blobAsDescriptor) throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(blobAsDescriptor, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.BLOB());

        // write
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        List<Object> blobs =
                Arrays.asList(
                        "hello".getBytes(),
                        null,
                        BlobPlaceholder.INSTANCE,
                        "world".getBytes(),
                        new byte[0]);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = writerFactory.create(out, null);
            for (Object blob : blobs) {
                if (blob == null) {
                    formatWriter.addElement(GenericRow.of((Object) null));
                } else if (blob == BlobPlaceholder.INSTANCE) {
                    formatWriter.addElement(GenericRow.of(BlobPlaceholder.INSTANCE));
                } else {
                    formatWriter.addElement(GenericRow.of(new BlobData((byte[]) blob)));
                }
            }
            formatWriter.close();
        }

        // read
        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<Object> result = new ArrayList<>();
        readerFactory
                .createReader(context)
                .forEachRemaining(
                        row -> {
                            if (row.isNullAt(0)) {
                                result.add(null);
                            } else {
                                Blob blob = row.getBlob(0);
                                if (blob == BlobPlaceholder.INSTANCE) {
                                    result.add(BlobPlaceholder.INSTANCE);
                                    return;
                                } else if (blobAsDescriptor) {
                                    assertThat(blob).isInstanceOf(BlobRef.class);
                                } else {
                                    assertThat(blob).isInstanceOf(BlobData.class);
                                }
                                result.add(blob.toData());
                            }
                        });

        // assert
        assertThat(result).hasSize(blobs.size());
        assertThat((byte[]) result.get(0)).isEqualTo((byte[]) blobs.get(0));
        assertThat(result.get(1)).isNull();
        assertThat(result.get(2)).isSameAs(BlobPlaceholder.INSTANCE);
        assertThat((byte[]) result.get(3)).isEqualTo((byte[]) blobs.get(3));
        assertThat((byte[]) result.get(4)).isEqualTo((byte[]) blobs.get(4));

        // read with selection
        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(2);
        context = new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection);
        result.clear();
        readerFactory.createReader(context).forEachRemaining(row -> result.add(row.getBlob(0)));

        // assert
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isSameAs(BlobPlaceholder.INSTANCE);
    }

    private void assertMalformedArrayPayload(
            ArrayPayloadCorruptor corruptor, String expectedMessage) throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            writer.addElement(
                    GenericRow.of(new GenericArray(new Object[] {new BlobData("a".getBytes())})));
            writer.close();
        }

        int payloadPosition;
        int payloadLength;
        try (SeekableInputStream input = fileIO.newInputStream(file)) {
            BlobFileMeta fileMeta = new BlobFileMeta(input, fileIO.getFileSize(file), null);
            payloadPosition = Math.toIntExact(fileMeta.blobOffset(0) + Integer.BYTES);
            payloadLength = Math.toIntExact(fileMeta.blobLength(0) - 16);
        }

        java.nio.file.Path localFile = Paths.get(file.toUri());
        byte[] bytes = Files.readAllBytes(localFile);
        corruptor.corrupt(bytes, payloadPosition, payloadLength);
        Files.write(localFile, bytes);

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        assertThatThrownBy(
                        () -> {
                            try (FileRecordReader<InternalRow> reader =
                                    readerFactory.createReader(context)) {
                                reader.forEachRemaining(ignored -> {});
                            }
                        })
                .hasMessageContaining(expectedMessage);
    }

    private void assertMalformedMapPayload(MapPayloadCorruptor corruptor, String expectedMessage)
            throws IOException {
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(BinaryString.fromString("a"), new BlobData("a".getBytes()));
        assertMalformedMapPayload(
                RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())),
                new GenericMap(entries),
                corruptor,
                expectedMessage);
    }

    private void assertMalformedMapPayload(
            RowType rowType, GenericMap map, MapPayloadCorruptor corruptor, String expectedMessage)
            throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, null);
            writer.addElement(GenericRow.of(map));
            writer.close();
        }

        int payloadPosition;
        int payloadLength;
        try (SeekableInputStream input = fileIO.newInputStream(file)) {
            BlobFileMeta fileMeta = new BlobFileMeta(input, fileIO.getFileSize(file), null);
            payloadPosition = Math.toIntExact(fileMeta.blobOffset(0) + Integer.BYTES);
            payloadLength = Math.toIntExact(fileMeta.blobLength(0) - 16);
        }

        java.nio.file.Path localFile = Paths.get(file.toUri());
        byte[] bytes = Files.readAllBytes(localFile);
        corruptor.corrupt(bytes, payloadPosition, payloadLength);
        Files.write(localFile, bytes);

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        assertThatThrownBy(
                        () -> {
                            try (FileRecordReader<InternalRow> reader =
                                    readerFactory.createReader(context)) {
                                reader.forEachRemaining(ignored -> {});
                            }
                        })
                .hasMessageContaining(expectedMessage);
    }

    @Test
    public void testReadWithProjectedRowTypeContainingExtraFields() throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(false, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType writeRowType = RowType.of(DataTypes.BLOB());

        // write blob data
        List<byte[]> blobs = Arrays.asList("hello".getBytes(), "world".getBytes());
        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = format.createWriterFactory(writeRowType).create(out, null);
            for (byte[] bytes : blobs) {
                formatWriter.addElement(GenericRow.of(new BlobData(bytes)));
            }
            formatWriter.close();
        }

        // read with a projectedRowType that has extra fields (simulating _ROW_ID scenario)
        // projectedRowType: <BIGINT, BLOB> — blob is at index 1
        RowType projectedRowType = RowType.of(DataTypes.BIGINT(), DataTypes.BLOB());
        FormatReaderFactory readerFactory =
                format.createReaderFactory(null, projectedRowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));

        List<InternalRow> rows = new ArrayList<>();
        readerFactory.createReader(context).forEachRemaining(rows::add);

        assertThat(rows).hasSize(2);
        for (InternalRow row : rows) {
            // row should have 2 fields
            assertThat(row.getFieldCount()).isEqualTo(2);
            // field 0 (BIGINT) should be null (default value)
            assertThat(row.isNullAt(0)).isTrue();
            // field 1 (BLOB) should contain data
            assertThat(row.isNullAt(1)).isFalse();
        }
        assertThat(rows.get(0).getBlob(1).toData()).isEqualTo("hello".getBytes());
        assertThat(rows.get(1).getBlob(1).toData()).isEqualTo("world".getBytes());
    }

    private void innerTestArray(boolean blobAsDescriptor) throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(blobAsDescriptor, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));

        GenericArray first =
                new GenericArray(
                        new Object[] {
                            new BlobData("hello".getBytes()), null, new BlobData("world".getBytes())
                        });
        GenericArray empty = new GenericArray(new Object[0]);
        List<Object> arrays = Arrays.asList(first, null, BlobArrayPlaceholder.INSTANCE, empty);

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = format.createWriterFactory(rowType).create(out, null);
            for (Object array : arrays) {
                formatWriter.addElement(GenericRow.of(array));
            }
            formatWriter.close();
        }

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<Object> result = new ArrayList<>();
        readerFactory
                .createReader(context)
                .forEachRemaining(
                        row -> {
                            if (row.isNullAt(0)) {
                                result.add(null);
                            } else {
                                addArrayBlobResult(row, blobAsDescriptor, result);
                            }
                        });

        assertThat(result).hasSize(4);
        assertThat((byte[]) ((Object[]) result.get(0))[0]).isEqualTo("hello".getBytes());
        assertThat(((Object[]) result.get(0))[1]).isNull();
        assertThat((byte[]) ((Object[]) result.get(0))[2]).isEqualTo("world".getBytes());
        assertThat(result.get(1)).isNull();
        assertThat(result.get(2)).isSameAs(BlobArrayPlaceholder.INSTANCE);
        assertThat((Object[]) result.get(3)).isEmpty();

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        context = new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection);
        result.clear();
        readerFactory
                .createReader(context)
                .forEachRemaining(row -> result.add(row.getArray(0).getBlob(0).toData()));
        assertThat(result).hasSize(1);
        assertThat((byte[]) result.get(0)).isEqualTo("hello".getBytes());
    }

    private void innerTestMap(boolean blobAsDescriptor) throws IOException {
        BlobFileFormat format =
                new BlobFileFormat(blobAsDescriptor, BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()));

        Map<Object, Object> firstEntries = new LinkedHashMap<>();
        firstEntries.put(null, new BlobData("null-key".getBytes()));
        firstEntries.put(BinaryString.fromString("null-value"), null);
        firstEntries.put(BinaryString.fromString("hello"), new BlobData("world".getBytes()));
        firstEntries.put(BinaryString.fromString("empty"), new BlobData(new byte[0]));
        GenericMap first = new GenericMap(firstEntries);
        GenericMap empty = new GenericMap(new LinkedHashMap<>());
        List<Object> maps = Arrays.asList(first, null, BlobMapPlaceholder.INSTANCE, empty);

        try (PositionOutputStream out = fileIO.newOutputStream(file, false)) {
            FormatWriter formatWriter = format.createWriterFactory(rowType).create(out, null);
            for (Object map : maps) {
                formatWriter.addElement(GenericRow.of(map));
            }
            formatWriter.close();
        }

        FormatReaderFactory readerFactory = format.createReaderFactory(null, rowType, null);
        FormatReaderContext context =
                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        List<InternalRow> rows = new ArrayList<>();
        readerFactory.createReader(context).forEachRemaining(rows::add);

        assertThat(rows).hasSize(4);
        GenericMap result = (GenericMap) rows.get(0).getMap(0);
        assertMapBlob(result.get(null), blobAsDescriptor, "null-key".getBytes());
        assertThat(result.get(BinaryString.fromString("null-value"))).isNull();
        assertMapBlob(
                result.get(BinaryString.fromString("hello")), blobAsDescriptor, "world".getBytes());
        assertMapBlob(result.get(BinaryString.fromString("empty")), blobAsDescriptor, new byte[0]);
        assertThat(rows.get(1).isNullAt(0)).isTrue();
        assertThat(rows.get(2).getMap(0)).isSameAs(BlobMapPlaceholder.INSTANCE);
        assertThat(rows.get(3).getMap(0).size()).isZero();

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        context = new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), selection);
        rows.clear();
        readerFactory.createReader(context).forEachRemaining(rows::add);
        assertThat(rows).hasSize(1);
        assertThat(((GenericMap) rows.get(0).getMap(0)).contains(null)).isTrue();

        RowType projectedRowType = RowType.of(DataTypes.BIGINT(), rowType.getTypeAt(0));
        readerFactory = format.createReaderFactory(null, projectedRowType, null);
        context = new FormatReaderContext(fileIO, file, fileIO.getFileSize(file));
        rows.clear();
        readerFactory.createReader(context).forEachRemaining(rows::add);
        assertThat(rows.get(0).isNullAt(0)).isTrue();
        assertThat(rows.get(0).getMap(1)).isInstanceOf(GenericMap.class);
    }

    private static void assertMapBlob(
            Object value, boolean blobAsDescriptor, byte[] expectedBytes) {
        assertThat(value).isInstanceOf(blobAsDescriptor ? BlobRef.class : BlobData.class);
        assertThat(((Blob) value).toData()).isEqualTo(expectedBytes);
    }

    private static int keyIndexPosition(byte[] bytes, int payloadPosition, int payloadLength) {
        int keyIndexLength =
                readLittleEndianInt(bytes, payloadPosition + payloadLength - 2 * Integer.BYTES);
        return valueIndexPosition(bytes, payloadPosition, payloadLength) - keyIndexLength;
    }

    private static int valueIndexPosition(byte[] bytes, int payloadPosition, int payloadLength) {
        int valueIndexLength =
                readLittleEndianInt(bytes, payloadPosition + payloadLength - Integer.BYTES);
        return payloadPosition + payloadLength - 2 * Integer.BYTES - valueIndexLength;
    }

    private static int readLittleEndianInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff)
                | ((bytes[offset + 1] & 0xff) << 8)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 3] & 0xff) << 24);
    }

    private static void writeLittleEndianInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) value;
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 3] = (byte) (value >>> 24);
    }

    private static void addArrayBlobResult(
            InternalRow row, boolean blobAsDescriptor, List<Object> result) {
        InternalArray array = row.getArray(0);
        if (array == BlobArrayPlaceholder.INSTANCE) {
            result.add(BlobArrayPlaceholder.INSTANCE);
            return;
        }
        Object[] bytes = new Object[array.size()];
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                bytes[i] = null;
            } else {
                Blob blob = array.getBlob(i);
                if (blobAsDescriptor) {
                    assertThat(blob).isInstanceOf(BlobRef.class);
                } else {
                    assertThat(blob).isInstanceOf(BlobData.class);
                }
                bytes[i] = blob.toData();
            }
        }
        result.add(bytes);
    }

    private static class TrackingLocalFileIO extends LocalFileIO {

        private TrackingSeekableInputStream lastInputStream;

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            this.lastInputStream = new TrackingSeekableInputStream(super.newInputStream(path));
            return lastInputStream;
        }
    }

    private static class TrackingSeekableInputStream extends SeekableInputStream {

        private final SeekableInputStream delegate;
        private int closeCount;

        private TrackingSeekableInputStream(SeekableInputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seek(long desired) throws IOException {
            delegate.seek(desired);
        }

        @Override
        public long getPos() throws IOException {
            return delegate.getPos();
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            return delegate.read(bytes, offset, length);
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            delegate.close();
        }
    }

    private interface ArrayPayloadCorruptor {
        void corrupt(byte[] bytes, int position, int length);
    }

    private interface MapPayloadCorruptor {
        void corrupt(byte[] bytes, int position, int length);
    }
}
