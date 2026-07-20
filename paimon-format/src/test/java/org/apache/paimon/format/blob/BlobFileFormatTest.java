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

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericArray;
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
import java.util.List;
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
    public void testArrayBlobAsDescriptor() throws IOException {
        innerTestArray(true);
    }

    @Test
    public void testReadArrayBlobInlineBytes() throws IOException {
        innerTestArray(false);
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

    private interface ArrayPayloadCorruptor {
        void corrupt(byte[] bytes, int position, int length);
    }
}
