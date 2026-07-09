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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedArray;
import org.apache.paimon.utils.UriReader;
import org.apache.paimon.utils.UriReaderFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobFormatWriter}. */
public class BlobFormatWriterTest {

    @Test
    public void testTwoConsecutiveBlobsPreserveReadback(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        byte[] firstPayload = "first-blob".getBytes();
        byte[] secondPayload = "second-blob-payload".getBytes();

        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType);
        writer.addElement(GenericRow.of(Blob.fromData(firstPayload)));
        writer.addElement(GenericRow.of(Blob.fromData(secondPayload)));
        writer.close();

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(2);

            BlobFormatReader reader =
                    new BlobFormatReader(
                            fileIO, filePath, fileMeta, in, 1, 0, DataTypes.BLOB(), false);
            FileRecordIterator<InternalRow> iterator = reader.readBatch();
            assertBlobPayload(iterator.next().getBlob(0), firstPayload);
            assertBlobPayload(iterator.next().getBlob(0), secondPayload);
        }
    }

    @Test
    public void testWriteNullOnFetchFailureFallbackForHttpBadRequest(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        false,
                        true);

        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingHttpReader(400),
                                new BlobDescriptor("https://example.com/bad-request.jpg", 0, -1))));

        writer.close();

        assertThat(outputFile.toFile()).exists();
    }

    @Test
    public void testHttpRateLimitWritesNullWhenFetchFailureEnabled(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        false,
                        true);

        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingHttpReader(420),
                                new BlobDescriptor("https://example.com/rate-limit.jpg", 0, -1))));

        writer.close();

        assertThat(outputFile.toFile()).exists();
    }

    @Test
    public void testHttpRateLimitFailsWhenFetchFailureDisabled(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        false,
                        false);

        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new BlobRef(
                                                        failingHttpReader(420),
                                                        new BlobDescriptor(
                                                                "https://example.com/rate-limit.jpg",
                                                                0,
                                                                -1)))))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("HTTP error code: 420");
    }

    @Test
    public void testHttpNotFoundPropagatesWhenFetchFailureDisabled(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        false,
                        false);

        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new BlobRef(
                                                        failingHttpReader(404),
                                                        new BlobDescriptor(
                                                                "https://example.com/missing.jpg",
                                                                0,
                                                                -1)))))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("HTTP error code: 404");
    }

    @Test
    public void testWriteNullOnFetchFailureForInvalidUriDescriptor(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        UriReaderFactory uriReaderFactory =
                new UriReaderFactory(CatalogContext.create(new Options()));
        byte[] descriptorBytes =
                new BlobDescriptor("https://img.alicdn.com/imgextra/##1304008055350781673", 0, -1)
                        .serialize();

        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        false,
                        true);

        writer.addElement(new DescriptorBytesRow(descriptorBytes, uriReaderFactory));
        writer.close();

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(1);
            assertThat(fileMeta.isNull(0)).isTrue();
        }
    }

    @Test
    public void testArrayWriteNullOnFetchFailureForInvalidUriDescriptor(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        UriReaderFactory uriReaderFactory =
                new UriReaderFactory(CatalogContext.create(new Options()));
        byte[] descriptorBytes =
                new BlobDescriptor("https://img.alicdn.com/imgextra/##1304008055350781673", 0, -1)
                        .serialize();

        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        false,
                        true);

        writer.addElement(
                GenericRow.of(new DescriptorBytesArray(descriptorBytes, uriReaderFactory)));
        writer.close();

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(1);
            assertThat(fileMeta.isNull(0)).isFalse();

            BlobFormatReader reader =
                    new BlobFormatReader(
                            fileIO,
                            filePath,
                            fileMeta,
                            in,
                            1,
                            0,
                            DataTypes.ARRAY(DataTypes.BLOB()),
                            false);
            InternalArray array = reader.readBatch().next().getArray(0);
            assertThat(array.size()).isEqualTo(1);
            assertThat(array.isNullAt(0)).isTrue();
        }
    }

    @Test
    public void testHttpNotFoundWritesNullWhenMissingFileEnabled(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        true,
                        false);

        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingHttpReader(404),
                                new BlobDescriptor("https://example.com/missing.jpg", 0, -1))));
        writer.close();

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(1);
            assertThat(fileMeta.isNull(0)).isTrue();
        }
    }

    @Test
    public void testBlobFetchMetricReporterForSuccessAndNullWritten(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        false,
                        true,
                        metricReporter);

        writer.addElement(GenericRow.of(Blob.fromData("image".getBytes())));
        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingHttpReader(500),
                                new BlobDescriptor("https://example.com/error.jpg", 0, -1))));
        writer.close();

        assertThat(metricReporter.success).isEqualTo(1);
        assertThat(metricReporter.successBytes).isEqualTo(5);
        assertThat(metricReporter.fetchFailureNullWritten).isEqualTo(1);
        assertThat(metricReporter.failure).isEqualTo(0);
    }

    @Test
    public void testBlobFetchMetricReporterForUnhandledFailure(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        false,
                        false,
                        metricReporter);

        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new BlobRef(
                                                        failingHttpReader(500),
                                                        new BlobDescriptor(
                                                                "https://example.com/error.jpg",
                                                                0,
                                                                -1)))))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("HTTP error code: 500");
        assertThat(metricReporter.failure).isEqualTo(1);
        assertThat(metricReporter.fetchFailureNullWritten).isEqualTo(0);
    }

    @Test
    public void testBlobFetchMetricReporterForPreCheckedMissingFile(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        UriReaderFactory uriReaderFactory =
                new UriReaderFactory(CatalogContext.create(new Options()));
        byte[] descriptorBytes =
                new BlobDescriptor("https://example.com/missing.jpg", 0, -1).serialize();
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        true,
                        false,
                        metricReporter);

        writer.addElement(new DescriptorBytesRow(descriptorBytes, uriReaderFactory, true));
        writer.close();

        assertThat(metricReporter.missingFileNullWritten).isEqualTo(1);
        assertThat(metricReporter.httpNotFound).isEqualTo(1);
    }

    @Test
    public void testBlobFetchMetricReporterIgnoresUserNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        true,
                        false,
                        metricReporter);

        writer.addElement(GenericRow.of((Object) null));
        writer.close();

        assertThat(metricReporter.missingFileNullWritten).isEqualTo(0);
        assertThat(metricReporter.httpNotFound).isEqualTo(0);
    }

    private static void assertBlobPayload(Blob blob, byte[] expected) throws Exception {
        try (SeekableInputStream blobIn = blob.newInputStream()) {
            byte[] actual = new byte[expected.length];
            org.apache.paimon.utils.IOUtils.readFully(blobIn, actual);
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static UriReader failingHttpReader(int statusCode) {
        return new UriReader() {
            @Override
            public SeekableInputStream newInputStream(String uri) {
                throw new RuntimeException("HTTP error code: " + statusCode);
            }
        };
    }

    private static final class DescriptorBytesRow implements InternalRow {
        private final byte[] descriptorBytes;
        private final UriReaderFactory uriReaderFactory;
        private final boolean nullAt;

        private DescriptorBytesRow(byte[] descriptorBytes, UriReaderFactory uriReaderFactory) {
            this(descriptorBytes, uriReaderFactory, false);
        }

        private DescriptorBytesRow(
                byte[] descriptorBytes, UriReaderFactory uriReaderFactory, boolean nullAt) {
            this.descriptorBytes = descriptorBytes;
            this.uriReaderFactory = uriReaderFactory;
            this.nullAt = nullAt;
        }

        @Override
        public int getFieldCount() {
            return 1;
        }

        @Override
        public RowKind getRowKind() {
            return RowKind.INSERT;
        }

        @Override
        public void setRowKind(RowKind kind) {}

        @Override
        public boolean isNullAt(int pos) {
            return nullAt;
        }

        @Override
        public Blob getBlob(int pos) {
            return Blob.fromBytes(descriptorBytes, uriReaderFactory, null);
        }

        private UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(int pos) {
            throw unsupported();
        }

        @Override
        public byte getByte(int pos) {
            throw unsupported();
        }

        @Override
        public short getShort(int pos) {
            throw unsupported();
        }

        @Override
        public int getInt(int pos) {
            throw unsupported();
        }

        @Override
        public long getLong(int pos) {
            throw unsupported();
        }

        @Override
        public float getFloat(int pos) {
            throw unsupported();
        }

        @Override
        public double getDouble(int pos) {
            throw unsupported();
        }

        @Override
        public org.apache.paimon.data.BinaryString getString(int pos) {
            throw unsupported();
        }

        @Override
        public org.apache.paimon.data.Decimal getDecimal(int pos, int precision, int scale) {
            throw unsupported();
        }

        @Override
        public org.apache.paimon.data.Timestamp getTimestamp(int pos, int precision) {
            throw unsupported();
        }

        @Override
        public byte[] getBinary(int pos) {
            return descriptorBytes;
        }

        @Override
        public org.apache.paimon.data.variant.Variant getVariant(int pos) {
            throw unsupported();
        }

        @Override
        public org.apache.paimon.data.InternalArray getArray(int pos) {
            throw unsupported();
        }

        @Override
        public org.apache.paimon.data.InternalVector getVector(int pos) {
            throw unsupported();
        }

        @Override
        public org.apache.paimon.data.InternalMap getMap(int pos) {
            throw unsupported();
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            throw unsupported();
        }
    }

    private static final class TestingBlobFetchMetricReporter implements BlobFetchMetricReporter {

        private int success;
        private long successBytes;
        private int missingFileNullWritten;
        private int httpNotFound;
        private int fetchFailureNullWritten;
        private int failure;

        @Override
        public void recordSuccess(long bytes) {
            success++;
            successBytes += bytes;
        }

        @Override
        public void recordMissingFileNullWritten(boolean httpNotFound) {
            missingFileNullWritten++;
            if (httpNotFound) {
                this.httpNotFound++;
            }
        }

        @Override
        public void recordFetchFailureNullWritten(Throwable throwable) {
            fetchFailureNullWritten++;
        }

        @Override
        public void recordFetchFailure(Throwable throwable) {
            failure++;
        }
    }

    private static final class DescriptorBytesArray extends ProjectedArray {
        private final byte[] descriptorBytes;
        private final UriReaderFactory uriReaderFactory;

        private DescriptorBytesArray(byte[] descriptorBytes, UriReaderFactory uriReaderFactory) {
            super(new int[] {0});
            this.descriptorBytes = descriptorBytes;
            this.uriReaderFactory = uriReaderFactory;
        }

        @Override
        public boolean isNullAt(int pos) {
            return false;
        }

        @Override
        public Blob getBlob(int pos) {
            return Blob.fromBytes(descriptorBytes, uriReaderFactory, null);
        }
    }
}
