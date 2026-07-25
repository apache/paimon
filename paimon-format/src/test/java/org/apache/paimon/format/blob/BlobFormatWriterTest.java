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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobMapPlaceholder;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobFormatWriter}. */
public class BlobFormatWriterTest {

    @Test
    public void testRawBlobGoldenBytes(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path sourceFile = tempDir.resolve("source.bin");
        Files.write(sourceFile, "descriptor".getBytes());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            false,
                            BlobFetchMetricReporter.NOOP,
                            BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
            writer.addElement(GenericRow.of(Blob.fromData("inline".getBytes())));
            writer.addElement(GenericRow.of(Blob.fromLocal(sourceFile.toString())));
            writer.addElement(GenericRow.of((Object) null));
            writer.addElement(GenericRow.of(BlobPlaceholder.INSTANCE));
            writer.close();
        }

        assertThat(toHex(Files.readAllBytes(outputFile)))
                .isEqualTo(
                        "cf114e58696e6c696e6516000000000000002960c8e9"
                                + "cf114e5864657363726970746f721a000000000000003f69146b"
                                + "2c0835010400000001");
    }

    @Test
    public void testArrayBlobGoldenBytes(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path sourceFile = tempDir.resolve("source.bin");
        Files.write(sourceFile, "descriptor".getBytes());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.ARRAY(DataTypes.BLOB())),
                            false,
                            false,
                            BlobFetchMetricReporter.NOOP,
                            BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
            writer.addElement(GenericRow.of(new GenericArray(new Object[0])));
            writer.addElement(
                    GenericRow.of(
                            new GenericArray(
                                    new Object[] {
                                        Blob.fromData("inline".getBytes()),
                                        null,
                                        Blob.fromData(new byte[0]),
                                        Blob.fromLocal(sourceFile.toString())
                                    })));
            writer.addElement(GenericRow.of((Object) null));
            writer.addElement(GenericRow.of(BlobArrayPlaceholder.INSTANCE));
            writer.close();
        }

        assertThat(toHex(Files.readAllBytes(outputFile)))
                .isEqualTo(
                        "cf114e58424342410100000000000000001d000000000000009bd49157"
                                + "cf114e58424342410104000000696e6c696e6564657363726970746f72"
                                + "0c0d0214040000003100000000000000d08307713a2863010400000001");
    }

    @Test
    public void testMapBlobGoldenBytes(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path sourceFile = tempDir.resolve("source.bin");
        Files.write(sourceFile, "descriptor".getBytes());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(null, null);
        entries.put(BinaryString.fromString(""), Blob.fromData(new byte[0]));
        entries.put(BinaryString.fromString("inline"), Blob.fromData("data".getBytes()));
        entries.put(BinaryString.fromString("descriptor"), Blob.fromLocal(sourceFile.toString()));

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())),
                            false,
                            false,
                            BlobFetchMetricReporter.NOOP,
                            BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
            writer.addElement(GenericRow.of(new GenericMap(new LinkedHashMap<>())));
            writer.addElement(GenericRow.of(new GenericMap(entries)));
            writer.addElement(GenericRow.of((Object) null));
            writer.addElement(GenericRow.of(BlobMapPlaceholder.INSTANCE));
            writer.close();
        }

        assertThat(toHex(Files.readAllBytes(outputFile)))
                .isEqualTo(
                        "cf114e584243424d010000000000000000000000002100000000000000"
                                + "8360591ecf114e584243424d0104000000696e6c696e65646573637269"
                                + "70746f726461746164657363726970746f7201020c080102080c040000"
                                + "000400000047000000000000006c9f5981424c8f01010500000001");
    }

    @Test
    public void testTwoConsecutiveBlobsPreserveReadback(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        byte[] firstPayload = "first-blob".getBytes();
        byte[] secondPayload = "second-blob-payload".getBytes();

        BlobFormatWriter writer = newWriter(outputFile, rowType);
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
        BlobFormatWriter writer = newWriter(outputFile, rowType, false, true);

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
        BlobFormatWriter writer = newWriter(outputFile, rowType, false, true);

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
        BlobFormatWriter writer = newWriter(tempDir.resolve("blob.out"), rowType, false, false);

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
        BlobFormatWriter writer = newWriter(tempDir.resolve("blob.out"), rowType, false, false);

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

        BlobFormatWriter writer = newWriter(outputFile, rowType, false, true);

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

        BlobFormatWriter writer = newWriter(outputFile, rowType, false, true);

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
        BlobFormatWriter writer = newWriter(outputFile, rowType, true, false);

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
                newWriter(tempDir.resolve("blob.out"), rowType, false, true, metricReporter);

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
                newWriter(tempDir.resolve("blob.out"), rowType, false, false, metricReporter);

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
                newWriter(tempDir.resolve("blob.out"), rowType, true, false, metricReporter);

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
                newWriter(tempDir.resolve("blob.out"), rowType, true, false, metricReporter);

        writer.addElement(GenericRow.of((Object) null));
        writer.close();

        assertThat(metricReporter.missingFileNullWritten).isEqualTo(0);
        assertThat(metricReporter.httpNotFound).isEqualTo(0);
    }

    @Test
    public void testArrayBlobFetchMetricReporter(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                newWriter(tempDir.resolve("blob.out"), rowType, true, true, metricReporter);

        writer.addElement(
                GenericRow.of(
                        new GenericArray(
                                new Object[] {
                                    Blob.fromData("image".getBytes()),
                                    new BlobRef(
                                            failingHttpReader(500),
                                            new BlobDescriptor(
                                                    "https://example.com/error.jpg", 0, -1)),
                                    new BlobRef(
                                            failingHttpReader(404),
                                            new BlobDescriptor(
                                                    "https://example.com/missing.jpg", 0, -1)),
                                    null
                                })));
        writer.addElement(GenericRow.of((Object) null));
        writer.close();

        assertThat(metricReporter.success).isEqualTo(1);
        assertThat(metricReporter.successBytes).isEqualTo(5);
        assertThat(metricReporter.missingFileNullWritten).isEqualTo(1);
        assertThat(metricReporter.httpNotFound).isEqualTo(1);
        assertThat(metricReporter.fetchFailureNullWritten).isEqualTo(1);
        assertThat(metricReporter.failure).isEqualTo(0);
    }

    @Test
    public void testArrayBlobFetchMetricReporterForUnhandledFailure(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.ARRAY(DataTypes.BLOB()));
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                newWriter(tempDir.resolve("blob.out"), rowType, false, false, metricReporter);

        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new GenericArray(
                                                        new Object[] {
                                                            new BlobRef(
                                                                    failingHttpReader(500),
                                                                    new BlobDescriptor(
                                                                            "https://example.com/error.jpg",
                                                                            0,
                                                                            -1))
                                                        }))))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("HTTP error code: 500");
        assertThat(metricReporter.failure).isEqualTo(1);
        assertThat(metricReporter.fetchFailureNullWritten).isEqualTo(0);
    }

    @Test
    public void testCopyBufferSizeIsRespectedForBlobRef(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(20);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()), 8);
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 20))));
        writer.close();

        // With an 8-byte copy buffer, no single read request exceeds 8 bytes.
        assertThat(reader.opened).hasSize(1);
        assertThat(reader.opened.get(0).maxReadRequest).isEqualTo(8);
        assertThat(readBackBlobs(outputFile, 1)).containsExactly(source);
    }

    @Test
    public void testDefaultCopyBufferSize(@TempDir java.nio.file.Path tempDir) throws Exception {
        // The configured default preserves the historical 4 KiB copy buffer.
        assertThat(BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE).isEqualTo(4 * 1024);

        String uri = "mem://file";
        byte[] source = sequentialBytes(5000);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 5000))));
        writer.close();

        assertThat(reader.opened.get(0).maxReadRequest).isEqualTo(4 * 1024);
        assertThat(readBackBlobs(outputFile, 1)).containsExactly(source);
    }

    private static BlobFormatWriter newWriter(java.nio.file.Path outputFile, RowType rowType)
            throws java.io.FileNotFoundException {
        return newWriter(
                outputFile,
                rowType,
                false,
                false,
                BlobFetchMetricReporter.NOOP,
                BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
    }

    private static BlobFormatWriter newWriter(
            java.nio.file.Path outputFile,
            RowType rowType,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure)
            throws java.io.FileNotFoundException {
        return newWriter(
                outputFile,
                rowType,
                writeNullOnMissingFile,
                writeNullOnFetchFailure,
                BlobFetchMetricReporter.NOOP,
                BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
    }

    private static BlobFormatWriter newWriter(
            java.nio.file.Path outputFile,
            RowType rowType,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter)
            throws java.io.FileNotFoundException {
        return newWriter(
                outputFile,
                rowType,
                writeNullOnMissingFile,
                writeNullOnFetchFailure,
                blobFetchMetricReporter,
                BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
    }

    private static BlobFormatWriter newWriter(
            java.nio.file.Path outputFile, RowType rowType, int copyBufferSize)
            throws java.io.FileNotFoundException {
        return newWriter(
                outputFile, rowType, false, false, BlobFetchMetricReporter.NOOP, copyBufferSize);
    }

    private static BlobFormatWriter newWriter(
            java.nio.file.Path outputFile,
            RowType rowType,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize)
            throws java.io.FileNotFoundException {
        return new BlobFormatWriter(
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                null,
                rowType,
                writeNullOnMissingFile,
                writeNullOnFetchFailure,
                blobFetchMetricReporter,
                copyBufferSize);
    }

    private static List<byte[]> readBackBlobs(java.nio.file.Path outputFile, int expectedCount)
            throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        List<byte[]> result = new ArrayList<>();
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(expectedCount);
            BlobFormatReader reader =
                    new BlobFormatReader(
                            fileIO, filePath, fileMeta, in, 1, 0, DataTypes.BLOB(), false);
            FileRecordIterator<InternalRow> iterator = reader.readBatch();
            for (int i = 0; i < expectedCount; i++) {
                InternalRow row = iterator.next();
                assertThat(row).isNotNull();
                result.add(readAll(row.getBlob(0)));
            }
        }
        return result;
    }

    private static byte[] readAll(Blob blob) throws Exception {
        try (SeekableInputStream in = blob.newInputStream()) {
            return org.apache.paimon.utils.IOUtils.readFully(in, false);
        }
    }

    private static byte[] sequentialBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    private static Map<String, byte[]> singleFile(String uri, byte[] data) {
        Map<String, byte[]> files = new LinkedHashMap<>();
        files.put(uri, data);
        return files;
    }

    /** A {@link UriReader} over in-memory files that records opened streams. */
    private static final class RecordingUriReader implements UriReader {

        private final Map<String, byte[]> files;
        private final List<CountingSeekableInputStream> opened = new ArrayList<>();

        private RecordingUriReader(Map<String, byte[]> files) {
            this.files = files;
        }

        @Override
        public SeekableInputStream newInputStream(String uri) {
            byte[] data = files.get(uri);
            if (data == null) {
                throw new IllegalArgumentException("Unknown uri: " + uri);
            }
            CountingSeekableInputStream stream = new CountingSeekableInputStream(data);
            opened.add(stream);
            return stream;
        }
    }

    /** A seekable stream over a byte array that records close count and max read request size. */
    private static final class CountingSeekableInputStream extends SeekableInputStream {

        private final byte[] data;
        private int pos;
        private int maxReadRequest;

        private CountingSeekableInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public void seek(long desired) {
            this.pos = (int) desired;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() {
            maxReadRequest = Math.max(maxReadRequest, 1);
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            maxReadRequest = Math.max(maxReadRequest, len);
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void close() {}
    }

    @Test
    public void testMapBlobFetchMetricReporter(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB()));
        TestingBlobFetchMetricReporter metricReporter = new TestingBlobFetchMetricReporter();
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        true,
                        true,
                        metricReporter,
                        BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);

        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(1, Blob.fromData("image".getBytes()));
        entries.put(
                2,
                new BlobRef(
                        failingHttpReader(500),
                        new BlobDescriptor("https://example.com/error.jpg", 0, -1)));
        entries.put(
                3,
                new BlobRef(
                        failingHttpReader(404),
                        new BlobDescriptor("https://example.com/missing.jpg", 0, -1)));
        entries.put(4, null);
        writer.addElement(GenericRow.of(new GenericMap(entries)));
        writer.addElement(GenericRow.of((Object) null));
        writer.close();

        assertThat(metricReporter.success).isEqualTo(1);
        assertThat(metricReporter.successBytes).isEqualTo(5);
        assertThat(metricReporter.missingFileNullWritten).isEqualTo(1);
        assertThat(metricReporter.httpNotFound).isEqualTo(1);
        assertThat(metricReporter.fetchFailureNullWritten).isEqualTo(1);
        assertThat(metricReporter.failure).isEqualTo(0);
    }

    @Test
    public void testMapBlobConsumerDescriptorsAndFlush(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        Path blobFile = new Path(outputFile.toUri());
        List<BlobDescriptor> descriptors = new ArrayList<>();

        try (CountingPositionOutputStream out =
                new CountingPositionOutputStream(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()))) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            (fieldName, descriptor) -> {
                                assertThat(fieldName).isEqualTo("f0");
                                descriptors.add(descriptor);
                                return descriptors.size() == 1;
                            },
                            RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())),
                            false,
                            false,
                            BlobFetchMetricReporter.NOOP,
                            BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE);
            writer.setFile(blobFile);
            Map<Object, Object> entries = new LinkedHashMap<>();
            entries.put(BinaryString.fromString("a"), Blob.fromData("x".getBytes()));
            entries.put(BinaryString.fromString("b"), Blob.fromData("yz".getBytes()));
            writer.addElement(GenericRow.of(new GenericMap(entries)));
            writer.close();

            assertThat(out.flushCount).isEqualTo(1);
        }

        assertThat(descriptors).hasSize(2);
        assertThat(descriptors.get(0)).isEqualTo(new BlobDescriptor(blobFile.toString(), 15, 1));
        assertThat(descriptors.get(1)).isEqualTo(new BlobDescriptor(blobFile.toString(), 16, 2));
    }

    private static void assertBlobPayload(Blob blob, byte[] expected) throws Exception {
        try (SeekableInputStream blobIn = blob.newInputStream()) {
            byte[] actual = new byte[expected.length];
            org.apache.paimon.utils.IOUtils.readFully(blobIn, actual);
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
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

    private static final class CountingPositionOutputStream extends PositionOutputStreamWrapper {

        private int flushCount;

        private CountingPositionOutputStream(PositionOutputStream out) {
            super(out);
        }

        @Override
        public void flush() throws java.io.IOException {
            flushCount++;
            super.flush();
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
