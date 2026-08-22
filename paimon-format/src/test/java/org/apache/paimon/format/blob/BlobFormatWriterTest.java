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
import org.apache.paimon.data.InternalMap;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

    @Test
    public void testMidBodyFailureWritesOnlyMiddleScalarAsNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        byte[] first = "first-image".getBytes();
        byte[] broken = "broken-image-body".getBytes();
        byte[] last = "last-image".getBytes();
        MidBodyFailingInputStream failingStream = new MidBodyFailingInputStream(broken, 5, false);
        UriReader failingReader = ignored -> failingStream;
        TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                newWriter(outputFile, RowType.of(DataTypes.BLOB()), false, true, metrics, 3);

        writer.addElement(GenericRow.of(Blob.fromData(first)));
        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingReader,
                                new BlobDescriptor("mem://broken", 0, broken.length))));
        writer.addElement(GenericRow.of(Blob.fromData(last)));
        writer.close();

        assertThat(readBackNullableBlobs(outputFile, 3)).containsExactly(first, null, last);
        assertThat(failingStream.closeCount).isEqualTo(1);
        assertThat(metrics.success).isEqualTo(2);
        assertThat(metrics.successBytes).isEqualTo(first.length + last.length);
        assertThat(metrics.fetchFailureNullWritten).isEqualTo(1);
        assertThat(metrics.failure).isEqualTo(0);
    }

    @Test
    public void testMidBodyFailureStillThrowsWhenFallbackDisabled(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] broken = "broken-image-body".getBytes();
        UriReader failingReader = ignored -> new MidBodyFailingInputStream(broken, 5, false);
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
                            3);

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    new BlobRef(
                                                            failingReader,
                                                            new BlobDescriptor(
                                                                    "mem://broken",
                                                                    0,
                                                                    broken.length)))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("body read failed");
        }

        // The disabled path retains the historical direct-streaming behavior. This partial output
        // is why the enabled path must stage a complete payload before appending it.
        assertThat(Files.size(outputFile))
                .isGreaterThan(BlobFormatWriter.MAGIC_NUMBER_BYTES.length);
    }

    @Test
    public void testKnownLengthPrematureEofWritesNullAndContinues(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] shortBody = sequentialBytes(5);
        RecordingUriReader reader = new RecordingUriReader(singleFile("mem://short", shortBody));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()), false, true);

        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                reader,
                                new BlobDescriptor("mem://short", 0, shortBody.length + 7))));
        writer.addElement(GenericRow.of(Blob.fromData("after-eof".getBytes())));
        writer.close();

        assertThat(readBackNullableBlobs(outputFile, 2))
                .containsExactly(null, "after-eof".getBytes());
        assertThat(reader.opened.get(0).closeCount).isEqualTo(1);
    }

    @Test
    public void testUnknownLengthBodyReadFailureWritesNullAndContinues(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] broken = sequentialBytes(16);
        MidBodyFailingInputStream failingStream = new MidBodyFailingInputStream(broken, 7, false);
        UriReader reader = ignored -> failingStream;
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                newWriter(
                        outputFile,
                        RowType.of(DataTypes.BLOB()),
                        false,
                        true,
                        BlobFetchMetricReporter.NOOP,
                        4);

        writer.addElement(
                GenericRow.of(new BlobRef(reader, new BlobDescriptor("mem://unknown", 0, -1))));
        writer.addElement(GenericRow.of(Blob.fromData("after-failure".getBytes())));
        writer.close();

        assertThat(readBackNullableBlobs(outputFile, 2))
                .containsExactly(null, "after-failure".getBytes());
        assertThat(failingStream.closeCount).isEqualTo(1);
    }

    @Test
    public void testArrayMiddleBodyFailureWritesOnlyThatElementAsNull(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] first = "array-first".getBytes();
        byte[] broken = "array-broken-body".getBytes();
        byte[] last = "array-last".getBytes();
        UriReader failingReader = ignored -> new MidBodyFailingInputStream(broken, 6, false);
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                newWriter(
                        outputFile,
                        RowType.of(DataTypes.ARRAY(DataTypes.BLOB())),
                        false,
                        true,
                        BlobFetchMetricReporter.NOOP,
                        4);

        writer.addElement(
                GenericRow.of(
                        new GenericArray(
                                new Object[] {
                                    Blob.fromData(first),
                                    new BlobRef(
                                            failingReader,
                                            new BlobDescriptor(
                                                    "mem://array-broken", 0, broken.length)),
                                    Blob.fromData(last)
                                })));
        writer.close();

        InternalArray array =
                readSingleRow(outputFile, DataTypes.ARRAY(DataTypes.BLOB())).getArray(0);
        assertThat(array.size()).isEqualTo(3);
        assertThat(readAll(array.getBlob(0))).isEqualTo(first);
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(readAll(array.getBlob(2))).isEqualTo(last);
    }

    @Test
    public void testMapMiddleBodyFailureWritesOnlyThatValueAsNull(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] first = "map-first".getBytes();
        byte[] broken = "map-broken-body".getBytes();
        byte[] last = "map-last".getBytes();
        UriReader failingReader = ignored -> new MidBodyFailingInputStream(broken, 6, false);
        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(1, Blob.fromData(first));
        entries.put(
                2,
                new BlobRef(
                        failingReader, new BlobDescriptor("mem://map-broken", 0, broken.length)));
        entries.put(3, Blob.fromData(last));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                newWriter(
                        outputFile,
                        RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                        false,
                        true,
                        BlobFetchMetricReporter.NOOP,
                        4);

        writer.addElement(GenericRow.of(new GenericMap(entries)));
        writer.close();

        InternalMap map =
                readSingleRow(outputFile, DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB()))
                        .getMap(0);
        assertThat(map.size()).isEqualTo(3);
        assertThat(map.keyArray().getInt(0)).isEqualTo(1);
        assertThat(map.keyArray().getInt(1)).isEqualTo(2);
        assertThat(map.keyArray().getInt(2)).isEqualTo(3);
        assertThat(readAll(map.valueArray().getBlob(0))).isEqualTo(first);
        assertThat(map.valueArray().isNullAt(1)).isTrue();
        assertThat(readAll(map.valueArray().getBlob(2))).isEqualTo(last);
    }

    @Test
    public void testFailedReusableSourceIsDiscardedBeforeNextBlob(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] payload = sequentialBytes(12);
        int[] openCount = {0};
        MidBodyFailingInputStream firstStream = new MidBodyFailingInputStream(payload, 5, false);
        UriReader reader =
                ignored -> {
                    openCount[0]++;
                    return openCount[0] == 1
                            ? firstStream
                            : new CountingSeekableInputStream(payload);
                };
        BlobDescriptor descriptor = new BlobDescriptor("mem://reused", 0, payload.length);
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                newWriter(
                        outputFile,
                        RowType.of(DataTypes.BLOB()),
                        false,
                        true,
                        BlobFetchMetricReporter.NOOP,
                        3);

        writer.addElement(GenericRow.of(new BlobRef(reader, descriptor)));
        writer.addElement(GenericRow.of(new BlobRef(reader, descriptor)));
        writer.close();

        assertThat(openCount[0]).isEqualTo(2);
        assertThat(firstStream.closeCount).isEqualTo(1);
        assertThat(readBackNullableBlobs(outputFile, 2)).containsExactly(null, payload);
    }

    @Test
    public void testSourceCloseFailureIsNotConvertedToNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        UriReader closeFailing = ignored -> new CloseFailingStream(new byte[] {1, 2, 3});
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            BlobFetchMetricReporter.NOOP,
                            2);

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    new BlobRef(
                                                            closeFailing,
                                                            new BlobDescriptor(
                                                                    "mem://close-failing",
                                                                    0,
                                                                    -1)))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("close failed");
            assertThat(out.getPos()).isZero();
        }
    }

    @Test
    public void testInterruptedSourceReadRestoresClearedInterruptAndIsNotConvertedToNull()
            throws Exception {
        Thread.interrupted();
        try {
            TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
            UriReader interruptedReader =
                    ignored ->
                            new ReadFailingInputStream(
                                    new InterruptedIOException("source read interrupted"), false);
            try (PositionOutputStream out = new InMemoryPositionOutputStream()) {
                BlobFormatWriter writer =
                        new BlobFormatWriter(
                                out, null, RowType.of(DataTypes.BLOB()), false, true, metrics, 2);

                assertThatThrownBy(
                                () ->
                                        writer.addElement(
                                                GenericRow.of(
                                                        new BlobRef(
                                                                interruptedReader,
                                                                new BlobDescriptor(
                                                                        "mem://interrupted",
                                                                        0,
                                                                        -1)))))
                        .isInstanceOf(InterruptedIOException.class)
                        .hasMessageContaining("source read interrupted");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(out.getPos()).isZero();
            }

            assertThat(metrics.fetchFailureNullWritten).isEqualTo(0);
            assertThat(metrics.failure).isEqualTo(1);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testInterruptedSourceReadPreservesInterruptWhenSourceCloseFails() throws Exception {
        Thread.interrupted();
        try {
            TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
            UriReader interruptedReader =
                    ignored ->
                            new ReadFailingInputStream(
                                    new InterruptedIOException("source read interrupted"),
                                    false,
                                    true);
            try (PositionOutputStream out = new InMemoryPositionOutputStream()) {
                BlobFormatWriter writer =
                        new BlobFormatWriter(
                                out, null, RowType.of(DataTypes.BLOB()), false, true, metrics, 2);

                assertThatThrownBy(
                                () ->
                                        writer.addElement(
                                                GenericRow.of(
                                                        new BlobRef(
                                                                interruptedReader,
                                                                new BlobDescriptor(
                                                                        "mem://interrupted-close-failing",
                                                                        0,
                                                                        -1)))))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("source close failed")
                        .satisfies(
                                failure ->
                                        assertThat(failure.getSuppressed())
                                                .extracting(Throwable::getMessage)
                                                .contains("source read interrupted"));
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(out.getPos()).isZero();
            }

            assertThat(metrics.fetchFailureNullWritten).isEqualTo(0);
            assertThat(metrics.failure).isEqualTo(0);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testLargeInlineBlobBypassesStaging(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        byte[] payload = sequentialBytes(BlobStagingFactory.DEFAULT_MEMORY_THRESHOLD + 1);
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            BlobFetchMetricReporter.NOOP,
                            BlobFormatWriter.DEFAULT_COPY_BUFFER_SIZE,
                            () -> {
                                throw new AssertionError("Inline BlobData must not be staged.");
                            });
            writer.addElement(GenericRow.of(Blob.fromData(payload)));
            writer.close();
        }

        assertThat(readBackNullableBlobs(outputFile, 1)).containsExactly(payload);
    }

    @Test
    public void testSocketTimeoutWithoutInterruptWritesNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
        UriReader timeoutReader =
                ignored ->
                        new ReadFailingInputStream(
                                new SocketTimeoutException("socket read timed out"), false);
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                newWriter(outputFile, RowType.of(DataTypes.BLOB()), false, true, metrics, 2);

        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                timeoutReader, new BlobDescriptor("mem://socket-timeout", 0, -1))));
        writer.close();

        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        assertThat(readBackNullableBlobs(outputFile, 1)).containsExactly((byte[]) null);
        assertThat(metrics.fetchFailureNullWritten).isEqualTo(1);
        assertThat(metrics.failure).isEqualTo(0);
    }

    @Test
    public void testReadAndSourceCloseFailureIsNotConvertedToNull(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] body = sequentialBytes(12);
        UriReader reader = ignored -> new MidBodyFailingInputStream(body, 5, true);
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            BlobFetchMetricReporter.NOOP,
                            3);

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    new BlobRef(
                                                            reader,
                                                            new BlobDescriptor(
                                                                    "mem://read-close-failing",
                                                                    0,
                                                                    -1)))))
                    .isInstanceOf(IOException.class)
                    .satisfies(
                            failure -> {
                                assertThat(failure).hasMessageContaining("source close failed");
                                assertThat(failure.getSuppressed())
                                        .extracting(Throwable::getMessage)
                                        .contains("body read failed after 5 bytes");
                            });
            assertThat(out.getPos()).isZero();
        }
    }

    @Test
    public void testFinalOutputFailureIsNotConvertedToNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (FailingPositionOutputStream out =
                new FailingPositionOutputStream(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()), 5)) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out, null, RowType.of(DataTypes.BLOB()), false, true, metrics, 2);

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    Blob.fromData(
                                                            "successfully-staged".getBytes()))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("final output failed");
        }

        assertThat(metrics.fetchFailureNullWritten).isEqualTo(0);
        assertThat(metrics.failure).isEqualTo(0);
        assertThat(metrics.success).isEqualTo(0);
    }

    @Test
    public void testSpillableStagingUsesMemoryAndDiskAndCleansUp(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path stagingDirectory = Files.createDirectory(tempDir.resolve("staging"));
        byte[] inMemoryPayload = sequentialBytes(4);
        try (SpillableBlobStaging staging =
                new SpillableBlobStaging(8, stagingDirectory.toFile())) {
            staging.write(inMemoryPayload, 0, inMemoryPayload.length);
            staging.finish();

            assertThat(staging.isInMemory()).isTrue();
            assertThat(staging.spillFile()).isNull();
            try (InputStream in = staging.openInputStream()) {
                assertThat(org.apache.paimon.utils.IOUtils.readFully(in, false))
                        .isEqualTo(inMemoryPayload);
            }
        }
        assertDirectoryEmpty(stagingDirectory);

        byte[] spilledPayload = sequentialBytes(16);
        File spillFile;
        try (SpillableBlobStaging staging =
                new SpillableBlobStaging(4, stagingDirectory.toFile())) {
            staging.write(spilledPayload, 0, spilledPayload.length);
            staging.finish();

            assertThat(staging.isInMemory()).isFalse();
            spillFile = staging.spillFile();
            assertThat(spillFile).isNotNull().exists();
            try (InputStream in = staging.openInputStream()) {
                assertThat(org.apache.paimon.utils.IOUtils.readFully(in, false))
                        .isEqualTo(spilledPayload);
            }
        }
        assertThat(spillFile).doesNotExist();
        assertDirectoryEmpty(stagingDirectory);

        SpillableBlobStaging aborted = new SpillableBlobStaging(4, stagingDirectory.toFile());
        aborted.write(spilledPayload, 0, spilledPayload.length);
        File abortedSpillFile = aborted.spillFile();
        assertThat(abortedSpillFile).isNotNull().exists();
        aborted.close();
        assertThat(abortedSpillFile).doesNotExist();
        assertDirectoryEmpty(stagingDirectory);
    }

    @Test
    public void testWriterDeletesSpillFilesAfterSuccessFailureAndClose(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path stagingDirectory = Files.createDirectory(tempDir.resolve("staging"));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        byte[] success = sequentialBytes(16);
        byte[] broken = sequentialBytes(16);
        UriReader failingReader = ignored -> new MidBodyFailingInputStream(broken, 7, false);

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            BlobFetchMetricReporter.NOOP,
                            3,
                            BlobStagingFactory.spillable(4, stagingDirectory.toFile()));

            writer.addElement(GenericRow.of(blobRefFromBytes("mem://successful-spill", success)));
            assertDirectoryEmpty(stagingDirectory);

            writer.addElement(
                    GenericRow.of(
                            new BlobRef(
                                    failingReader,
                                    new BlobDescriptor("mem://broken-spill", 0, broken.length))));
            assertDirectoryEmpty(stagingDirectory);

            writer.close();
            assertDirectoryEmpty(stagingDirectory);
        }

        assertThat(readBackNullableBlobs(outputFile, 2)).containsExactly(success, null);
    }

    @Test
    public void testStagingLocalWriteFailureIsFatal(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            metrics,
                            2,
                            () -> new TestingBlobStaging(StagingFailure.WRITE));

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    blobRefFromBytes(
                                                            "mem://staging-write-failure",
                                                            "payload".getBytes()))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("staging write failed");
            assertThat(out.getPos()).isZero();
        }

        assertThat(metrics.fetchFailureNullWritten).isEqualTo(0);
        assertThat(metrics.failure).isEqualTo(0);
    }

    @Test
    public void testStagingFactoryFailureClosesSourceAndIsFatal(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        byte[] payload = "payload".getBytes();
        CountingSeekableInputStream source = new CountingSeekableInputStream(payload);
        UriReader reader = ignored -> source;
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            BlobFetchMetricReporter.NOOP,
                            2,
                            () -> {
                                throw new IOException("staging create failed");
                            });

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    new BlobRef(
                                                            reader,
                                                            new BlobDescriptor(
                                                                    "mem://create-failing",
                                                                    0,
                                                                    payload.length)))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("staging create failed");
            assertThat(out.getPos()).isZero();
        }

        assertThat(source.closeCount).isEqualTo(1);
    }

    @Test
    public void testStagingCleanupFailureAfterReadFailureIsFatal(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        byte[] broken = sequentialBytes(12);
        UriReader reader = ignored -> new MidBodyFailingInputStream(broken, 5, false);
        TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            metrics,
                            3,
                            () -> new TestingBlobStaging(StagingFailure.CLOSE));

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    new BlobRef(
                                                            reader,
                                                            new BlobDescriptor(
                                                                    "mem://broken-cleanup",
                                                                    0,
                                                                    -1)))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("staging close failed")
                    .satisfies(
                            failure ->
                                    assertThat(failure.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .contains("body read failed after 5 bytes"));
            assertThat(out.getPos()).isZero();
        }

        assertThat(metrics.fetchFailureNullWritten).isEqualTo(0);
        assertThat(metrics.failure).isEqualTo(0);
    }

    @Test
    public void testStagingCleanupFailureAfterFinalCopyIsFatal(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        TestingBlobFetchMetricReporter metrics = new TestingBlobFetchMetricReporter();
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        try (PositionOutputStream out =
                new LocalFileIO.LocalPositionOutputStream(outputFile.toFile())) {
            BlobFormatWriter writer =
                    new BlobFormatWriter(
                            out,
                            null,
                            RowType.of(DataTypes.BLOB()),
                            false,
                            true,
                            metrics,
                            2,
                            () -> new TestingBlobStaging(StagingFailure.CLOSE));

            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(
                                                    blobRefFromBytes(
                                                            "mem://staging-cleanup-failure",
                                                            "payload".getBytes()))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("staging close failed");
            assertThat(out.getPos()).isGreaterThan(0);
        }

        assertThat(metrics.fetchFailureNullWritten).isEqualTo(0);
        assertThat(metrics.failure).isEqualTo(0);
        assertThat(metrics.success).isEqualTo(0);
    }

    private static BlobRef blobRefFromBytes(String uri, byte[] bytes) {
        return new BlobRef(
                ignored -> new CountingSeekableInputStream(bytes), new BlobDescriptor(uri, 0, -1));
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

    private static List<byte[]> readBackNullableBlobs(
            java.nio.file.Path outputFile, int expectedCount) throws Exception {
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
                result.add(row.isNullAt(0) ? null : readAll(row.getBlob(0)));
            }
        }
        return result;
    }

    private static InternalRow readSingleRow(
            java.nio.file.Path outputFile, org.apache.paimon.types.DataType dataType)
            throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(1);
            BlobFormatReader reader =
                    new BlobFormatReader(fileIO, filePath, fileMeta, in, 1, 0, dataType, false);
            return reader.readBatch().next();
        }
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

    private static void assertDirectoryEmpty(java.nio.file.Path directory) {
        assertThat(directory.toFile().list()).isNotNull().isEmpty();
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
        private int openCount;

        private RecordingUriReader(Map<String, byte[]> files) {
            this.files = files;
        }

        @Override
        public SeekableInputStream newInputStream(String uri) {
            byte[] data = files.get(uri);
            if (data == null) {
                throw new IllegalArgumentException("Unknown uri: " + uri);
            }
            openCount++;
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
        private int closeCount;

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
        public void close() {
            closeCount++;
        }
    }

    /** A source which returns a prefix and then fails while its body is being read. */
    private static final class MidBodyFailingInputStream extends SeekableInputStream {

        private final byte[] data;
        private final int failAfter;
        private final boolean failOnClose;
        private int pos;
        private int closeCount;

        private MidBodyFailingInputStream(byte[] data, int failAfter, boolean failOnClose) {
            this.data = data;
            this.failAfter = failAfter;
            this.failOnClose = failOnClose;
        }

        @Override
        public void seek(long desired) {
            pos = (int) desired;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() throws IOException {
            if (pos >= failAfter) {
                throw new IOException("body read failed after " + failAfter + " bytes");
            }
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (pos >= failAfter) {
                throw new IOException("body read failed after " + failAfter + " bytes");
            }
            if (pos >= data.length) {
                return -1;
            }
            int bytesBeforeFailure = failAfter - pos;
            int bytesRead = Math.min(len, Math.min(data.length - pos, bytesBeforeFailure));
            System.arraycopy(data, pos, b, off, bytesRead);
            pos += bytesRead;
            return bytesRead;
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            if (failOnClose) {
                throw new IOException("source close failed");
            }
        }
    }

    /** A source which fails its body read, optionally marking the current thread interrupted. */
    private static final class ReadFailingInputStream extends SeekableInputStream {

        private final IOException failure;
        private final boolean interruptThread;
        private final boolean failOnClose;

        private ReadFailingInputStream(IOException failure, boolean interruptThread) {
            this(failure, interruptThread, false);
        }

        private ReadFailingInputStream(
                IOException failure, boolean interruptThread, boolean failOnClose) {
            this.failure = failure;
            this.interruptThread = interruptThread;
            this.failOnClose = failOnClose;
        }

        @Override
        public void seek(long desired) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read() throws IOException {
            return fail();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return fail();
        }

        private int fail() throws IOException {
            if (interruptThread) {
                Thread.currentThread().interrupt();
            }
            throw failure;
        }

        @Override
        public void close() throws IOException {
            if (failOnClose) {
                throw new IOException("source close failed");
            }
        }
    }

    /** A final sink whose position remains observable while the test thread is interrupted. */
    private static final class InMemoryPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream data = new ByteArrayOutputStream();

        @Override
        public long getPos() {
            return data.size();
        }

        @Override
        public void write(int b) {
            data.write(b);
        }

        @Override
        public void write(byte[] b) {
            data.write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            data.write(b, off, len);
        }

        @Override
        public void flush() {}

        @Override
        public void close() throws IOException {
            data.close();
        }
    }

    /** A final sink which fails once the configured number of bytes has been appended. */
    private static final class FailingPositionOutputStream extends PositionOutputStreamWrapper {

        private final long failAfter;

        private FailingPositionOutputStream(PositionOutputStream out, long failAfter) {
            super(out);
            this.failAfter = failAfter;
        }

        @Override
        public void write(int b) throws IOException {
            if (getPos() >= failAfter) {
                throw new IOException("final output failed");
            }
            super.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (getPos() + len > failAfter) {
                throw new IOException("final output failed");
            }
            super.write(b, off, len);
        }
    }

    private enum StagingFailure {
        WRITE,
        CLOSE
    }

    /** An injectable staging area used to prove local staging failures remain fatal. */
    private static final class TestingBlobStaging implements BlobStaging {

        private final StagingFailure failure;
        private final ByteArrayOutputStream data = new ByteArrayOutputStream();

        private TestingBlobStaging(StagingFailure failure) {
            this.failure = failure;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (failure == StagingFailure.WRITE) {
                throw new IOException("staging write failed");
            }
            data.write(bytes, offset, length);
        }

        @Override
        public void finish() {}

        @Override
        public InputStream openInputStream() {
            return new ByteArrayInputStream(data.toByteArray());
        }

        @Override
        public long length() {
            return data.size();
        }

        @Override
        public void close() throws IOException {
            if (failure == StagingFailure.CLOSE) {
                throw new IOException("staging close failed");
            }
        }
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

    @Test
    public void testArrayBlobRefsReuseSource(@TempDir java.nio.file.Path tempDir) throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(30);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer =
                newWriter(outputFile, RowType.of(DataTypes.ARRAY(DataTypes.BLOB())));
        writer.addElement(
                GenericRow.of(
                        new GenericArray(
                                new Object[] {
                                    new BlobRef(reader, new BlobDescriptor(uri, 0, 10)),
                                    new BlobRef(reader, new BlobDescriptor(uri, 10, 10)),
                                    new BlobRef(reader, new BlobDescriptor(uri, 20, 10))
                                })));
        writer.close();

        assertThat(reader.openCount).isEqualTo(1);

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(1);
            BlobFormatReader arrayReader =
                    new BlobFormatReader(
                            fileIO,
                            filePath,
                            fileMeta,
                            in,
                            1,
                            0,
                            DataTypes.ARRAY(DataTypes.BLOB()),
                            false);
            InternalArray array = arrayReader.readBatch().next().getArray(0);
            assertThat(array.size()).isEqualTo(3);
            assertThat(readAll(array.getBlob(0))).isEqualTo(Arrays.copyOfRange(source, 0, 10));
            assertThat(readAll(array.getBlob(1))).isEqualTo(Arrays.copyOfRange(source, 10, 20));
            assertThat(readAll(array.getBlob(2))).isEqualTo(Arrays.copyOfRange(source, 20, 30));
        }
    }

    @Test
    public void testMapBlobRefsReuseSource(@TempDir java.nio.file.Path tempDir) throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(30);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        Map<Object, Object> entries = new LinkedHashMap<>();
        entries.put(
                BinaryString.fromString("a"), new BlobRef(reader, new BlobDescriptor(uri, 0, 10)));
        entries.put(
                BinaryString.fromString("b"), new BlobRef(reader, new BlobDescriptor(uri, 10, 10)));
        entries.put(
                BinaryString.fromString("c"), new BlobRef(reader, new BlobDescriptor(uri, 20, 10)));

        BlobFormatWriter writer =
                newWriter(
                        outputFile,
                        RowType.of(DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB())));
        writer.addElement(GenericRow.of(new GenericMap(entries)));
        writer.close();

        assertThat(reader.openCount).isEqualTo(1);
    }

    @Test
    public void testBlobRefEqualityContractUnchanged() {
        String uri = "mem://file";
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, new byte[] {1, 2, 3}));
        BlobDescriptor descriptor = new BlobDescriptor(uri, 0, 3);
        BlobRef plain = new BlobRef(reader, descriptor);
        BlobRef sameDescriptor = new BlobRef(reader, descriptor);
        BlobRef subclass = new BlobRef(reader, descriptor) {};

        // Exact-class equality: plain refs equal, subclasses unequal both ways.
        assertThat(plain).isEqualTo(sameDescriptor);
        assertThat(plain.hashCode()).isEqualTo(sameDescriptor.hashCode());
        assertThat(plain).isNotEqualTo(subclass);
        assertThat(subclass).isNotEqualTo(plain);
        assertThat(new java.util.HashSet<>(Arrays.asList(plain, subclass))).hasSize(2);
        assertThat(new java.util.HashSet<>(Arrays.asList(subclass, plain))).hasSize(2);
    }

    @Test
    public void testBlobRefSubclassIsNotBypassed(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        // Raw source holds [1,2,3]; the subclass overrides newInputStream() to yield [9,9,9].
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, new byte[] {1, 2, 3}));
        BlobRef transforming =
                new BlobRef(reader, new BlobDescriptor(uri, 0, 3)) {
                    @Override
                    public SeekableInputStream newInputStream() {
                        return new CountingSeekableInputStream(new byte[] {9, 9, 9});
                    }
                };
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(transforming));
        writer.close();

        // The override is honored (slow path); the raw-source fast path did not bypass it.
        assertThat(readBackBlobs(outputFile, 1)).containsExactly(new byte[] {9, 9, 9});
        assertThat(reader.openCount).isEqualTo(0);
    }

    @Test
    public void testConsecutiveBlobRefsShareSingleOpenSource(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(1000);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        List<byte[]> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int offset = i * 10;
            writer.addElement(
                    GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, offset, 10))));
            expected.add(Arrays.copyOfRange(source, offset, offset + 10));
        }
        writer.close();

        // 100 references into the same source only opened it once.
        assertThat(reader.openCount).isEqualTo(1);
        assertThat(reader.opened.get(0).closeCount).isEqualTo(1);
        assertThat(readBackBlobs(outputFile, 100)).containsExactlyElementsOf(expected);
    }

    @Test
    public void testEmptyBlobRefsReuseWithoutFailing(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://empty";
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, new byte[0]));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 0))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 0))));
        writer.close();

        // Two consecutive empty refs reuse one open source and write empty (non-NULL) blobs.
        assertThat(reader.openCount).isEqualTo(1);
        assertThat(readBackBlobs(outputFile, 2)).containsExactly(new byte[0], new byte[0]);
    }

    @Test
    public void testForwardOnlyRewindCloseErrorNotWrittenNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // First source is forward-only and fails to close; the second same-uri ref needs a rewind.
        // Even with writeNullOnFetchFailure, the close error must abort, not become a NULL.
        boolean[] first = {true};
        UriReader reader =
                u -> {
                    if (first[0]) {
                        first[0] = false;
                        return new ForwardOnlyCloseFailingStream(new byte[] {1, 2, 3});
                    }
                    return new CountingSeekableInputStream(new byte[] {1, 2, 3});
                };
        BlobFormatWriter writer =
                newWriter(tempDir.resolve("blob.out"), RowType.of(DataTypes.BLOB()), false, true);
        BlobDescriptor descriptor = new BlobDescriptor("mem://f", 0, 3);
        writer.addElement(GenericRow.of(new BlobRef(reader, descriptor)));
        assertThatThrownBy(() -> writer.addElement(GenericRow.of(new BlobRef(reader, descriptor))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("close failed");
    }

    @Test
    public void testKnownLengthNonSeekableSourceStillWrites(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        byte[] payload = {1, 2, 3};
        // A non-seekable source (like a wrapped HTTP stream) with known length and offset 0.
        UriReader nonSeekable = u -> SeekableInputStream.wrap(new ByteArrayInputStream(payload));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(
                GenericRow.of(new BlobRef(nonSeekable, new BlobDescriptor("mem://x", 0, 3))));
        writer.close();

        // Fast-path seek fails, so it falls back to the per-blob stream and still writes the blob.
        assertThat(readBackBlobs(outputFile, 1)).containsExactly(payload);
    }

    @Test
    public void testNonBlobRefDoesNotDisturbReusableSource(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(30);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        byte[] inlinePayload = "inline-blob".getBytes();
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 5))));
        // a non-BlobRef blob in between must not close/reopen the reusable source.
        writer.addElement(GenericRow.of(Blob.fromData(inlinePayload)));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 5, 5))));
        writer.close();

        assertThat(reader.openCount).isEqualTo(1);
        assertThat(readBackBlobs(outputFile, 3))
                .containsExactly(
                        Arrays.copyOfRange(source, 0, 5),
                        inlinePayload,
                        Arrays.copyOfRange(source, 5, 10));
    }

    @Test
    public void testOutOfOrderDescriptorsReadViaSeek(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(30);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 20, 10))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 10))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 10, 10))));
        writer.close();

        assertThat(reader.openCount).isEqualTo(1);
        assertThat(readBackBlobs(outputFile, 3))
                .containsExactly(
                        Arrays.copyOfRange(source, 20, 30),
                        Arrays.copyOfRange(source, 0, 10),
                        Arrays.copyOfRange(source, 10, 20));
    }

    @Test
    public void testReaderProducedBlobRefsReuseSingleOpen(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // Write a source blob file with several payloads.
        byte[][] payloads = {
            "blob-0".getBytes(),
            "blob-1".getBytes(),
            "blob-2-longer-payload".getBytes(),
            "blob-3".getBytes(),
            "blob-4".getBytes()
        };
        java.nio.file.Path sourceFile = tempDir.resolve("source.blob");
        BlobFormatWriter sourceWriter = newWriter(sourceFile, RowType.of(DataTypes.BLOB()));
        for (byte[] payload : payloads) {
            sourceWriter.addElement(GenericRow.of(Blob.fromData(payload)));
        }
        sourceWriter.close();

        // Read it back as descriptors through the real BlobFormatReader path.
        CountingLocalFileIO fileIO = new CountingLocalFileIO();
        Path sourcePath = new Path(sourceFile.toUri());
        long sourceSize = Files.size(sourceFile);
        List<InternalRow> rows = new ArrayList<>();
        try (SeekableInputStream in = fileIO.newInputStream(sourcePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, sourceSize, null);
            BlobFormatReader reader =
                    new BlobFormatReader(
                            fileIO, sourcePath, fileMeta, in, 1, 0, DataTypes.BLOB(), true);
            FileRecordIterator<InternalRow> iterator = reader.readBatch();
            for (int i = 0; i < payloads.length; i++) {
                InternalRow row = iterator.next();
                // Plain BlobRef: reader refs keep the public equality contract.
                assertThat(row.getBlob(0).getClass()).isEqualTo(BlobRef.class);
                rows.add(GenericRow.of(row.getBlob(0)));
            }
        }
        // Reading only opened the source once (for the reader's own stream).
        assertThat(fileIO.openCount(sourcePath)).isEqualTo(1);

        // Rewrite the descriptor BlobRefs; the writer must reuse one source stream, not one per
        // blob.
        java.nio.file.Path outputFile = tempDir.resolve("out.blob");
        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();

        // Exactly one additional open for all rewritten blobs.
        assertThat(fileIO.openCount(sourcePath)).isEqualTo(2);
        assertThat(readBackBlobs(outputFile, payloads.length)).containsExactly(payloads);
    }

    @Test
    public void testSameUriDifferentOffsetAndLength(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(30);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 4))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 4, 11))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 15, 15))));
        writer.close();

        assertThat(reader.openCount).isEqualTo(1);
        assertThat(readBackBlobs(outputFile, 3))
                .containsExactly(
                        Arrays.copyOfRange(source, 0, 4),
                        Arrays.copyOfRange(source, 4, 15),
                        Arrays.copyOfRange(source, 15, 30));
    }

    @Test
    public void testSeekFailureRespectsWriteNullConfig(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        UriReader seekFailing = u -> new SeekFailingStream();
        BlobDescriptor descriptor = new BlobDescriptor("mem://x", 2, 3);

        // writeNullOnFetchFailure=true: a seek failure writes NULL rather than aborting.
        java.nio.file.Path nullFile = tempDir.resolve("null.blob");
        BlobFormatWriter nullWriter = newWriter(nullFile, rowType, false, true);
        nullWriter.addElement(GenericRow.of(new BlobRef(seekFailing, descriptor)));
        nullWriter.close();
        LocalFileIO fileIO = new LocalFileIO();
        try (SeekableInputStream in = fileIO.newInputStream(new Path(nullFile.toUri()))) {
            BlobFileMeta meta = new BlobFileMeta(in, Files.size(nullFile), null);
            assertThat(meta.recordNumber()).isEqualTo(1);
            assertThat(meta.isNull(0)).isTrue();
        }

        // writeNullOnFetchFailure=false: the seek failure propagates.
        BlobFormatWriter failWriter =
                newWriter(tempDir.resolve("fail.blob"), rowType, false, false);
        assertThatThrownBy(
                        () ->
                                failWriter.addElement(
                                        GenericRow.of(new BlobRef(seekFailing, descriptor))))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testShortSourceThrowsEof(@TempDir java.nio.file.Path tempDir) throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(5);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        // descriptor claims 100 bytes but source only has 5.
        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new BlobRef(
                                                        reader, new BlobDescriptor(uri, 0, 100)))))
                .isInstanceOf(EOFException.class);
        // the broken source stream is closed on failure.
        assertThat(reader.opened.get(0).closeCount).isEqualTo(1);
    }

    @Test
    public void testSourceSwitchCloseErrorDoesNotWriteNull(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // First source fails to close; switching to a second source must not turn that into a NULL.
        UriReader reader =
                u ->
                        u.equals("mem://a")
                                ? new CloseFailingStream(new byte[] {1, 2, 3})
                                : new CountingSeekableInputStream(new byte[] {4, 5, 6});
        BlobFormatWriter writer =
                newWriter(tempDir.resolve("blob.out"), RowType.of(DataTypes.BLOB()), false, true);
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor("mem://a", 0, 3))));
        // Even with writeNullOnFetchFailure, the switch-close error aborts rather than losing data.
        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new BlobRef(
                                                        reader,
                                                        new BlobDescriptor("mem://b", 0, 3)))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("close failed");
    }

    @Test
    public void testSourceSwitchClosesPreviousStream(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uriA = "mem://a";
        String uriB = "mem://b";
        byte[] sourceA = sequentialBytes(5);
        byte[] sourceB = sequentialBytes(7);
        Map<String, byte[]> files = new LinkedHashMap<>();
        files.put(uriA, sourceA);
        files.put(uriB, sourceB);
        RecordingUriReader reader = new RecordingUriReader(files);
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uriA, 0, 5))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uriB, 0, 7))));

        // Switching sources opened a new stream and closed the previous one immediately.
        assertThat(reader.openCount).isEqualTo(2);
        assertThat(reader.opened.get(0).closeCount).isEqualTo(1);
        assertThat(reader.opened.get(1).closeCount).isEqualTo(0);

        writer.close();
        assertThat(reader.opened.get(0).closeCount).isEqualTo(1);
        assertThat(reader.opened.get(1).closeCount).isEqualTo(1);
        assertThat(readBackBlobs(outputFile, 2)).containsExactly(sourceA, sourceB);
    }

    @Test
    public void testWriterCloseClosesSourceOnce(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        String uri = "mem://file";
        byte[] source = sequentialBytes(30);
        RecordingUriReader reader = new RecordingUriReader(singleFile(uri, source));
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");

        BlobFormatWriter writer = newWriter(outputFile, RowType.of(DataTypes.BLOB()));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 0, 10))));
        writer.addElement(GenericRow.of(new BlobRef(reader, new BlobDescriptor(uri, 10, 10))));
        assertThat(reader.opened.get(0).closeCount).isEqualTo(0);

        writer.close();
        assertThat(reader.opened.get(0).closeCount).isEqualTo(1);
    }

    @Test
    public void testWriterCloseSurfacesSourceCloseError(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        UriReader closeFailing = u -> new CloseFailingStream(new byte[] {1, 2, 3});
        BlobFormatWriter writer =
                newWriter(tempDir.resolve("blob.out"), RowType.of(DataTypes.BLOB()));
        writer.addElement(
                GenericRow.of(new BlobRef(closeFailing, new BlobDescriptor("mem://x", 0, 3))));
        // The reusable source's close error propagates from writer.close().
        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("close failed");
    }

    private static final class CloseFailingStream extends SeekableInputStream {

        private final byte[] data;
        private int pos;

        private CloseFailingStream(byte[] data) {
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
            return pos < data.length ? data[pos++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void close() throws IOException {
            throw new IOException("close failed");
        }
    }

    private static final class CountingLocalFileIO extends LocalFileIO {

        private final Map<String, Integer> opens = new HashMap<>();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            opens.merge(path.toString(), 1, Integer::sum);
            return super.newInputStream(path);
        }

        private int openCount(Path path) {
            return opens.getOrDefault(path.toString(), 0);
        }
    }

    private static final class ForwardOnlyCloseFailingStream extends SeekableInputStream {

        private final byte[] data;
        private int pos;

        private ForwardOnlyCloseFailingStream(byte[] data) {
            this.data = data;
        }

        @Override
        public void seek(long desired) {
            throw new UnsupportedOperationException("forward-only");
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void close() throws IOException {
            throw new IOException("close failed");
        }
    }

    private static final class SeekFailingStream extends SeekableInputStream {

        @Override
        public void seek(long desired) throws IOException {
            throw new IOException("seek failed");
        }

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return -1;
        }

        @Override
        public void close() {}
    }
}
