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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobFormatWriter}. */
public class BlobFormatWriterTest {

    @Test
    public void testWriteNullOnMissingFileFallbackForHttpNotFound(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        true);

        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingHttpReader(),
                                new BlobDescriptor("https://example.com/missing.jpg", 0, -1))));

        writer.close();

        assertThat(outputFile.toFile()).exists();
    }

    @Test
    public void testMissingHttpBlobFollowedByValidBlobPreservesReadback(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        java.nio.file.Path outputFile = tempDir.resolve("blob.out");
        byte[] validPayload = "valid-blob-content".getBytes();

        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(outputFile.toFile()),
                        null,
                        rowType,
                        true);
        writer.addElement(
                GenericRow.of(
                        new BlobRef(
                                failingHttpReader(),
                                new BlobDescriptor("https://example.com/missing.jpg", 0, -1))));
        writer.addElement(GenericRow.of(Blob.fromData(validPayload)));
        writer.close();

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(2);
            assertThat(fileMeta.isNull(0)).isTrue();
            assertThat(fileMeta.isNull(1)).isFalse();

            BlobFormatReader reader = new BlobFormatReader(fileIO, filePath, fileMeta, in, 1, 0);
            FileRecordIterator<InternalRow> iterator = reader.readBatch();
            InternalRow nullRow = iterator.next();
            InternalRow validRow = iterator.next();

            assertThat(nullRow.isNullAt(0)).isTrue();
            Blob readBlob = validRow.getBlob(0);
            try (SeekableInputStream blobIn = readBlob.newInputStream()) {
                byte[] actual = new byte[validPayload.length];
                org.apache.paimon.utils.IOUtils.readFully(blobIn, actual);
                assertThat(actual).isEqualTo(validPayload);
            }
        }
    }

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
                        rowType,
                        true);
        writer.addElement(GenericRow.of(Blob.fromData(firstPayload)));
        writer.addElement(GenericRow.of(Blob.fromData(secondPayload)));
        writer.close();

        LocalFileIO fileIO = new LocalFileIO();
        Path filePath = new Path(outputFile.toUri());
        long fileSize = Files.size(outputFile);
        try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
            BlobFileMeta fileMeta = new BlobFileMeta(in, fileSize, null);
            assertThat(fileMeta.recordNumber()).isEqualTo(2);

            BlobFormatReader reader = new BlobFormatReader(fileIO, filePath, fileMeta, in, 1, 0);
            FileRecordIterator<InternalRow> iterator = reader.readBatch();
            assertBlobPayload(iterator.next().getBlob(0), firstPayload);
            assertBlobPayload(iterator.next().getBlob(0), secondPayload);
        }
    }

    private static void assertBlobPayload(Blob blob, byte[] expected) throws Exception {
        try (SeekableInputStream blobIn = blob.newInputStream()) {
            byte[] actual = new byte[expected.length];
            org.apache.paimon.utils.IOUtils.readFully(blobIn, actual);
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void testWriteNullOnMissingFileDisabledPropagatesHttpNotFound(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.BLOB());
        BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                tempDir.resolve("blob.out").toFile()),
                        null,
                        rowType,
                        false);

        assertThatThrownBy(
                        () ->
                                writer.addElement(
                                        GenericRow.of(
                                                new BlobRef(
                                                        failingHttpReader(),
                                                        new BlobDescriptor(
                                                                "https://example.com/missing.jpg",
                                                                0,
                                                                -1)))))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("HTTP error code: 404");
    }

    private static UriReader failingHttpReader() {
        return new UriReader() {
            @Override
            public SeekableInputStream newInputStream(String uri) {
                throw new RuntimeException("HTTP error code: 404");
            }
        };
    }
}
