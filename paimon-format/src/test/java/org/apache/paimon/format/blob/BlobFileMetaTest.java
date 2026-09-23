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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobFileMeta}. */
public class BlobFileMetaTest {

    private static final int RECORD_LENGTH = Integer.BYTES + Long.BYTES + Integer.BYTES;

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private Path file;

    @BeforeEach
    public void beforeEach() {
        fileIO = LocalFileIO.create();
        file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    @Test
    public void testRejectsFileSmallerThanFooter() throws IOException {
        for (int size = 0; size < Integer.BYTES + Byte.BYTES; size++) {
            writeFile(new byte[size]);
            assertThatThrownBy(() -> readMeta(null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("smaller than footer");
        }
    }

    @Test
    public void testRejectsInvalidIndexLength() throws IOException {
        writeFile(blobFileBytes(0, new byte[0], -1, BlobFormatWriter.VERSION));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid index length -1");

        writeFile(blobFileBytes(0, new byte[0], 1, BlobFormatWriter.VERSION));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid index length 1");
    }

    @Test
    public void testRejectsUnsupportedVersionAndMalformedIndex() throws IOException {
        writeFile(blobFileBytes(0, new byte[0], 0, (byte) (BlobFormatWriter.VERSION + 1)));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported version");

        byte[] malformedIndex = new byte[] {(byte) 0x80};
        writeFile(
                blobFileBytes(0, malformedIndex, malformedIndex.length, BlobFormatWriter.VERSION));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid index")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    public void testRejectsInvalidRecordLength() throws IOException {
        assertInvalidRecordLength(-3, 0);
        assertInvalidRecordLength(0, 0);
        assertInvalidRecordLength(RECORD_LENGTH - 1, RECORD_LENGTH - 1);
    }

    @Test
    public void testRejectsRecordLengthSumMismatch() throws IOException {
        writeFile(blobFileBytes(RECORD_LENGTH - 1, RECORD_LENGTH));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exceeds the data region");

        writeFile(blobFileBytes(RECORD_LENGTH + 1, RECORD_LENGTH));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("indexed records use")
                .hasMessageContaining("data region contains");
    }

    @Test
    public void testAcceptsEmptyFileAndKnownSentinels() throws IOException {
        writeFile(blobFileBytes(0));
        assertThat(readMeta(null).recordNumber()).isZero();

        writeFile(
                blobFileBytes(
                        RECORD_LENGTH,
                        BlobFormatWriter.NULL_LENGTH,
                        RECORD_LENGTH,
                        BlobFormatWriter.PLACE_HOLDER_LENGTH));
        BlobFileMeta meta = readMeta(null);
        assertThat(meta.recordNumber()).isEqualTo(3);
        assertThat(meta.isNull(0)).isTrue();
        assertThat(meta.blobOffset(0)).isEqualTo(-1);
        assertThat(meta.blobOffset(1)).isZero();
        assertThat(meta.isPlaceHolder(2)).isTrue();
        assertThat(meta.blobOffset(2)).isEqualTo(-1);
    }

    @Test
    public void testValidatesSelectionPositions() throws IOException {
        writeFile(blobFileBytes(RECORD_LENGTH, RECORD_LENGTH));

        RoaringBitmap32 tooMany = new RoaringBitmap32();
        tooMany.add(0);
        tooMany.add(1);
        assertThatThrownBy(() -> readMeta(tooMany))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("cardinality 2 exceeds record count 1");

        RoaringBitmap32 outOfRange = new RoaringBitmap32();
        outOfRange.add(1);
        assertThatThrownBy(() -> readMeta(outOfRange))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("position 1 is outside record count 1");
    }

    @Test
    public void testNonContiguousAndEmptySelection() throws IOException {
        writeFile(blobFileBytes(RECORD_LENGTH * 3, RECORD_LENGTH, RECORD_LENGTH, RECORD_LENGTH));

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        selection.add(2);
        BlobFileMeta selected = readMeta(selection);
        assertThat(selected.recordNumber()).isEqualTo(2);
        assertThat(selected.blobOffset(0)).isZero();
        assertThat(selected.blobOffset(1)).isEqualTo(RECORD_LENGTH * 2L);
        assertThat(selected.returnedPosition(1)).isZero();
        assertThat(selected.returnedPosition(2)).isEqualTo(2);

        BlobFileMeta empty = readMeta(new RoaringBitmap32());
        assertThat(empty.recordNumber()).isZero();
    }

    private void assertInvalidRecordLength(long recordLength, int dataLength) throws IOException {
        writeFile(blobFileBytes(dataLength, recordLength));
        assertThatThrownBy(() -> readMeta(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid record length " + recordLength);
    }

    private BlobFileMeta readMeta(RoaringBitmap32 selection) throws IOException {
        try (SeekableInputStream in = fileIO.newInputStream(file)) {
            return new BlobFileMeta(in, fileIO.getFileSize(file), selection);
        }
    }

    private void writeFile(byte[] bytes) throws IOException {
        Files.write(Paths.get(file.toUri()), bytes);
    }

    private static byte[] blobFileBytes(int dataLength, long... recordLengths) {
        byte[] index = DeltaVarintCompressor.compress(recordLengths);
        return blobFileBytes(dataLength, index, index.length, BlobFormatWriter.VERSION);
    }

    private static byte[] blobFileBytes(
            int dataLength, byte[] index, int declaredIndexLength, byte version) {
        byte[] bytes = new byte[dataLength + index.length + Integer.BYTES + Byte.BYTES];
        System.arraycopy(index, 0, bytes, dataLength, index.length);
        byte[] indexLengthBytes = intToLittleEndian(declaredIndexLength);
        System.arraycopy(
                indexLengthBytes,
                0,
                bytes,
                bytes.length - Integer.BYTES - Byte.BYTES,
                indexLengthBytes.length);
        bytes[bytes.length - 1] = version;
        return bytes;
    }
}
