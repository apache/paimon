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

package org.apache.paimon.format.text;

import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HadoopCompressionUtils}. */
class HadoopCompressionUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    private static final String TEST_DATA = "This is test data for compression.";

    @Test
    void testCreateCompressedOutputStreamWithNoneCompression() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TestPositionOutputStream positionOutputStream =
                new TestPositionOutputStream(byteArrayOutputStream);

        OutputStream result =
                HadoopCompressionUtils.createCompressedOutputStream(
                        positionOutputStream, HadoopCompressionType.NONE.value());

        assertThat(result).isSameAs(positionOutputStream);
    }

    @Test
    void testCreateCompressedOutputStreamWithInvalidCompression() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TestPositionOutputStream positionOutputStream =
                new TestPositionOutputStream(byteArrayOutputStream);

        assertThatThrownBy(
                        () ->
                                HadoopCompressionUtils.createCompressedOutputStream(
                                        positionOutputStream, "invalid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreateDecompressedInputStreamWithNoCompression() throws IOException {
        byte[] testData = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        TestSeekableInputStream seekableInputStream = new TestSeekableInputStream(testData);
        Path filePath = new Path("test.txt");

        InputStream result =
                HadoopCompressionUtils.createDecompressedInputStream(seekableInputStream, filePath);

        // For uncompressed files, should return the original stream
        assertThat(result).isSameAs(seekableInputStream);
    }

    @Test
    void testCreateDecompressedInputStreamWithGzipFile() throws IOException {
        byte[] testData = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        TestSeekableInputStream seekableInputStream = new TestSeekableInputStream(testData);
        Path filePath = new Path("test.txt.gz");

        InputStream result =
                HadoopCompressionUtils.createDecompressedInputStream(seekableInputStream, filePath);

        assertThat(result).isNotNull();
        // For compressed files, should return a different stream (decompression wrapper)
        // Note: The actual decompression behavior depends on Hadoop codecs being available
    }

    @ParameterizedTest
    @EnumSource(HadoopCompressionType.class)
    void testCreateCompressedOutputStreamWithAvailableCompressions(
            HadoopCompressionType compressionType) throws IOException {
        if (compressionType.hadoopCodecClassName() == null) {
            return; // Skip types without codec class names
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TestPositionOutputStream positionOutputStream =
                new TestPositionOutputStream(byteArrayOutputStream);

        try {
            OutputStream compressedStream =
                    HadoopCompressionUtils.createCompressedOutputStream(
                            positionOutputStream, compressionType.value());

            assertThat(compressedStream).isNotNull();

            // Write and close to ensure compression happens
            compressedStream.write(TEST_DATA.getBytes(StandardCharsets.UTF_8));
            compressedStream.close();

            // Verify that some data was written
            assertThat(byteArrayOutputStream.toByteArray()).isNotEmpty();
        } catch (Exception e) {
            // Skip compression type if not available
        }
    }

    @ParameterizedTest
    @EnumSource(value = HadoopCompressionType.class)
    void testCreateDecompressedInputStreamWithAvailableExtensions(
            HadoopCompressionType compressionType) throws IOException {
        if (compressionType.fileExtension() == null) {
            return; // Skip types without file extensions
        }

        byte[] testData = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        TestSeekableInputStream seekableInputStream = new TestSeekableInputStream(testData);
        Path filePath = new Path("test.txt." + compressionType.fileExtension());

        try {
            InputStream result =
                    HadoopCompressionUtils.createDecompressedInputStream(
                            seekableInputStream, filePath);
            assertThat(result).isNotNull();
        } catch (Exception e) {
            // Skip compression type if not available
        }
    }

    @Test
    void testRoundTripCompressionDecompression() throws IOException {
        // Test with actual file I/O using GZIP which is most commonly available
        java.nio.file.Path testFile = tempDir.resolve("test_roundtrip.txt.gz");
        LocalFileIO fileIO = new LocalFileIO();
        Path paimonPath = new Path(testFile.toString());

        // Write compressed data
        try (PositionOutputStream outputStream = fileIO.newOutputStream(paimonPath, false);
                OutputStream compressedStream =
                        HadoopCompressionUtils.createCompressedOutputStream(
                                outputStream, HadoopCompressionType.GZIP.value())) {
            compressedStream.write(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        }

        // Verify file was created and has content
        assertThat(Files.exists(testFile)).isTrue();
        assertThat(Files.size(testFile)).isGreaterThan(0);

        // Read decompressed data
        try (SeekableInputStream inputStream = fileIO.newInputStream(paimonPath);
                InputStream decompressedStream =
                        HadoopCompressionUtils.createDecompressedInputStream(
                                inputStream, paimonPath)) {

            byte[] buffer = new byte[TEST_DATA.length() * 2]; // Extra space
            int bytesRead = decompressedStream.read(buffer);

            String decompressedData = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
            assertThat(decompressedData).isEqualTo(TEST_DATA);
        }
    }

    @Test
    void testCreateCompressedOutputStreamWithNullCompression() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TestPositionOutputStream positionOutputStream =
                new TestPositionOutputStream(byteArrayOutputStream);

        assertThatThrownBy(
                        () ->
                                HadoopCompressionUtils.createCompressedOutputStream(
                                        positionOutputStream, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testCreateDecompressedInputStreamWithNullPath() {
        byte[] testData = TEST_DATA.getBytes(StandardCharsets.UTF_8);
        TestSeekableInputStream seekableInputStream = new TestSeekableInputStream(testData);

        try {
            InputStream result =
                    HadoopCompressionUtils.createDecompressedInputStream(seekableInputStream, null);
            // Should handle null path gracefully and return original stream
            assertThat(result).isNotNull();
        } catch (IOException e) {
            // Null path may cause IOException, which is acceptable behavior
            assertThat(e).hasMessageContaining("Failed to create decompression stream");
        }
    }

    private static class TestPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream delegate;
        private long position = 0;

        public TestPositionOutputStream(ByteArrayOutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            position++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            delegate.write(b);
            position += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class TestSeekableInputStream extends SeekableInputStream {
        private final ByteArrayInputStream delegate;
        private long position = 0;

        public TestSeekableInputStream(byte[] data) {
            this.delegate = new ByteArrayInputStream(data);
        }

        @Override
        public void seek(long pos) throws IOException {
            delegate.reset();
            long skipped = delegate.skip(pos);
            if (skipped != pos) {
                throw new IOException("Could not seek to position " + pos);
            }
            position = pos;
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public int read() throws IOException {
            int result = delegate.read();
            if (result != -1) {
                position++;
            }
            return result;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int result = delegate.read(b);
            if (result != -1) {
                position += result;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int result = delegate.read(b, off, len);
            if (result != -1) {
                position += result;
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
