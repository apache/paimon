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

package org.apache.paimon.compression;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockCompressionTest {

    private static Stream<String> compressCodecGenerator() {
        return Stream.of("LZ4", "LZO", "ZSTD");
    }

    @ParameterizedTest
    @MethodSource("compressCodecGenerator")
    void testBlockCompression(String compress) {
        BlockCompressionFactory factory =
                BlockCompressionFactory.create(new CompressOptions(compress, 1));
        runTest(factory, 32768);
        runTest(factory, 16);
    }

    private void runTest(BlockCompressionFactory factory, int originalLen) {
        BlockCompressor compressor = factory.getCompressor();
        BlockDecompressor decompressor = factory.getDecompressor();

        int originalOff = 64;
        byte[] data = new byte[originalOff + originalLen];
        for (int i = 0; i < originalLen; i++) {
            data[originalOff + i] = (byte) i;
        }

        int compressedOff = 32;

        // 1. test compress with insufficient target
        byte[] insufficientCompressArray = new byte[compressedOff + HEADER_LENGTH + 1];
        assertThatThrownBy(
                        () ->
                                compressor.compress(
                                        data,
                                        originalOff,
                                        originalLen,
                                        insufficientCompressArray,
                                        compressedOff))
                .isInstanceOf(BufferCompressionException.class);

        // 2. test normal compress
        byte[] compressedData =
                new byte[compressedOff + compressor.getMaxCompressedSize(originalLen)];
        int compressedLen =
                compressor.compress(data, originalOff, originalLen, compressedData, compressedOff);

        int decompressedOff = 16;

        // 3. test decompress with insufficient target
        byte[] insufficientDecompressArray = new byte[decompressedOff + originalLen - 1];
        assertThatThrownBy(
                        () ->
                                decompressor.decompress(
                                        compressedData,
                                        compressedOff,
                                        compressedLen,
                                        insufficientDecompressArray,
                                        decompressedOff))
                .isInstanceOf(BufferDecompressionException.class);

        // 4. test normal decompress
        byte[] decompressedData = new byte[decompressedOff + originalLen];
        int decompressedLen =
                decompressor.decompress(
                        compressedData,
                        compressedOff,
                        compressedLen,
                        decompressedData,
                        decompressedOff);
        assertThat(decompressedLen).isEqualTo(originalLen);

        for (int i = 0; i < originalLen; i++) {
            assertThat(decompressedData[decompressedOff + i]).isEqualTo(data[originalOff + i]);
        }
    }
}
