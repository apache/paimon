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

package org.apache.paimon.io;

import org.apache.paimon.compression.Lz4BlockCompressionFactory;
import org.apache.paimon.utils.MathUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompressedPageFileOutput} and {@link CompressedPageFileInput}. */
public class CompressedInputOutputTest {

    @TempDir Path tempDir;

    @Test
    public void testRandom() throws IOException {
        for (int i = 0; i < 100; i++) {
            innerTestRandom();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void innerTestRandom() throws IOException {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        byte[] bytes = new byte[rnd.nextInt(1_000) + 1];
        rnd.nextBytes(bytes);
        int pageSize = MathUtils.roundDownToPowerOf2(rnd.nextInt(2_000) + 2);

        // prepare compressed file
        File compressed = new File(tempDir.toFile(), "compressed");
        compressed.delete();
        Lz4BlockCompressionFactory compressionFactory = new Lz4BlockCompressionFactory();
        CompressedPageFileOutput output1 =
                new CompressedPageFileOutput(compressed, pageSize, compressionFactory);
        long uncompressBytes;
        long[] pagePositions;
        try {
            output1.write(bytes, 0, bytes.length);
        } finally {
            output1.close();
            uncompressBytes = output1.uncompressBytes();
            pagePositions = output1.pages();
        }
        CompressedPageFileInput input1 =
                new CompressedPageFileInput(
                        new RandomAccessFile(compressed, "r"),
                        pageSize,
                        compressionFactory,
                        uncompressBytes,
                        pagePositions);

        // prepare uncompressed file
        File uncompressed = new File(tempDir.toFile(), "uncompressed");
        uncompressed.delete();
        try (UncompressedPageFileOutput output2 = new UncompressedPageFileOutput(uncompressed)) {
            output2.write(bytes, 0, bytes.length);
        }
        UncompressedPageFileInput input2 =
                new UncompressedPageFileInput(new RandomAccessFile(uncompressed, "r"), pageSize);

        // test uncompressBytes
        assertThat(input1.uncompressBytes()).isEqualTo(input2.uncompressBytes());

        // test readPage
        for (int i = 0; i < pagePositions.length; i++) {
            assertThat(input1.readPage(i)).isEqualTo(input2.readPage(i));
        }

        // test readPosition
        for (int i = 0; i < 10; i++) {
            long position;
            int length;
            if (uncompressBytes == 1) {
                position = 0;
                length = 1;
            } else {
                position = rnd.nextLong(uncompressBytes - 1);
                length = rnd.nextInt((int) (uncompressBytes - position)) + 1;
            }
            assertThat(input1.readPosition(position, length))
                    .isEqualTo(input2.readPosition(position, length));
        }

        input1.close();
        input2.close();
    }
}
