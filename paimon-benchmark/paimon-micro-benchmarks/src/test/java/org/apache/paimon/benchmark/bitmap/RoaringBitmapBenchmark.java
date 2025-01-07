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

package org.apache.paimon.benchmark.bitmap;

import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for {@link RoaringBitmap}. */
public class RoaringBitmapBenchmark {

    public static final int ROW_COUNT = 10000000;

    @TempDir Path tempDir;

    @Test
    public void testDeserialize() throws Exception {
        Random random = new Random();
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < ROW_COUNT; i++) {
            if (random.nextBoolean()) {
                bitmap.add(i);
            }
        }

        File file = new File(tempDir.toFile(), "bitmap32-deserialize-benchmark");
        assertThat(file.createNewFile()).isTrue();
        try (FileOutputStream output = new FileOutputStream(file);
                DataOutputStream dos = new DataOutputStream(output)) {
            bitmap.serialize(dos);
        }

        Benchmark benchmark =
                new Benchmark("bitmap32-deserialize-benchmark", 100)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "deserialize(DataInput)",
                10,
                () -> {
                    try (LocalFileIO.LocalSeekableInputStream seekableStream =
                                    new LocalFileIO.LocalSeekableInputStream(file);
                            DataInputStream input = new DataInputStream(seekableStream)) {
                        new RoaringBitmap().deserialize(input);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        benchmark.addCase(
                "deserialize(DataInput, byte[])",
                10,
                () -> {
                    try (LocalFileIO.LocalSeekableInputStream seekableStream =
                                    new LocalFileIO.LocalSeekableInputStream(file);
                            DataInputStream input = new DataInputStream(seekableStream)) {
                        new RoaringBitmap().deserialize(input, null);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        benchmark.addCase(
                "deserialize(ByteBuffer)",
                10,
                () -> {
                    try (LocalFileIO.LocalSeekableInputStream seekableStream =
                                    new LocalFileIO.LocalSeekableInputStream(file);
                            DataInputStream input = new DataInputStream(seekableStream)) {
                        byte[] bytes = new byte[(int) file.length()];
                        input.readFully(bytes);
                        new RoaringBitmap().deserialize(ByteBuffer.wrap(bytes));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        benchmark.run();
    }
}
