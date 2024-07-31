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

package org.apache.paimon.cache;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.BlockCacheType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.FileOutputStream;
import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;

/** Test for {@link CachedSeekableInputStream}. */
public class CachedSeekableInputStreamTest {

    private static @TempDir java.nio.file.Path path;
    private static File file;

    @BeforeAll
    public static void setup() throws Exception {
        file = new File(path.toFile(), "random.bin");
        generateRandomFile(file, 100 * 1024 * 1024L);
    }

    @ParameterizedTest()
    @EnumSource(BlockCacheType.class)
    public void testCached(BlockCacheType type) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BLOCK_CACHE_TYPE, type);

        LocalFileIO.LocalSeekableInputStream local = new LocalFileIO.LocalSeekableInputStream(file);
        CachedSeekableInputStream inputStream =
                new CachedSeekableInputStream(
                        new LocalFileIO.LocalSeekableInputStream(file),
                        new Path(file.getAbsolutePath()),
                        file.length(),
                        new BlockCacheConfig(new CoreOptions(options)));
        for (int i = 0; i < 50; i++) {
            testSeekAndRead(file.length(), local, inputStream);
        }
    }

    private void testSeekAndRead(long size, SeekableInputStream in1, CachedSeekableInputStream in2)
            throws Exception {
        int seekTo = ThreadLocalRandom.current().nextInt(0, (int) size / 2);
        int length = ThreadLocalRandom.current().nextInt(1, (int) size / 2);

        in1.seek(seekTo);
        in2.seek(seekTo);

        int destOff = ThreadLocalRandom.current().nextInt(0, 2 * 1024 * 1024);
        int arrLength = length + destOff;
        byte[] bytes1 = new byte[arrLength];
        byte[] bytes2 = new byte[arrLength];
        byte[] bytes3 = new byte[arrLength];
        in1.read(bytes1, destOff, length);
        in2.read(bytes2, destOff, length);

        in2.seek(seekTo);
        in2.read(bytes3, destOff, length);

        Assertions.assertArrayEquals(bytes1, bytes2);
        Assertions.assertArrayEquals(bytes2, bytes3);
    }

    protected static void generateRandomFile(File file, long size) throws Exception {
        byte[] buffer = new byte[128 * 1024];
        SecureRandom random = new SecureRandom();
        long remainingBytes = size;
        try (FileOutputStream fio = new FileOutputStream(file)) {
            while (remainingBytes > 0) {
                int bytesToWrite = (int) Math.min(buffer.length, remainingBytes);
                random.nextBytes(buffer);
                fio.write(buffer, 0, bytesToWrite);
                remainingBytes -= bytesToWrite;
            }
        }
    }
}
