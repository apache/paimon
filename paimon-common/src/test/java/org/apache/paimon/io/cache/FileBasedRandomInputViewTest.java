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

package org.apache.paimon.io.cache;

import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Test for {@link FileBasedRandomInputView}. */
@ExtendWith(ParameterizedTestExtension.class)
public class FileBasedRandomInputViewTest {

    @TempDir Path tempDir;

    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    private final Cache.CacheType cacheType;

    public FileBasedRandomInputViewTest(Cache.CacheType cacheType) {
        this.cacheType = cacheType;
    }

    @Parameters(name = "{0}")
    public static List<Cache.CacheType> getVarSeg() {
        return Arrays.asList(Cache.CacheType.CAFFEINE, Cache.CacheType.GUAVA);
    }

    @TestTemplate
    public void testMatched() throws IOException {
        innerTest(1024 * 512, 5000);
    }

    @TestTemplate
    public void testNotMatched() throws IOException {
        innerTest(131092, 1000);
    }

    @TestTemplate
    public void testRandom() throws IOException {
        innerTest(rnd.nextInt(5000, 100000), 100);
    }

    private void innerTest(int len, int maxFileReadCount) throws IOException {
        byte[] bytes = new byte[len];
        MemorySegment segment = MemorySegment.wrap(bytes);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) rnd.nextInt();
        }

        File file = writeFile(bytes);
        CacheManager cacheManager = new CacheManager(cacheType, MemorySize.ofKibiBytes(128), 0.25);
        FileBasedRandomInputView view =
                new FileBasedRandomInputView(
                        PageFileInput.create(file, 1024, null, 0, null), cacheManager);

        // read first one
        // this assertThatCode check the ConcurrentModificationException is not threw.
        assertThatCode(() -> view.setReadPosition(0)).doesNotThrowAnyException();
        assertThat(view.readLong()).isEqualTo(segment.getLongBigEndian(0));

        // read mid
        int mid = bytes.length / 2;
        assertThatCode(() -> view.setReadPosition(mid)).doesNotThrowAnyException();
        assertThat(view.readLong()).isEqualTo(segment.getLongBigEndian(mid));

        // read special
        assertThatCode(() -> view.setReadPosition(1021)).doesNotThrowAnyException();
        assertThat(view.readLong()).isEqualTo(segment.getLongBigEndian(1021));

        // read last one
        assertThatCode(() -> view.setReadPosition(bytes.length - 1)).doesNotThrowAnyException();
        assertThat(view.readByte()).isEqualTo(bytes[bytes.length - 1]);

        // random read
        for (int i = 0; i < 10000; i++) {
            // hot key -> 10
            int position = rnd.nextBoolean() ? 10 : rnd.nextInt(bytes.length - 8);
            assertThatCode(() -> view.setReadPosition(position)).doesNotThrowAnyException();
            assertThat(view.readLong()).isEqualTo(segment.getLongBigEndian(position));
        }

        view.close();

        // hot key in LRU, should have good cache hit rate
        assertThat(cacheManager.fileReadCount()).isLessThan(maxFileReadCount);
        assertThat(cacheManager.dataCache().asMap().size()).isEqualTo(0);
        assertThat(cacheManager.indexCache().asMap().size()).isEqualTo(0);
    }

    private File writeFile(byte[] bytes) throws IOException {
        File file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        if (!file.createNewFile()) {
            throw new IOException("Can not create: " + file);
        }
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
        return file;
    }
}
