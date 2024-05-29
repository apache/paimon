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

package org.apache.paimon.fs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class VectoredReadUtilsTest {

    private final byte[] bytes;
    private final VectoredReadable readable;

    public VectoredReadUtilsTest() {
        this.bytes = new byte[1024 * 1024];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        random.nextBytes(bytes);
        this.readable =
                new VectoredReadable() {
                    @Override
                    public int minSeekForVectorReads() {
                        return 100;
                    }

                    @Override
                    public int pread(long position, byte[] buffer, int offset, int length)
                            throws IOException {
                        boolean returnAll = random.nextBoolean();
                        int len = returnAll ? length : random.nextInt(length) + 1;
                        System.arraycopy(bytes, (int) position, buffer, offset, len);
                        return len;
                    }
                };
    }

    private void doTest(List<FileRange> ranges) throws Exception {
        VectoredReadUtils.readVectored(readable, ranges);
        for (FileRange range : ranges) {
            byte[] expected = new byte[range.getLength()];
            System.arraycopy(bytes, (int) range.getOffset(), expected, 0, range.getLength());
            assertThat(range.getData().get()).isEqualTo(expected);
        }
    }

    @Test
    public void testNormal() throws Exception {
        // test empty
        doTest(Collections.emptyList());

        // test without merge
        doTest(
                Arrays.asList(
                        FileRange.createFileRange(0, 100),
                        FileRange.createFileRange(100, 200),
                        FileRange.createFileRange(500, 1000)));

        // test with merge
        doTest(
                Arrays.asList(
                        FileRange.createFileRange(0, 60),
                        FileRange.createFileRange(100, 90),
                        FileRange.createFileRange(300, 200)));

        // test with batchSize
        doTest(
                Arrays.asList(
                        FileRange.createFileRange(60, 800),
                        FileRange.createFileRange(1000, 500),
                        FileRange.createFileRange(1550, 600)));

        // test with align huge
        doTest(
                Arrays.asList(
                        FileRange.createFileRange(0, 5000),
                        FileRange.createFileRange(6000, 500),
                        FileRange.createFileRange(7000, 800)));

        // test with no align huge
        doTest(
                Arrays.asList(
                        FileRange.createFileRange(60, 5120),
                        FileRange.createFileRange(6020, 520),
                        FileRange.createFileRange(7300, 850)));
    }

    @Test
    public void testRandom() throws Exception {
        List<FileRange> ranges = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int lastEnd = 0;
        for (int i = 0; i < random.nextInt(10); i++) {
            int start = random.nextInt(102 * 1024) + lastEnd;
            int len = random.nextInt(102 * 1024) + 1;
            if (start + len > bytes.length) {
                break;
            }
            ranges.add(FileRange.createFileRange(start, len));
            lastEnd = start + len;
        }
        doTest(ranges);
    }
}
