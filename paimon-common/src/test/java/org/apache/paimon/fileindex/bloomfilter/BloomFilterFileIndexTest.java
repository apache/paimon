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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/** Tests for {@link BloomFilterFileIndex}. */
public class BloomFilterFileIndexTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testAddFindByRandom() {
        BloomFilterFileIndex filter =
                new BloomFilterFileIndex(
                        DataTypes.BYTES(),
                        new Options(
                                new HashMap<String, String>() {
                                    {
                                        put("items", "10000");
                                        put("fpp", "0.02");
                                    }
                                }));
        FileIndexWriter writer = filter.createWriter();
        List<byte[]> testData = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            testData.add(random());
        }

        // test empty bytes
        testData.add(new byte[0]);

        testData.forEach(writer::write);

        byte[] serializedBytes = writer.serializedBytes();
        FileIndexReader reader =
                filter.createReader(
                        new ByteArraySeekableStream(serializedBytes),
                        Pair.of(0, serializedBytes.length));

        for (byte[] bytes : testData) {
            Assertions.assertThat(reader.visitEqual(null, bytes).remain()).isTrue();
        }

        int errorCount = 0;
        int num = 1000000;
        for (int i = 0; i < num; i++) {
            byte[] ra = random();
            if (reader.visitEqual(null, ra).remain()) {
                errorCount++;
            }
        }

        // ffp should be less than 0.03
        Assertions.assertThat((double) errorCount / num).isLessThan(0.03);
    }

    @Test
    public void testAddFindByRandomLong() {
        BloomFilterFileIndex filter =
                new BloomFilterFileIndex(
                        DataTypes.BIGINT(),
                        new Options(
                                new HashMap<String, String>() {
                                    {
                                        put("items", "10000");
                                        put("fpp", "0.02");
                                    }
                                }));
        FileIndexWriter writer = filter.createWriter();
        List<Long> testData = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            testData.add(RANDOM.nextLong());
        }

        testData.forEach(writer::write);

        byte[] serializedBytes = writer.serializedBytes();
        FileIndexReader reader =
                filter.createReader(
                        new ByteArraySeekableStream(serializedBytes),
                        Pair.of(0, serializedBytes.length));

        for (Long value : testData) {
            Assertions.assertThat(reader.visitEqual(null, value).remain()).isTrue();
        }

        int errorCount = 0;
        int num = 1000000;
        for (int i = 0; i < num; i++) {
            Long ra = RANDOM.nextLong();
            if (reader.visitEqual(null, ra).remain()) {
                errorCount++;
            }
        }

        // ffp should be less than 0.03
        Assertions.assertThat((double) errorCount / num).isLessThan(0.03);
    }

    private byte[] random() {
        byte[] b = new byte[Math.abs(RANDOM.nextInt(400) + 1)];
        RANDOM.nextBytes(b);
        return b;
    }
}
