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

package org.apache.paimon.fileindex.token;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Tests for {@link TokenBloomFilterFileIndex}. */
public class TokenBloomFilterFileIndexTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testTokenBloomFilterWithDefaultDelimiter() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("items", "10000");
        optionsMap.put("fpp", "0.02");

        TokenBloomFilterFileIndex filter =
                new TokenBloomFilterFileIndex(DataTypes.STRING(), new Options(optionsMap));

        FileIndexWriter writer = filter.createWriter();
        List<String> testData = new ArrayList<>();

        testData.add("hello world");
        testData.add("apache paimon project");
        testData.add("bloom filter index");
        testData.add("token based search");
        testData.add("");

        testData.forEach(writer::write);

        byte[] serializedBytes = writer.serializedBytes();
        FileIndexReader reader =
                filter.createReader(
                        new ByteArraySeekableStream(serializedBytes), 0, serializedBytes.length);

        for (String text : testData) {
            Assertions.assertThat(reader.visitEqual(null, text).remain()).isTrue();
        }

        Assertions.assertThat(reader.visitEqual(null, "hello").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "world").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "apache").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "paimon").remain()).isTrue();

        Assertions.assertThat(reader.visitEqual(null, "nonexistent").remain()).isFalse();
        Assertions.assertThat(reader.visitEqual(null, "missing token").remain()).isFalse();

        Assertions.assertThat(reader.visitEqual(null, null).remain()).isTrue();
    }

    @Test
    public void testTokenBloomFilterWithCustomDelimiter() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("items", "10000");
        optionsMap.put("fpp", "0.02");
        optionsMap.put("token.delimiter", ",");

        TokenBloomFilterFileIndex filter =
                new TokenBloomFilterFileIndex(DataTypes.STRING(), new Options(optionsMap));

        FileIndexWriter writer = filter.createWriter();
        List<String> testData = new ArrayList<>();

        testData.add("apple,orange,banana");
        testData.add("red,green,blue");
        testData.add("cat,dog,bird");

        testData.forEach(writer::write);

        byte[] serializedBytes = writer.serializedBytes();
        FileIndexReader reader =
                filter.createReader(
                        new ByteArraySeekableStream(serializedBytes), 0, serializedBytes.length);

        for (String text : testData) {
            Assertions.assertThat(reader.visitEqual(null, text).remain()).isTrue();
        }

        Assertions.assertThat(reader.visitEqual(null, "apple").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "orange").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "red").remain()).isTrue();

        Assertions.assertThat(reader.visitEqual(null, "apple orange").remain()).isFalse();
        Assertions.assertThat(reader.visitEqual(null, "grape").remain()).isFalse();
    }

    @Test
    public void testFalsePositiveRate() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("items", "10000");
        optionsMap.put("fpp", "0.02");

        TokenBloomFilterFileIndex filter =
                new TokenBloomFilterFileIndex(DataTypes.STRING(), new Options(optionsMap));

        FileIndexWriter writer = filter.createWriter();
        List<String> testData = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            StringBuilder sb = new StringBuilder();
            int numTokens = 1 + RANDOM.nextInt(5); // 1-5 tokens

            for (int j = 0; j < numTokens; j++) {
                if (j > 0){
                    sb.append(" ");
                }
                sb.append("token").append(RANDOM.nextInt(1000));
            }
            testData.add(sb.toString());
        }

        testData.forEach(writer::write);

        byte[] serializedBytes = writer.serializedBytes();
        FileIndexReader reader =
                filter.createReader(
                        new ByteArraySeekableStream(serializedBytes), 0, serializedBytes.length);

        for (String text : testData) {
            Assertions.assertThat(reader.visitEqual(null, text).remain()).isTrue();
        }

        int errorCount = 0;
        int numTests = 10000;

        for (int i = 0; i < numTests; i++) {
            StringBuilder sb = new StringBuilder();
            int numTokens = 1 + RANDOM.nextInt(3);

            for (int j = 0; j < numTokens; j++) {
                if (j > 0){
                    sb.append(" ");
                }
                sb.append("token").append(1000 + RANDOM.nextInt(10000));
            }

            if (reader.visitEqual(null, sb.toString()).remain()) {
                errorCount++;
            }
        }

        Assertions.assertThat((double) errorCount / numTests).isLessThan(0.03);
    }

    @Test
    public void testNonStringInput() {
        TokenBloomFilterFileIndex filter =
                new TokenBloomFilterFileIndex(DataTypes.STRING(), new Options());

        FileIndexWriter writer = filter.createWriter();

        writer.write("hello world");

        writer.write(123);
        writer.write(true);

        byte[] serializedBytes = writer.serializedBytes();
        FileIndexReader reader =
                filter.createReader(
                        new ByteArraySeekableStream(serializedBytes), 0, serializedBytes.length);

        Assertions.assertThat(reader.visitEqual(null, "hello").remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, "world").remain()).isTrue();

        Assertions.assertThat(reader.visitEqual(null, 123).remain()).isTrue();
        Assertions.assertThat(reader.visitEqual(null, true).remain()).isTrue();
    }
}
