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

package org.apache.paimon.fileindex.ngram;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NgramFileIndexBenchmarkTest {

    @Test
    void benchmarkIndexBuildTime() {
        int recordCount = 100_000;
        List<String> testData = generateTestData(recordCount);

        // Warmup
        buildIndex(testData.subList(0, 1000));

        // Benchmark
        long startTime = System.nanoTime();
        byte[] indexBytes = buildIndex(testData);
        long buildTimeMs = (System.nanoTime() - startTime) / 1_000_000;

        System.out.println(
                String.format(
                        "Index Build: %d records -> %d bytes, time: %d ms (%.2f records/ms)",
                        recordCount,
                        indexBytes.length,
                        buildTimeMs,
                        (double) recordCount / buildTimeMs));
        assertThat(buildTimeMs).isLessThan(5000);
    }

    @Test
    void benchmarkIndexSize() {
        int[] recordCounts = {1_000, 10_000, 100_000};

        System.out.println("Index Size Benchmark:");
        System.out.println("Records\t\tIndex Size\tCompression");
        for (int count : recordCounts) {
            List<String> data = generateTestData(count);
            byte[] indexBytes = buildIndex(data);
            double compressionRatio = (double) indexBytes.length / (count * 20);
            System.out.println(
                    String.format(
                            "%d\t\t%d bytes\t%.2f%%",
                            count, indexBytes.length, compressionRatio * 100));
        }
    }

    @Test
    void benchmarkFilteringPerformance() {
        int recordCount = 50_000;
        List<String> testData = generateTestData(recordCount);
        byte[] indexBytes = buildIndex(testData);
        FileIndexReader reader = createReader(indexBytes);

        String[] queries = {"ap", "ba", "xy", "zz", "test", "app", "ban"};

        System.out.println("\nFiltering Performance:");
        System.out.println("Query\t\tResult\t\tTime(µs)");
        for (String query : queries) {
            long startTime = System.nanoTime();
            FileIndexReader tempReader = createReader(indexBytes);
            boolean remain =
                    tempReader.visitStartsWith(null, BinaryString.fromString(query)).remain();
            long timeUs = (System.nanoTime() - startTime) / 1_000;
            System.out.println(
                    String.format("%s\t\t%s\t\t%d", query, remain ? "REMAIN" : "SKIP", timeUs));
        }
    }

    @Test
    void benchmarkSkipRateVsCardinality() {
        System.out.println("\nSkip Rate vs Data Cardinality:");
        System.out.println("Scenario\t\t\tRecords\t\tSkip Rate");

        // Scenario 1: Low cardinality (repeating values)
        List<String> lowCard = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            lowCard.add("apple_" + (i % 100));
        }
        byte[] indexLowCard = buildIndex(lowCard);
        int skipsLowCard = countSkips(indexLowCard, new String[] {"xyz", "def", "ghi"});
        System.out.println(
                String.format(
                        "Low Cardinality (100 unique)\t10000\t\t%.1f%%",
                        (double) skipsLowCard * 100 / 3));

        // Scenario 2: High cardinality (unique values)
        List<String> highCard = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            highCard.add("prefix_" + i);
        }
        byte[] indexHighCard = buildIndex(highCard);
        int skipsHighCard = countSkips(indexHighCard, new String[] {"xyz", "def", "ghi"});
        System.out.println(
                String.format(
                        "High Cardinality (10000 unique)\t10000\t\t%.1f%%",
                        (double) skipsHighCard * 100 / 3));

        // Scenario 3: Domain-specific (emails)
        List<String> emails = new ArrayList<>();
        String[] domains = {"gmail.com", "yahoo.com", "outlook.com", "example.org"};
        for (int i = 0; i < 10_000; i++) {
            emails.add("user_" + i + "@" + domains[i % domains.length]);
        }
        byte[] indexEmails = buildIndex(emails);
        int skipsEmails =
                countSkips(indexEmails, new String[] {"@qq", "@sina", "@qq.com", "@sina.com.cn"});
        System.out.println(
                String.format(
                        "Email Domain Pattern\t\t10000\t\t%.1f%%", (double) skipsEmails * 100 / 4));
    }

    @Test
    void benchmarkMemoryUsage() {
        int recordCount = 100_000;
        List<String> testData = generateTestData(recordCount);

        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        byte[] indexBytes = buildIndex(testData);
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("\nMemory Usage:");
        System.out.println(
                String.format(
                        "Records: %d, Index Size: %d bytes, Overhead: %d MB",
                        recordCount, indexBytes.length, (memAfter - memBefore) / (1024 * 1024)));
    }

    @Test
    void benchmarkQueryPatternVariations() {
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            data.add("application_service_" + i + "_prod_server_" + (i % 100));
        }
        byte[] indexBytes = buildIndex(data);

        System.out.println("\nPattern Variation Skip Rates:");
        System.out.println("Pattern\t\t\t\tSkip?");

        String[] patterns = {
            "app",
            "application",
            "service",
            "prod",
            "server",
            "xyz",
            "notexist",
            "ap",
            "appl",
            "applicat"
        };

        for (String pattern : patterns) {
            FileIndexReader reader = createReader(indexBytes);
            boolean remain =
                    reader.visitStartsWith(null, BinaryString.fromString(pattern)).remain();
            System.out.println(String.format("%-30s\t%s", pattern, remain ? "REMAIN" : "SKIP"));
        }
    }

    private List<String> generateTestData(int count) {
        List<String> data = new ArrayList<>();
        String[] prefixes = {"apple", "banana", "application", "test", "production", "service"};
        String[] suffixes = {"_prod", "_test", "_dev", "_staging", "_backup", ""};

        for (int i = 0; i < count; i++) {
            String value = prefixes[i % prefixes.length] + "_" + i + suffixes[i % suffixes.length];
            data.add(value);
        }
        return data;
    }

    private byte[] buildIndex(List<String> data) {
        Options options = new Options();
        NgramFileIndex index = new NgramFileIndex(options);
        FileIndexWriter writer = index.createWriter();
        for (String value : data) {
            writer.write(BinaryString.fromString(value));
        }
        return writer.serializedBytes();
    }

    private FileIndexReader createReader(byte[] indexBytes) {
        Options options = new Options();
        NgramFileIndex index = new NgramFileIndex(options);
        ByteArraySeekableStream stream = new ByteArraySeekableStream(indexBytes);
        return index.createReader(stream, 0, indexBytes.length);
    }

    private int countSkips(byte[] indexBytes, String[] queries) {
        int skips = 0;
        for (String query : queries) {
            FileIndexReader reader = createReader(indexBytes);
            if (!reader.visitStartsWith(null, BinaryString.fromString(query)).remain()) {
                skips++;
            }
        }
        return skips;
    }
}
