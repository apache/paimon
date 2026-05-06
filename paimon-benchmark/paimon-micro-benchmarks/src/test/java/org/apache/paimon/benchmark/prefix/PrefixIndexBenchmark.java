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

package org.apache.paimon.benchmark.prefix;

import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.prefix.PrefixFileIndex;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataTypes;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/** Benchmark for {@link PrefixFileIndex}. */
public class PrefixIndexBenchmark {

    public static final int ROW_COUNT = 1000000;
    public static final int[] CARDINALITIES = new int[] {100, 1000, 10000};
    public static final int[] PREFIX_LENGTHS = new int[] {2, 3, 4};
    public static final String[] CATEGORIES =
            new String[] {"electronics", "clothing", "books", "food", "sports"};

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testQueryHit() throws Exception {
        for (int cardinality : CARDINALITIES) {
            IndexFiles files = createIndexes(cardinality);
            String existsPrefix = CATEGORIES[0] + "_" + (cardinality / 2);

            Benchmark benchmark =
                    new Benchmark(
                                    String.format(
                                            "prefix-index-query-hit-cardinality-%s", cardinality),
                                    100)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            benchmark.addCase(
                    "prefix-index-prefix2",
                    10,
                    () -> queryPrefix(files.prefixFile2, existsPrefix, 2));
            benchmark.addCase(
                    "prefix-index-prefix3",
                    10,
                    () -> queryPrefix(files.prefixFile3, existsPrefix, 3));
            benchmark.addCase(
                    "prefix-index-prefix4",
                    10,
                    () -> queryPrefix(files.prefixFile4, existsPrefix, 4));
            benchmark.addCase(
                    "bitmap-index-equal", 10, () -> queryBitmap(files.bitmapFile, existsPrefix));
            benchmark.addCase(
                    "no-index-full-scan", 10, () -> queryNoIndexScan(files.data, existsPrefix));

            benchmark.run();
        }
    }

    @Test
    public void testQuerySkip() throws Exception {
        for (int cardinality : CARDINALITIES) {
            IndexFiles files = createIndexes(cardinality);
            // A prefix that does not exist in the index
            String notExistsPrefix = "unknown_not_exists";

            Benchmark benchmark =
                    new Benchmark(
                                    String.format(
                                            "prefix-index-query-skip-cardinality-%s", cardinality),
                                    100)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            benchmark.addCase(
                    "prefix-index-prefix2",
                    10,
                    () -> queryPrefix(files.prefixFile2, notExistsPrefix, 2));
            benchmark.addCase(
                    "prefix-index-prefix3",
                    10,
                    () -> queryPrefix(files.prefixFile3, notExistsPrefix, 3));
            benchmark.addCase(
                    "prefix-index-prefix4",
                    10,
                    () -> queryPrefix(files.prefixFile4, notExistsPrefix, 4));
            benchmark.addCase(
                    "bitmap-index-equal", 10, () -> queryBitmap(files.bitmapFile, notExistsPrefix));
            benchmark.addCase(
                    "no-index-full-scan", 10, () -> queryNoIndexScan(files.data, notExistsPrefix));

            benchmark.run();
        }
    }

    @Test
    public void testIndexSize() throws Exception {
        System.out.println("\n========== Prefix Index Size Comparison ==========");
        System.out.printf(
                "%-15s %-15s %-15s %-15s %-15s %-15s%n",
                "Cardinality",
                "PrefixLen=2",
                "PrefixLen=3",
                "PrefixLen=4",
                "BitmapIndex",
                "RawData");
        System.out.println(
                "---------------------------------------------------------------------------------------------");

        for (int cardinality : CARDINALITIES) {
            IndexFiles files = createIndexes(cardinality);
            long size2 = files.prefixFile2.length();
            long size3 = files.prefixFile3.length();
            long size4 = files.prefixFile4.length();
            long bitmapSize = files.bitmapFile.length();
            long rawDataSize = estimateRawDataSize(files.data);
            System.out.printf(
                    "%-15d %-15d %-15d %-15d %-15d %-15d%n",
                    cardinality, size2, size3, size4, bitmapSize, rawDataSize);
        }
        System.out.println();
    }

    @Test
    public void testBuildTime() throws Exception {
        for (int cardinality : CARDINALITIES) {
            Benchmark benchmark =
                    new Benchmark(
                                    String.format("prefix-index-build-cardinality-%s", cardinality),
                                    ROW_COUNT)
                            .setNumWarmupIters(0)
                            .setOutputPerIteration(false);

            benchmark.addCase("prefix-index-prefix2", 5, () -> buildPrefixIndex(cardinality, 2));
            benchmark.addCase("prefix-index-prefix3", 5, () -> buildPrefixIndex(cardinality, 3));
            benchmark.addCase("prefix-index-prefix4", 5, () -> buildPrefixIndex(cardinality, 4));
            benchmark.addCase("bitmap-index", 5, () -> buildBitmapIndex(cardinality));

            benchmark.run();
        }
    }

    private IndexFiles createIndexes(int cardinality) throws IOException {
        folder.create();

        File prefixFile2 = folder.newFile("prefix-index-2-" + cardinality);
        File prefixFile3 = folder.newFile("prefix-index-3-" + cardinality);
        File prefixFile4 = folder.newFile("prefix-index-4-" + cardinality);
        File bitmapFile = folder.newFile("bitmap-index-" + cardinality);

        FileIndexWriter writer2 =
                new PrefixFileIndex(
                                DataTypes.STRING(),
                                new Options(new HashMap<String, String>()) {
                                    {
                                        setString("prefix-length", "2");
                                    }
                                })
                        .createWriter();
        FileIndexWriter writer3 =
                new PrefixFileIndex(
                                DataTypes.STRING(),
                                new Options(new HashMap<String, String>()) {
                                    {
                                        setString("prefix-length", "3");
                                    }
                                })
                        .createWriter();
        FileIndexWriter writer4 =
                new PrefixFileIndex(
                                DataTypes.STRING(),
                                new Options(new HashMap<String, String>()) {
                                    {
                                        setString("prefix-length", "4");
                                    }
                                })
                        .createWriter();
        FileIndexWriter bitmapWriter =
                new BitmapFileIndex(DataTypes.STRING(), new Options()).createWriter();

        String[] data = new String[ROW_COUNT];
        Random random = new Random(42);
        for (int i = 0; i < ROW_COUNT; i++) {
            String value =
                    CATEGORIES[random.nextInt(CATEGORIES.length)]
                            + "_"
                            + random.nextInt(cardinality);
            data[i] = value;
            writer2.write(BinaryString.fromString(value));
            writer3.write(BinaryString.fromString(value));
            writer4.write(BinaryString.fromString(value));
            bitmapWriter.write(BinaryString.fromString(value));
        }

        FileUtils.writeByteArrayToFile(prefixFile2, writer2.serializedBytes());
        FileUtils.writeByteArrayToFile(prefixFile3, writer3.serializedBytes());
        FileUtils.writeByteArrayToFile(prefixFile4, writer4.serializedBytes());
        FileUtils.writeByteArrayToFile(bitmapFile, bitmapWriter.serializedBytes());

        return new IndexFiles(prefixFile2, prefixFile3, prefixFile4, bitmapFile, data);
    }

    private void buildPrefixIndex(int cardinality, int prefixLength) {
        try {
            FileIndexWriter writer =
                    new PrefixFileIndex(
                                    DataTypes.STRING(),
                                    new Options(new HashMap<String, String>()) {
                                        {
                                            setString(
                                                    "prefix-length", String.valueOf(prefixLength));
                                        }
                                    })
                            .createWriter();
            Random random = new Random(42);
            for (int i = 0; i < ROW_COUNT; i++) {
                writer.write(
                        BinaryString.fromString(
                                CATEGORIES[random.nextInt(CATEGORIES.length)]
                                        + "_"
                                        + random.nextInt(cardinality)));
            }
            writer.serializedBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void buildBitmapIndex(int cardinality) {
        try {
            FileIndexWriter writer =
                    new BitmapFileIndex(DataTypes.STRING(), new Options()).createWriter();
            Random random = new Random(42);
            for (int i = 0; i < ROW_COUNT; i++) {
                writer.write(
                        BinaryString.fromString(
                                CATEGORIES[random.nextInt(CATEGORIES.length)]
                                        + "_"
                                        + random.nextInt(cardinality)));
            }
            writer.serializedBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void queryPrefix(File indexFile, String prefix, int prefixLength) {
        try {
            FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
            Options options =
                    new Options(new HashMap<String, String>()) {
                        {
                            setString("prefix-length", String.valueOf(prefixLength));
                        }
                    };
            LocalFileIO.LocalSeekableInputStream stream =
                    new LocalFileIO.LocalSeekableInputStream(indexFile);
            FileIndexReader reader =
                    new PrefixFileIndex(DataTypes.STRING(), options)
                            .createReader(stream, 0, (int) indexFile.length());
            reader.visitStartsWith(fieldRef, BinaryString.fromString(prefix));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void queryBitmap(File indexFile, String value) {
        try {
            FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
            Options options = new Options();
            LocalFileIO.LocalSeekableInputStream stream =
                    new LocalFileIO.LocalSeekableInputStream(indexFile);
            FileIndexReader reader =
                    new BitmapFileIndex(DataTypes.STRING(), options)
                            .createReader(stream, 0, (int) indexFile.length());
            ((BitmapIndexResult) reader.visitEqual(fieldRef, BinaryString.fromString(value))).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void queryNoIndexScan(String[] data, String prefix) {
        boolean found = false;
        for (String value : data) {
            if (value.startsWith(prefix)) {
                found = true;
                break;
            }
        }
        // Return the result for file pruning decision (REMAIN if found, SKIP if not)
        // We don't throw here because skip scenarios intentionally query non-existing prefixes
        boolean remain = found;
        // Prevent the JVM from optimizing away the result
        if (remain && System.nanoTime() == 0) {
            throw new RuntimeException("Unreachable");
        }
    }

    private static long estimateRawDataSize(String[] data) {
        long size = 0;
        for (String s : data) {
            size += s.getBytes().length + 4; // 4 bytes for length prefix
        }
        return size;
    }

    private static class IndexFiles {
        final File prefixFile2;
        final File prefixFile3;
        final File prefixFile4;
        final File bitmapFile;
        final String[] data;

        IndexFiles(
                File prefixFile2,
                File prefixFile3,
                File prefixFile4,
                File bitmapFile,
                String[] data) {
            this.prefixFile2 = prefixFile2;
            this.prefixFile3 = prefixFile3;
            this.prefixFile4 = prefixFile4;
            this.bitmapFile = bitmapFile;
            this.data = data;
        }
    }
}
