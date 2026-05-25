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
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.bsi.BitSliceIndexBitmapFileIndex;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndex;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataTypes;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;
import java.util.function.BiFunction;

/** Benchmark for {@link RangeBitmapFileIndex}. */
public class RangeBitmapIndexBenchmark {

    public static final int ROW_COUNT = 1000000;
    public static final int[] BOUNDS =
            new int[] {3000, 10000, 50000, 100000, 300000, 500000, 800000, 3000000};
    public static final int NOT_EXISTS_PREDICATE = 123;

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private File bsiFile;
    private File bitmapFile;
    private File rangebitmapFile;

    @Test
    public void testEQ() throws Exception {
        for (int bound : BOUNDS) {
            createIndex(bound);
            Random random = new Random();
            int value;
            do {
                value = random.nextInt(bound);
            } while (value == NOT_EXISTS_PREDICATE);

            int predicate = value;
            Benchmark benchmark =
                    new Benchmark(String.format("index-query-benchmark-%s", bound), 100)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            BiFunction<FileIndexReader, FieldRef, FileIndexResult> func =
                    (reader, field) -> reader.visitEqual(field, predicate);

            benchmark.addCase("bsi-index", 10, () -> queryBsi(bsiFile, func));

            benchmark.addCase("bitmap-index", 10, () -> queryBitmap(bitmapFile, func));

            benchmark.addCase(
                    "range-bitmap-index", 10, () -> queryRangeBitmap(rangebitmapFile, func));

            benchmark.run();
        }
    }

    @Test
    public void testNotExists() throws Exception {
        for (int bound : BOUNDS) {
            createIndex(bound);

            Benchmark benchmark =
                    new Benchmark(String.format("index-query-benchmark-%s", bound), 100)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            BiFunction<FileIndexReader, FieldRef, FileIndexResult> func =
                    (reader, field) -> reader.visitEqual(field, NOT_EXISTS_PREDICATE);

            benchmark.addCase("bsi-index", 10, () -> queryBsi(bsiFile, func));

            benchmark.addCase("bitmap-index", 10, () -> queryBitmap(bitmapFile, func));

            benchmark.addCase(
                    "range-bitmap-index", 10, () -> queryRangeBitmap(rangebitmapFile, func));

            benchmark.run();
        }
    }

    @Test
    public void testRange() throws Exception {
        for (int bound : BOUNDS) {
            createIndex(bound);
            Random random = new Random();
            int predicate = random.nextInt(bound);

            Benchmark benchmark =
                    new Benchmark(String.format("index-query-benchmark-%s", bound), 100)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            BiFunction<FileIndexReader, FieldRef, FileIndexResult> func =
                    (reader, field) -> reader.visitGreaterThan(field, predicate);

            benchmark.addCase("bsi-index", 10, () -> queryBsi(bsiFile, func));

            benchmark.addCase(
                    "range-bitmap-index", 10, () -> queryRangeBitmap(rangebitmapFile, func));

            benchmark.run();
        }
    }

    @Test
    public void testIsNotNull() throws IOException {
        for (int bound : BOUNDS) {
            createIndex(bound);

            Benchmark benchmark =
                    new Benchmark(String.format("index-query-benchmark-%s", bound), 100)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            BiFunction<FileIndexReader, FieldRef, FileIndexResult> func =
                    FileIndexReader::visitIsNotNull;

            benchmark.addCase("bsi-index", 10, () -> queryBsi(bsiFile, func));

            benchmark.addCase("bitmap-index", 10, () -> queryBitmap(bitmapFile, func));

            benchmark.addCase(
                    "range-bitmap-index", 10, () -> queryRangeBitmap(rangebitmapFile, func));

            benchmark.run();
        }
    }

    private void createIndex(int bound) throws IOException {
        Random random = new Random();

        FileIndexWriter bsiWriter =
                new BitSliceIndexBitmapFileIndex(DataTypes.INT()).createWriter();

        Options bitmapOptions = new Options();
        bitmapOptions.setInteger(BitmapFileIndex.VERSION, BitmapFileIndex.VERSION_2);
        FileIndexWriter bitmapWriter =
                new BitmapFileIndex(DataTypes.INT(), bitmapOptions).createWriter();

        FileIndexWriter rangeWriter =
                new RangeBitmapFileIndex(DataTypes.INT(), new Options()).createWriter();

        for (int i = 0; i < ROW_COUNT; i++) {
            Integer next;
            if (random.nextInt(10) == 0) {
                next = null;
            } else {
                next = random.nextInt(bound);
                if (next == NOT_EXISTS_PREDICATE) {
                    next = null;
                }
            }
            bsiWriter.writeRecord(next);
            bitmapWriter.writeRecord(next);
            rangeWriter.writeRecord(next);
        }

        folder.create();

        this.bsiFile = folder.newFile("bsi-index");
        this.bitmapFile = folder.newFile("bitmap-index");
        this.rangebitmapFile = folder.newFile("range-bitmap-index");

        FileUtils.writeByteArrayToFile(bsiFile, bsiWriter.serializedBytes());
        FileUtils.writeByteArrayToFile(bitmapFile, bitmapWriter.serializedBytes());
        FileUtils.writeByteArrayToFile(rangebitmapFile, rangeWriter.serializedBytes());

        List<File> files = Arrays.asList(bsiFile, bitmapFile, rangebitmapFile);
        files.sort(Comparator.comparingLong(File::length));
        StringJoiner joiner = new StringJoiner(" < ");
        files.forEach(
                file ->
                        joiner.add(
                                String.format("%s(%s kb)", file.getName(), file.length() / 1024)));
        System.out.println("file size: " + joiner);
    }

    private void queryBsi(
            File file, BiFunction<FileIndexReader, FieldRef, FileIndexResult> function) {
        try {
            FieldRef fieldRef = new FieldRef(0, "", DataTypes.INT());
            Options options = new Options();
            LocalFileIO.LocalSeekableInputStream localSeekableInputStream =
                    new LocalFileIO.LocalSeekableInputStream(file);
            FileIndexReader reader =
                    new BitSliceIndexBitmapFileIndex(DataTypes.INT())
                            .createReader(localSeekableInputStream, 0, 0);
            FileIndexResult result = function.apply(reader, fieldRef);
            ((BitmapIndexResult) result).get();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void queryBitmap(
            File file, BiFunction<FileIndexReader, FieldRef, FileIndexResult> function) {
        try {
            FieldRef fieldRef = new FieldRef(0, "", DataTypes.INT());
            Options options = new Options();
            LocalFileIO.LocalSeekableInputStream localSeekableInputStream =
                    new LocalFileIO.LocalSeekableInputStream(file);
            FileIndexReader reader =
                    new BitmapFileIndex(DataTypes.INT(), options)
                            .createReader(localSeekableInputStream, 0, 0);
            FileIndexResult result = function.apply(reader, fieldRef);
            ((BitmapIndexResult) result).get();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void queryRangeBitmap(
            File file, BiFunction<FileIndexReader, FieldRef, FileIndexResult> function) {
        try {
            FieldRef fieldRef = new FieldRef(0, "", DataTypes.INT());
            Options options = new Options();
            LocalFileIO.LocalSeekableInputStream localSeekableInputStream =
                    new LocalFileIO.LocalSeekableInputStream(file);
            FileIndexReader reader =
                    new RangeBitmapFileIndex(DataTypes.INT(), options)
                            .createReader(localSeekableInputStream, 0, 0);
            FileIndexResult result = function.apply(reader, fieldRef);
            ((BitmapIndexResult) result).get();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
