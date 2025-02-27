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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;

/** Benchmark for {@link BitmapFileIndex}. */
public class BitmapIndexBenchmark {

    public static final int ROW_COUNT = 1000000;

    private static final String prefix = "asdfghjkl";

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testQuery10() throws Exception {
        testQuery(10);
    }

    @Test
    public void testQuery100() throws Exception {
        testQuery(100);
    }

    @Test
    public void testQuery1000() throws Exception {
        testQuery(1000);
    }

    @Test
    public void testQuery10000() throws Exception {
        testQuery(10000);
    }

    @Test
    public void testQuery30000() throws Exception {
        testQuery(30000);
    }

    @Test
    public void testQuery50000() throws Exception {
        testQuery(50000);
    }

    @Test
    public void testQuery80000() throws Exception {
        testQuery(80000);
    }

    @Test
    public void testQuery100000() throws Exception {
        testQuery(100000);
    }

    private void testQuery(int approxCardinality) throws Exception {

        RoaringBitmap32 middleBm = new RoaringBitmap32();

        Options writeOptions1 = new Options();
        writeOptions1.setInteger(BitmapFileIndex.VERSION, BitmapFileIndex.VERSION_1);
        FileIndexWriter writer1 =
                new BitmapFileIndex(DataTypes.STRING(), writeOptions1).createWriter();

        Options writeOptions2 = new Options();
        writeOptions2.setInteger(BitmapFileIndex.VERSION, BitmapFileIndex.VERSION_2);
        FileIndexWriter writer2 =
                new BitmapFileIndex(DataTypes.STRING(), writeOptions2).createWriter();

        for (int i = 0; i < ROW_COUNT; i++) {
            int sid = (int) (Math.random() * approxCardinality);
            if (sid == approxCardinality / 2) {
                middleBm.add(i);
            }
            writer1.write(BinaryString.fromString(prefix + sid));
            writer2.write(BinaryString.fromString(prefix + sid));
        }

        folder.create();

        File file1 = folder.newFile("bitmap-index-v1");
        File file2 = folder.newFile("bitmap-index-v2");
        FileUtils.writeByteArrayToFile(file1, writer1.serializedBytes());
        FileUtils.writeByteArrayToFile(file2, writer2.serializedBytes());

        Benchmark benchmark =
                new Benchmark(
                                String.format("bitmap-index-query-benchmark-%d", approxCardinality),
                                100)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "formatV1-bufferedInput-bitmapByteBuffer",
                10,
                () -> query(approxCardinality, file1));

        benchmark.addCase(
                "format-v2-bufferedInput-bitmapByteBuffer",
                10,
                () -> query(approxCardinality, file2));

        benchmark.run();
    }

    private static void query(int approxCardinality, File file1) {
        try {
            FieldRef fieldRef = new FieldRef(0, "", DataTypes.STRING());
            Options options = new Options();
            LocalFileIO.LocalSeekableInputStream localSeekableInputStream =
                    new LocalFileIO.LocalSeekableInputStream(file1);
            FileIndexReader reader =
                    new BitmapFileIndex(DataTypes.STRING(), options)
                            .createReader(localSeekableInputStream, 0, 0);
            FileIndexResult result =
                    reader.visitEqual(
                            fieldRef, BinaryString.fromString(prefix + (approxCardinality / 2)));
            ((BitmapIndexResult) result).get();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
