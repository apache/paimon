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

package org.apache.flink.table.store.benchmark.file.mergetree;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.mergetree.MergeTreeReaders;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.types.RowKind;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/** Benchmark for merge tree reader. */
public class MergeTreeReaderBenchmark extends MergeTreeBenchmark {

    public static void main(String[] args) throws Exception {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + MergeTreeReaderBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        createRecordWriter();
        try {
            KeyValue kv = new KeyValue();
            Set<String> newFileNames = new HashSet<>();
            Random random = new Random();
            for (int k = 0; k < batchCount; k++) {
                for (int i = 0; i < countPerBatch; i++) {
                    GenericRowData key = new GenericRowData(1);
                    key.setField(0, random.nextInt());
                    key.setRowKind(RowKind.INSERT);

                    GenericRowData value = new GenericRowData(1);
                    value.setField(0, random.nextInt());

                    kv.replace(key, sequenceNumber++, RowKind.INSERT, value);
                    writer.write(kv);
                }

                RecordWriter.CommitIncrement increment = writer.prepareCommit(false);
                mergeCompacted(newFileNames, compactedFiles, increment);
            }

            writer.close();
        } catch (Exception e) {
            cleanUp();
            throw e;
        }
    }

    @TearDown
    public void tearDown() throws Exception {
        cleanUp();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public long scanAll() throws Exception {
        try (RecordReader<KeyValue> reader =
                MergeTreeReaders.readerForMergeTree(
                        new IntervalPartition(compactedFiles, comparator).partition(),
                        true,
                        readerFactory,
                        comparator,
                        DeduplicateMergeFunction.factory().create())) {
            long sum = 0;
            try (RecordReaderIterator<KeyValue> iterator = new RecordReaderIterator<>(reader)) {
                while (iterator.hasNext()) {
                    KeyValue kv = iterator.next();
                    sum += kv.value().getInt(0);
                }
            }
            return sum;
        }
    }
}
