/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.ReusingTestData;
import org.apache.flink.table.store.file.utils.TestReusingRecordReader;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.store.file.mergetree.compact.CombiningRecordReaderTestBase.KEY_COMPARATOR;

/** Benchmark for {@link org.apache.flink.table.store.file.mergetree.compact.SortMergeReader}. */
public class SortMergeReaderBenchmark {

    /** Benchmark test scenarios for {@link SortMergeReader}. */
    @State(Scope.Benchmark)
    public static class StateBaseline {
        @Param({"20", "50", "100"})
        int maxNumReaders;

        @Param({ "10", "50", "100"})
        int maxNumKeys;

        List<List<ReusingTestData>> benchmarkData = new ArrayList<>();
    }

    /** Tests for {@link SortMergeReader}. */
    @BenchmarkMode(Mode.AverageTime)
    @Fork(10)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public abstract static class AbstractSortMergeReaderBenchmark {

        @Setup(Level.Trial)
        public void setup(StateBaseline stateBaseline) {
            if (stateBaseline.benchmarkData.size() == 0) {
                Random random = new Random();
                int numReaders = random.nextInt(stateBaseline.maxNumReaders) + 1;
                for (int i = 0; i < numReaders; i++) {
                    stateBaseline.benchmarkData.add(
                            ReusingTestData.generateOrderedNoDuplicatedKeys(
                                    random.nextInt(stateBaseline.maxNumKeys * 10000) + 1,
                                    addOnly()));
                }
            }
        }

        @Benchmark
        @Group("SortMergeReaderBenchmark")
        @Warmup(iterations = 3, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        @Measurement(iterations = 10, time = 100, timeUnit = TimeUnit.MILLISECONDS)
        public void sortMergeReaderAfter(StateBaseline stateBaseline, Blackhole blackhole)
                throws IOException {

            List<TestReusingRecordReader> readers = new ArrayList<>();
            for (List<ReusingTestData> readerData : stateBaseline.benchmarkData) {
                readers.add(new TestReusingRecordReader(readerData));
            }
            RecordReader<KeyValue> recordReader =
                    new SortMergeReader(
                            new ArrayList<>(readers), KEY_COMPARATOR, createMergeFunction());

            RecordReader.RecordIterator<KeyValue> batch;
            while ((batch = recordReader.readBatch()) != null) {
                KeyValue kv;
                while ((kv = batch.next()) != null) {
                    blackhole.consume(kv);
                }

                batch.releaseBatch();
            }
            recordReader.close();

            for (TestReusingRecordReader reader : readers) {
                reader.assertCleanUp();
            }
        }

        protected abstract MergeFunction createMergeFunction();

        protected abstract boolean addOnly();

        protected RecordReader<KeyValue> createRecordReader(List<TestReusingRecordReader> readers) {
            return new SortMergeReader(
                    new ArrayList<>(readers), KEY_COMPARATOR, createMergeFunction());
        }
    }

    /** Tests for {@link SortMergeReader} with {@link DeduplicateMergeFunction}. */
    public static class WithDeduplicateMergeFunction extends AbstractSortMergeReaderBenchmark {

        @Override
        protected MergeFunction createMergeFunction() {
            return new DeduplicateMergeFunction();
        }

        @Override
        protected boolean addOnly() {
            return false;
        }
    }

    /** Tests for {@link SortMergeReader} with {@link ValueCountMergeFunction}. */
    public static class WithValueRecordMergeFunctionTest extends AbstractSortMergeReaderBenchmark {

        @Override
        protected MergeFunction createMergeFunction() {
            return new ValueCountMergeFunction();
        }

        @Override
        protected boolean addOnly() {
            return true;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(SortMergeReaderBenchmark.class.getSimpleName())
                        .threads(1)
                        .syncIterations(false)
                        .build();

        new Runner(opt).run();
    }
}
