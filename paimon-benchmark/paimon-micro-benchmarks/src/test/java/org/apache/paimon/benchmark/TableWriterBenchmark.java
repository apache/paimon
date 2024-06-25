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

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/** Benchmark for table writer. */
public class TableWriterBenchmark extends TableBenchmark {

    @Test
    public void testAvro() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        innerTest("avro", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * avro:            Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * avro_write        40309 / 41161             74.4          13436.3       1.0X
         */
    }

    @Test
    public void testAvroWithoutStats() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        options.set(CoreOptions.METADATA_STATS_MODE, "none");
        innerTest("avro", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * avro:            Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * avro_write        31817 / 32359             94.3          10605.6       1.0X
         */
    }

    @Test
    public void testOrcNoCompression() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_ORC);
        options.set("orc.compress", "none");
        innerTest("orc", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * orc:            Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * orc_write        32751 / 33032             91.6          10917.0       1.0X
         */
    }

    @Test
    public void testParquet() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        innerTest("parquet", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * parquet:            Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * parquet_write       46279 / 46715             64.8          15426.3       1.0X
         */
    }

    @Test
    public void testOrc() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_ORC);
        innerTest("orc", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * orc:               Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * orc_write           31812 / 32223             94.3          10604.1       1.0X
         */
    }

    public void innerTest(String name, Options options) throws Exception {
        options.set(CoreOptions.BUCKET, 1);
        Table table = createTable(options, "T");
        long valuesPerIteration = 3_000_000;
        Benchmark benchmark =
                new Benchmark(name, valuesPerIteration)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        AtomicInteger writeCount = new AtomicInteger(0);
        benchmark.addCase(
                "write",
                3,
                () -> {
                    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
                    BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit();
                    for (int i = 0; i < valuesPerIteration; i++) {
                        try {
                            write.write(newRandomRow());
                            writeCount.incrementAndGet();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    try {
                        commit.commit(write.prepareCommit());
                        writeBuilder.newCommit().truncateTable();
                        write.close();
                        commit.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        benchmark.run();
    }
}
