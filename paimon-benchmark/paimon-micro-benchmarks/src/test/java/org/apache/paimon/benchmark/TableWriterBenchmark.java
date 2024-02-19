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
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Benchmark for table writer. */
public class TableWriterBenchmark extends TableBenchmark {

    @Test
    public void testAvro() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.AVRO);
        innerTest("avro", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * avro:            Best/Avg Time(ms)    Row Rate(M/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * avro_write        5847 / 7296              0.1          19489.5       1.0X
         */
    }

    @Test
    public void testAvroWithoutStats() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.AVRO);
        options.set(CoreOptions.METADATA_STATS_MODE, "none");
        innerTest("avro", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * avro:            Best/Avg Time(ms)    Row Rate(M/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * avro_write        4701 / 5780              0.1          15669.6       1.0X
         */
    }

    @Test
    public void testOrcNoCompression() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.ORC);
        options.set("orc.compress", "none");
        innerTest("orc", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * orc:            Best/Avg Time(ms)    Row Rate(M/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * orc_write        8448 / 9584              0.0          28160.1       1.0X
         */
    }

    @Test
    public void testParquet() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.PARQUET);
        innerTest("parquet", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * parquet:            Best/Avg Time(ms)    Row Rate(M/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * parquet_write       10872 / 12566              0.0          36239.7       1.0X
         */
    }

    @Test
    public void testOrc() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.ORC);
        innerTest("orc", options);
        /*
         * Java HotSpot(TM) 64-Bit Server VM 1.8.0_301-b09 on Mac OS X 10.16
         * Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
         * orc:               Best/Avg Time(ms)    Row Rate(M/s)      Per Row(ns)   Relative
         * ---------------------------------------------------------------------------------
         * orc_write           8690 / 9771              0.0          28968.0       1.0X
         */
    }

    public void innerTest(String name, Options options) throws Exception {
        StreamWriteBuilder writeBuilder = createTable(options, "T").newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();
        long valuesPerIteration = 300_000;
        Benchmark benchmark =
                new Benchmark(name, valuesPerIteration)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        AtomicInteger writeCount = new AtomicInteger(0);
        AtomicInteger commitIdentifier = new AtomicInteger(0);
        benchmark.addCase(
                "write",
                5,
                () -> {
                    for (int i = 0; i < valuesPerIteration; i++) {
                        try {
                            write.write(newRandomRow());
                            writeCount.incrementAndGet();
                            if (writeCount.get() % 10_000 == 0) {
                                List<CommitMessage> commitMessages =
                                        write.prepareCommit(false, commitIdentifier.get());
                                commit.commit(commitIdentifier.get(), commitMessages);
                                commitIdentifier.incrementAndGet();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        benchmark.run();
        write.close();
    }
}
