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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.Split;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Benchmark for table read. */
public class TableFormatBenchmark extends TableBenchmark {

    private final int rowCount = 10000000;

    @Test
    public void testRead() throws Exception {
        innerTest(prepareData());
        /*
         * OpenJDK 64-Bit Server VM 1.8.0_292-b10 on Mac OS X 10.16
         * Apple M1 Pro
         * read:                            Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ------------------------------------------------------------------------------------------------
         * OPERATORTEST_read_read-orc           11314 / 11366           2651.6            377.1       1.0X
         */
    }

    private void innerTest(Table table) {
        int readTime = 3;
        Benchmark benchmark =
                new Benchmark("read", readTime * rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        benchmark.addCase(
                "read",
                5,
                () -> {
                    for (int i = 0; i < readTime; i++) {
                        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                        AtomicLong readCount = new AtomicLong(0);
                        try {
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        table.newReadBuilder().newRead().createReader(split);
                                reader.forEachRemaining(row -> readCount.incrementAndGet());
                            }
                            System.out.printf("Finish read %d rows.\n", readCount.get());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        benchmark.run();
    }

    private Table prepareData() throws Exception {
        Table table = createTable(new Options(), "table", Collections.emptyList());
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();
        AtomicInteger writeCount = new AtomicInteger(0);
        for (int i = 0; i < rowCount; i++) {
            try {
                write.write(newRandomRow());
                writeCount.incrementAndGet();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        List<CommitMessage> commitMessages = write.prepareCommit(true, 1);
        commit.commit(1, commitMessages);

        write.close();
        return table;
    }
}
