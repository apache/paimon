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

package org.apache.flink.table.store.benchmark.mergetree;

import org.apache.flink.table.store.benchmark.Benchmark;
import org.apache.flink.table.store.table.sink.CommitMessage;
import org.apache.flink.table.store.table.sink.StreamTableCommit;
import org.apache.flink.table.store.table.sink.StreamTableWrite;
import org.apache.flink.table.store.table.sink.StreamWriteBuilder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Benchmark for merge tree writer with compaction. */
public class MergeTreeWriterBenchmark extends MergeTreeBenchmark {

    private StreamTableWrite write;

    private StreamTableCommit commit;

    private long writeCount = 0;

    private long commitIdentifier = 0;

    @BeforeEach
    public void beforeEach() throws Exception {
        super.beforeEach();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
    }

    @AfterEach
    public void afterEach() throws Exception {
        write.close();
    }

    @Test
    public void testWrite() {
        long valuesPerIteration = 1_000_000;
        Benchmark benchmark =
                new Benchmark("mergetree-write", valuesPerIteration)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        benchmark.addCase(
                "write",
                5,
                () -> {
                    for (int i = 0; i < valuesPerIteration; i++) {
                        write();
                    }
                });
        benchmark.run();
    }

    private void write() {
        try {
            write.write(newRandomRow());
            writeCount++;
            if (writeCount % 10_000 == 0) {
                List<CommitMessage> commitMessages = write.prepareCommit(false, commitIdentifier);
                commit.commit(commitIdentifier, commitMessages);
                commitIdentifier++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
