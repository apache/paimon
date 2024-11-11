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

package org.apache.paimon.benchmark.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.LOOKUP_LOCAL_FILE_TYPE;

/** Benchmark for measuring the throughput of writing for lookup. */
@ExtendWith(ParameterizedTestExtension.class)
public class LookupWriterBenchmark extends AbstractLookupBenchmark {
    private final int recordCount;
    private final boolean bloomFilterEnabled;
    @TempDir Path tempDir;

    public LookupWriterBenchmark(List<Object> countBloomList) {
        this.recordCount = (Integer) countBloomList.get(0);
        this.bloomFilterEnabled = (Boolean) countBloomList.get(1);
    }

    @Parameters(name = "countBloom-{0}")
    public static List<List<Object>> getVarSeg() {
        return getCountBloomList();
    }

    @TestTemplate
    void testLookupWriterSameValue() {
        writeLookupDataBenchmark(generateSequenceInputs(0, recordCount), true);
    }

    @TestTemplate
    void testLookupWriterDiffValue() {
        writeLookupDataBenchmark(generateSequenceInputs(0, recordCount), false);
    }

    private void writeLookupDataBenchmark(byte[][] inputs, boolean sameValue) {
        Benchmark benchmark =
                new Benchmark("writer-" + inputs.length, inputs.length)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        for (int valueLength : VALUE_LENGTHS) {
            for (CoreOptions.LookupLocalFileType fileType :
                    CoreOptions.LookupLocalFileType.values()) {
                CoreOptions options =
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        LOOKUP_LOCAL_FILE_TYPE.key(), fileType.name()));
                benchmark.addCase(
                        String.format(
                                "%s-write-%dB-value-%d-num",
                                fileType.name(), valueLength, inputs.length),
                        5,
                        () -> {
                            try {
                                writeData(
                                        tempDir,
                                        options,
                                        inputs,
                                        valueLength,
                                        sameValue,
                                        bloomFilterEnabled);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }

        benchmark.run();
    }
}
