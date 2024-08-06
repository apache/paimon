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
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.LOOKUP_LOCAL_FILE_TYPE;

/** Benchmark for measuring the throughput of writing for lookup. */
@ExtendWith(ParameterizedTestExtension.class)
public class LookupWriterBenchmark extends AbstractLookupBenchmark {

    private final int recordCount;
    @TempDir Path tempDir;

    public LookupWriterBenchmark(int recordCount) {
        this.recordCount = recordCount;
    }

    @Parameters(name = "record-count-{0}")
    public static List<Integer> getVarSeg() {
        return Arrays.asList(1000000, 5000000, 10000000, 15000000, 20000000);
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
                                writeData(options, inputs, valueLength, sameValue);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }

        benchmark.run();
    }

    private void writeData(CoreOptions options, byte[][] inputs, int valueLength, boolean sameValue)
            throws IOException {
        byte[] value1 = new byte[valueLength];
        byte[] value2 = new byte[valueLength];
        Arrays.fill(value1, (byte) 1);
        Arrays.fill(value2, (byte) 2);
        LookupStoreFactory factory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(MemorySize.ofMebiBytes(10)),
                        new RowCompactedSerializer(RowType.of(new IntType()))
                                .createSliceComparator());

        File file = new File(tempDir.toFile(), UUID.randomUUID().toString());
        LookupStoreWriter writer = factory.createWriter(file, null);
        boolean first = true;
        for (byte[] input : inputs) {
            if (first) {
                writer.put(input, value1);
            } else {
                writer.put(input, value2);
            }
            if (!sameValue) {
                first = !first;
            }
        }
        writer.close();
    }
}
