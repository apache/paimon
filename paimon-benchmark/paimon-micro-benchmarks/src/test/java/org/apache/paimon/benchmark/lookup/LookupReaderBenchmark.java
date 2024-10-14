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
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.LOOKUP_LOCAL_FILE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Benchmark for measuring the throughput of writing for lookup. */
@ExtendWith(ParameterizedTestExtension.class)
public class LookupReaderBenchmark extends AbstractLookupBenchmark {
    private static final int QUERY_KEY_COUNT = 10000;
    private final int recordCount;
    private final boolean bloomFilterEnabled;
    @TempDir Path tempDir;

    public LookupReaderBenchmark(List<Object> countBloomList) {
        this.recordCount = (Integer) countBloomList.get(0);
        this.bloomFilterEnabled = (Boolean) countBloomList.get(1);
    }

    @Parameters(name = "countBloom-{0}")
    public static List<List<Object>> getVarSeg() {
        return getCountBloomList();
    }

    @TestTemplate
    void testLookupReader() throws IOException {
        readLookupDataBenchmark(
                generateSequenceInputs(0, recordCount),
                generateRandomInputs(0, recordCount, QUERY_KEY_COUNT));
    }

    private void readLookupDataBenchmark(byte[][] inputs, byte[][] randomInputs)
            throws IOException {
        Benchmark benchmark =
                new Benchmark("reader-" + randomInputs.length, randomInputs.length)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        for (int valueLength : VALUE_LENGTHS) {
            for (CoreOptions.LookupLocalFileType fileType :
                    CoreOptions.LookupLocalFileType.values()) {
                CoreOptions options =
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        LOOKUP_LOCAL_FILE_TYPE.key(), fileType.name()));
                Pair<String, LookupStoreFactory.Context> pair =
                        writeData(tempDir, options, inputs, valueLength, false, bloomFilterEnabled);
                benchmark.addCase(
                        String.format(
                                "%s-read-%dB-value-%d-num",
                                fileType.name(), valueLength, randomInputs.length),
                        5,
                        () -> {
                            try {
                                readData(options, randomInputs, pair.getLeft(), pair.getRight());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }

        benchmark.run();
    }

    private void readData(
            CoreOptions options,
            byte[][] randomInputs,
            String filePath,
            LookupStoreFactory.Context context)
            throws IOException {
        LookupStoreFactory factory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(MemorySize.ofMebiBytes(10)),
                        new RowCompactedSerializer(RowType.of(new IntType()))
                                .createSliceComparator());

        File file = new File(filePath);
        LookupStoreReader reader = factory.createReader(file, context);
        for (byte[] input : randomInputs) {
            assertThat(reader.lookup(input)).isNotNull();
        }
        reader.close();
    }
}
