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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFileTestDataGenerator;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.RepeatedTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Serialization test for {@link PartitionedManifestMeta}. */
public class PartitionedManifestMetaSerializationTest {

    @RepeatedTest(10)
    public void testJsonSerDe() throws IOException, ClassNotFoundException {
        PartitionedManifestMeta partitionedMeta =
                new PartitionedManifestMeta(new Random().nextLong(), genManifestEntries());
        String serialized =
                Base64.getEncoder()
                        .encodeToString(InstantiationUtil.serializeObject(partitionedMeta));
        Object deserialized =
                InstantiationUtil.deserializeObject(
                        Base64.getDecoder().decode(serialized),
                        Thread.currentThread().getContextClassLoader());
        assertThat(deserialized).isEqualTo(partitionedMeta);
    }

    private static Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> genManifestEntries() {
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> manifestEntries = new HashMap<>();
        DataFileTestDataGenerator gen =
                DataFileTestDataGenerator.builder()
                        .numBuckets(new Random().nextInt(16) + 1)
                        .build();
        IntStream.range(0, new Random().nextInt(1000))
                .boxed()
                .map(i -> gen.next())
                .forEach(
                        data ->
                                manifestEntries
                                        .computeIfAbsent(data.partition, entry -> new HashMap<>())
                                        .computeIfAbsent(data.bucket, entry -> new ArrayList<>())
                                        .add(data.meta));
        return manifestEntries;
    }
}
