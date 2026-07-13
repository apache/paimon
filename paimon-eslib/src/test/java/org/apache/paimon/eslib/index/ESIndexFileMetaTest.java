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

package org.apache.paimon.eslib.index;

import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.elasticsearch.eslib.api.model.VectorAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ESIndexFileMetaTest {

    @Test
    void roundTripsBuildConfigurationAndArchiveOffsets(@TempDir Path tempDir) throws Exception {
        Path first = tempDir.resolve("segments_1");
        Path second = tempDir.resolve("_0.cfs");
        Files.write(first, new byte[] {1, 2, 3});
        Files.write(second, new byte[] {4, 5});

        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("m", "32");
        parameters.put("ef_construction", "200");
        Map<String, FieldIndexConfig> configs = new LinkedHashMap<>();
        configs.put(
                "embedding",
                FieldIndexConfig.builder("embedding", FieldIndexConfig.IndexType.VECTOR)
                        .algorithm(VectorAlgorithm.HNSW)
                        .dimension(4)
                        .metric("cosine")
                        .algorithmParams(parameters)
                        .build());

        ESIndexFileMeta.Parsed parsed =
                ESIndexFileMeta.read(
                        ESIndexFileMeta.write(
                                new java.io.File[] {first.toFile(), second.toFile()}, configs));

        assertThat(parsed.hasFieldConfigs()).isTrue();
        FieldIndexConfig vector = parsed.fieldConfigs().get("embedding");
        assertThat(vector.algorithm()).isEqualTo(VectorAlgorithm.HNSW);
        assertThat(vector.dimension()).isEqualTo(4);
        assertThat(vector.metric()).isEqualTo("cosine");
        assertThat(vector.algorithmParams()).containsAllEntriesOf(parameters);

        long firstOffset = 4L + 4L + "segments_1".getBytes(StandardCharsets.UTF_8).length + 8L;
        assertThat(parsed.fileOffsets().get("segments_1")).containsExactly(firstOffset, 3L);
        long secondOffset =
                firstOffset + 3L + 4L + "_0.cfs".getBytes(StandardCharsets.UTF_8).length + 8L;
        assertThat(parsed.fileOffsets().get("_0.cfs")).containsExactly(secondOffset, 2L);
    }

    @Test
    void readsLegacyOffsetOnlyMetadata() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        byte[] name = "segments_1".getBytes(StandardCharsets.UTF_8);
        out.writeInt(1);
        out.writeInt(name.length);
        out.write(name);
        out.writeLong(17L);
        out.writeLong(23L);
        out.flush();

        ESIndexFileMeta.Parsed parsed = ESIndexFileMeta.read(bytes.toByteArray());
        assertThat(parsed.hasFieldConfigs()).isFalse();
        assertThat(parsed.fieldConfigs()).isEmpty();
        assertThat(parsed.fileOffsets().get("segments_1")).containsExactly(17L, 23L);
    }
}
