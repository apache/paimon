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

package org.apache.paimon.manifest;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.paimon.data.BinaryArray.fromLongArray;
import static org.apache.paimon.data.BinaryRow.singleColumn;
import static org.assertj.core.api.Assertions.assertThat;

/** Compatibility Test for {@link ManifestCommittableSerializer}. */
public class ManifestCommittableSerializerCompatibilityTest {

    @Test
    public void testProduction() throws IOException {
        BinaryTableStats keyStats =
                new BinaryTableStats(
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        fromLongArray(new Long[] {0L}));
        BinaryTableStats valueStats =
                new BinaryTableStats(
                        singleColumn("min_value"),
                        singleColumn("max_value"),
                        fromLongArray(new Long[] {0L}));
        DataFileMeta dataFile =
                new DataFileMeta(
                        "my_file",
                        1024 * 1024,
                        1024,
                        singleColumn("min_key"),
                        singleColumn("max_key"),
                        keyStats,
                        valueStats,
                        15,
                        200,
                        5,
                        3,
                        Arrays.asList("extra1", "extra2"),
                        Timestamp.fromLocalDateTime(LocalDateTime.parse("2022-03-02T20:20:12")),
                        11L,
                        new byte[] {1, 2, 4});
        List<DataFileMeta> dataFiles = Collections.singletonList(dataFile);

        LinkedHashMap<String, Pair<Integer, Integer>> dvRanges = new LinkedHashMap<>();
        dvRanges.put("dv_key1", Pair.of(1, 2));
        dvRanges.put("dv_key2", Pair.of(3, 4));
        IndexFileMeta indexFile =
                new IndexFileMeta("my_index_type", "my_index_file", 1024 * 100, 1002, dvRanges);
        List<IndexFileMeta> indexFiles = Collections.singletonList(indexFile);

        CommitMessageImpl commitMessage =
                new CommitMessageImpl(
                        singleColumn("my_partition"),
                        11,
                        new DataIncrement(dataFiles, dataFiles, dataFiles),
                        new CompactIncrement(dataFiles, dataFiles, dataFiles),
                        new IndexIncrement(indexFiles));

        ManifestCommittable manifestCommittable =
                new ManifestCommittable(
                        5,
                        202020L,
                        Collections.singletonMap(5, 555L),
                        Collections.singletonList(commitMessage));

        ManifestCommittableSerializer serializer = new ManifestCommittableSerializer();
        byte[] bytes = serializer.serialize(manifestCommittable);
        ManifestCommittable deserialized = serializer.deserialize(2, bytes);
        assertThat(deserialized).isEqualTo(manifestCommittable);
    }
}
