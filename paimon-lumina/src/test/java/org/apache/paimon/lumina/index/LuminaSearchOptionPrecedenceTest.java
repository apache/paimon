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

package org.apache.paimon.lumina.index;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.lumina.index.LuminaVectorGlobalIndexReader.mergeOptions;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A search resolves its options from three sources, and the index metadata has to win over the
 * current table configuration: the metadata records what the index was built with, while the table
 * value may have been changed since. These pin that a build-time option left stale or invalid on
 * the table cannot reach the searcher, and therefore cannot be validated against on the read path.
 */
class LuminaSearchOptionPrecedenceTest {

    @Test
    void indexMetadataWinsOverAStaleTableValue() {
        // The table says pq.m = 0 and leaves the dimension at its default; the index on disk was
        // built with 8 chunks over 256 dimensions.
        Map<String, String> tableOptions =
                new LuminaVectorIndexOptions(
                                Options.fromMap(
                                        mapOf(
                                                "lumina.encoding.type", "pq",
                                                "lumina.encoding.pq.m", "0",
                                                "lumina.index.dimension", "128")))
                        .toLuminaOptions();
        Map<String, String> indexMeta =
                mapOf(
                        "encoding.pq.m", "8",
                        "index.dimension", "256");

        Map<String, String> merged = mergeOptions(tableOptions, indexMeta, Collections.emptyMap());

        assertThat(merged).containsEntry("encoding.pq.m", "8");
        assertThat(merged).containsEntry("index.dimension", "256");
    }

    @Test
    void aQueryOptionWinsOverBoth() {
        Map<String, String> tableOptions = mapOf("search.list_size", "10");
        Map<String, String> indexMeta = mapOf("search.list_size", "20");
        Map<String, String> queryOptions = mapOf("search.list_size", "30");

        assertThat(mergeOptions(tableOptions, indexMeta, queryOptions))
                .containsEntry("search.list_size", "30");
    }

    private static Map<String, String> mapOf(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }
}
