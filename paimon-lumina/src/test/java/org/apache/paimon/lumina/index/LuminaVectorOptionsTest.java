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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Lumina vector options. */
public class LuminaVectorOptionsTest {

    @Test
    public void testQueryOptionsOverrideIndexOptions() {
        Map<String, String> baseOptions = new HashMap<>();
        baseOptions.put("diskann.search.list_size", "16");
        baseOptions.put("search.parallel_number", "2");
        Map<String, String> indexOptions = new HashMap<>();
        indexOptions.put("diskann.search.list_size", "32");
        indexOptions.put("index.dimension", "4");
        Map<String, String> queryOptions = new HashMap<>();
        queryOptions.put("diskann.search.list_size", "64");
        queryOptions.put("hnsw.ef_search", "128");

        Map<String, String> merged =
                LuminaVectorGlobalIndexReader.mergeOptions(baseOptions, indexOptions, queryOptions);

        assertThat(merged)
                .containsEntry("diskann.search.list_size", "64")
                .containsEntry("search.parallel_number", "2")
                .containsEntry("index.dimension", "4")
                .containsEntry("hnsw.ef_search", "128");
    }
}
