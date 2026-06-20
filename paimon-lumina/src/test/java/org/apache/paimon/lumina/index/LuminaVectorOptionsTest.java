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

    @Test
    public void testFieldOptionsOverrideGlobal() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("lumina.distance.metric", "l2");
        tableOptions.put("lumina.index.dimension", "128");
        // Field-level keys follow the #8239 convention: fields.<field>.<option>, no lumina. prefix.
        tableOptions.put("fields.embed.distance.metric", "inner_product");
        tableOptions.put("fields.embed.index.dimension", "256");

        Map<String, String> meta = metaFor("embed", tableOptions);

        // The per-field value wins over the global one.
        assertThat(meta)
                .containsEntry("distance.metric", "inner_product")
                .containsEntry("index.dimension", "256");
        // The meta keeps the native key shape; no fields.* keys ever leak into it.
        assertThat(meta.keySet()).noneMatch(k -> k.startsWith("fields"));
    }

    @Test
    public void testForeignFieldOptionsAreIgnored() {
        Map<String, String> globalOnly = new HashMap<>();
        globalOnly.put("lumina.distance.metric", "l2");
        globalOnly.put("lumina.index.dimension", "128");

        Map<String, String> withForeign = new HashMap<>(globalOnly);
        // A per-field option that is not a recognized Lumina key (e.g. a merge/agg option) must be
        // ignored, never flattened into the index meta as a bogus native key.
        withForeign.put("fields.embed.aggregate-function", "sum");

        Map<String, String> meta = metaFor("embed", withForeign);
        assertThat(meta).isEqualTo(metaFor("embed", globalOnly));
        assertThat(meta).doesNotContainKey("aggregate-function");
    }

    @Test
    public void testFieldOptionsForOtherFieldDoNotAffectMeta() {
        Map<String, String> globalOnly = new HashMap<>();
        globalOnly.put("lumina.distance.metric", "l2");
        globalOnly.put("lumina.index.dimension", "128");

        Map<String, String> withOtherField = new HashMap<>(globalOnly);
        // Per-field config for a different column must not influence "embed".
        withOtherField.put("fields.other.distance.metric", "inner_product");
        withOtherField.put("fields.other.index.dimension", "256");

        // The meta for "embed" is byte-for-byte the same as before per-field support existed.
        assertThat(metaFor("embed", withOtherField)).isEqualTo(metaFor("embed", globalOnly));
    }

    @Test
    public void testResolvedFieldMetaIsReadable() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("lumina.distance.metric", "l2");
        tableOptions.put("lumina.index.dimension", "128");
        tableOptions.put("fields.embed.distance.metric", "inner_product");
        tableOptions.put("fields.embed.index.dimension", "256");

        // The reader reconstructs everything it needs from the serialized meta.
        LuminaIndexMeta meta = new LuminaIndexMeta(metaFor("embed", tableOptions));
        assertThat(meta.dim()).isEqualTo(256);
        assertThat(meta.distanceMetric()).isEqualTo("inner_product");
        assertThat(meta.metric()).isEqualTo(LuminaVectorMetric.INNER_PRODUCT);
    }

    /** Builds the native lumina meta map (what gets serialized into the index file) for a field. */
    private static Map<String, String> metaFor(String fieldName, Map<String, String> tableOptions) {
        Options resolved =
                LuminaVectorIndexOptions.resolveFieldOptions(
                        fieldName, Options.fromMap(tableOptions));
        return new LuminaVectorIndexOptions(resolved).toLuminaOptions();
    }
}
