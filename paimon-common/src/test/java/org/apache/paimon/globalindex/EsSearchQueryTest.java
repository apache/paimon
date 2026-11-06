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

package org.apache.paimon.globalindex;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for bounded, identity-based ES DSL field rewriting. */
class EsSearchQueryTest {
    @Test
    void rewritesReferencesWithoutChangingValues() {
        EsSearchQuery query =
                new EsSearchQuery(
                        "{\"bool\":{\"must\":{\"match\":{\"title\":\"title category\"}},"
                                + "\"filter\":[{\"terms\":{\"category\":[\"title\",\"category\"]}},"
                                + "{\"range\":{\"price\":{\"gte\":9007199254740993}}}],"
                                + "\"must_not\":{\"exists\":{\"field\":\"deleted\"}}}}",
                        4);
        EsSearchQuery renamed = query.mapFields(name -> "physical_" + name);
        assertThat(query.fields()).containsExactly("title", "category", "price", "deleted");
        JsonNode node = EsSearchQuery.parse(renamed.queryJson()).get("bool");
        assertThat(node.at("/must/match/physical_title").textValue()).isEqualTo("title category");
        assertThat(node.at("/filter/0/terms/physical_category/1").textValue())
                .isEqualTo("category");
        assertThat(node.at("/filter/1/range/physical_price/gte").longValue())
                .isEqualTo(9007199254740993L);
        assertThat(node.at("/must_not/exists/field").textValue()).isEqualTo("physical_deleted");
    }

    @Test
    void keepsKnnFilterAndCandidateCountInNativeSource() {
        EsSearchQuery query =
                new EsSearchQuery(
                        "{\"knn\":{\"field\":\"v\",\"query_vector\":[1,0],\"k\":5,"
                                + "\"num_candidates\":20,\"filter\":{\"term\":{\"tag\":\"v\"}}}}",
                        3);
        JsonNode source =
                EsSearchQuery.parse(query.mapFields(name -> "f_" + name).searchSourceJson());
        assertThat(source.has("query")).isFalse();
        assertThat(source.at("/knn/field").textValue()).isEqualTo("f_v");
        assertThat(source.at("/knn/filter/term/f_tag").textValue()).isEqualTo("v");
        assertThat(source.at("/knn/num_candidates").intValue()).isEqualTo(20);
        assertThat(source.get("size").intValue()).isEqualTo(3);
        assertThat(source.get("_source").booleanValue()).isFalse();
    }

    @Test
    void rejectsDuplicateAndTrailingJson() {
        for (String json :
                new String[] {"{\"match_all\":{},\"match_all\":{}}", "{} {}", " ", "{"}) {
            assertThatThrownBy(() -> EsSearchQuery.parse(json))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsExcessiveNestingBeforeTreeConstruction() {
        StringBuilder json = new StringBuilder();
        for (int i = 0; i < 33; i++) {
            json.append('[');
        }
        json.append('0');
        for (int i = 0; i < 33; i++) {
            json.append(']');
        }
        assertThatThrownBy(() -> EsSearchQuery.parse(json.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nesting");
    }

    @Test
    void rejectsUnsupportedAndMalformedClauses() {
        for (String json :
                new String[] {
                    "{\"script\":{}}", "{\"match_all\":{\"boost\":2}}",
                    "{\"term\":{\"tag\":null}}", "{\"range\":{\"n\":{\"gt\":1,\"gte\":2}}}",
                    "{\"bool\":{\"must\":{\"knn\":{}}}}", "{\"exists\":{}}",
                    "{\"match\":{\"title\":{\"query\":\"a\",\"analyzer\":\"keyword\"}}}"
                }) {
            assertThatThrownBy(() -> new EsSearchQuery(json, 1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void requiresKnnToCoverGlobalWindow() {
        String prefix = "{\"knn\":{\"field\":\"v\",\"query_vector\":[1,0],\"k\":";
        assertThatThrownBy(() -> new EsSearchQuery(prefix + "2}}", 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EsSearchQuery(prefix + "3,\"num_candidates\":2}}", 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new EsSearchQuery(
                                        prefix + "3,\"rescore_vector\":{\"oversample\":2}}", 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnboundFieldsAndNonFiniteVectors() {
        assertThatThrownBy(
                        () ->
                                new EsSearchQuery("{\"term\":{\"tag\":1}}", 1)
                                        .mapFields(name -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown query field");
        assertThatThrownBy(
                        () ->
                                new EsSearchQuery(
                                        "{\"knn\":{\"field\":\"v\",\"query_vector\":[1e39],\"k\":1}}",
                                        1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finite");
    }
}
