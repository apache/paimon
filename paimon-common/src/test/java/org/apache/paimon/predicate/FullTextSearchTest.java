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

package org.apache.paimon.predicate;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FullTextSearch}. */
public class FullTextSearchTest {

    private static final String QUERY = "{\"match\":{\"query\":\"paimon lake\"}}";

    @Test
    public void testFullTextSearchKeepsFieldAndQueryString() {
        FullTextSearch search = new FullTextSearch("content", QUERY, 10);

        assertThat(search.fieldName()).isEqualTo("content");
        assertThat(search.column()).isEqualTo("content");
        assertThat(search.query()).isEqualTo(QUERY);
        assertThat(search.toString()).contains("content").contains(QUERY);
    }

    @Test
    public void testFullTextSearchRejectsInvalidArguments() {
        assertThatThrownBy(() -> new FullTextSearch(null, QUERY, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field name cannot be null or empty");

        assertThatThrownBy(() -> new FullTextSearch("", QUERY, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field name cannot be null or empty");

        assertThatThrownBy(() -> new FullTextSearch("content", null, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Query cannot be null");

        assertThatThrownBy(() -> new FullTextSearch("content", QUERY, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");
    }

    @Test
    public void testHybridFullTextRouteKeepsExplicitFieldAndQuery() {
        HybridSearchRoute route =
                HybridSearchRoute.fullText("content", QUERY, 10, 2.0f, Collections.emptyMap());

        assertThat(route.isFullText()).isTrue();
        assertThat(route.fieldName()).isEqualTo("content");
        assertThat(route.fullTextQuery()).isEqualTo(QUERY);
        assertThat(route.columns()).containsExactly("content");
        assertThat(route.toFullTextSearch().query()).isEqualTo(QUERY);
    }

    @Test
    public void testHybridFullTextRouteRejectsOptions() {
        assertThatThrownBy(
                        () ->
                                HybridSearchRoute.fullText(
                                        "content",
                                        QUERY,
                                        10,
                                        1.0f,
                                        Collections.singletonMap("some.option", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Full-text hybrid route options are not supported yet");

        assertThatThrownBy(
                        () ->
                                HybridSearchRoute.builder()
                                        .fullTextColumn("content")
                                        .query(QUERY)
                                        .limit(10)
                                        .option("some.option", "x")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Full-text hybrid route options are not supported yet");
    }

    @Test
    public void testHybridRouteBuilderRejectsMixedRouteTypes() {
        assertThatThrownBy(
                        () ->
                                HybridSearchRoute.builder()
                                        .vectorColumn("embedding")
                                        .queryVector(new float[] {1.0f, 2.0f})
                                        .query(QUERY)
                                        .limit(10)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix vector and full-text hybrid route settings");

        assertThatThrownBy(
                        () ->
                                HybridSearchRoute.builder()
                                        .fullTextColumn("content")
                                        .query(QUERY)
                                        .vectorColumn("embedding")
                                        .limit(10)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot mix vector and full-text hybrid route settings");
    }

    @Test
    public void testHybridRouteRejectsNonFiniteWeight() {
        assertThatThrownBy(
                        () ->
                                HybridSearchRoute.vector(
                                        "embedding",
                                        new float[] {1.0f},
                                        10,
                                        Float.NaN,
                                        Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Weight must be finite and positive");

        assertThatThrownBy(
                        () ->
                                HybridSearchRoute.builder()
                                        .fullTextColumn("content")
                                        .query(QUERY)
                                        .limit(10)
                                        .weight(Float.NEGATIVE_INFINITY)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Weight must be finite and positive");
    }
}
