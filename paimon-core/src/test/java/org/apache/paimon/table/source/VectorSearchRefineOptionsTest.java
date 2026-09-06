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

package org.apache.paimon.table.source;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** Tests for shared vector refine option resolution. */
class VectorSearchRefineOptionsTest {

    @Test
    void testQueryOptionsTakePrecedenceOverMoreSpecificTableOptions() {
        Map<String, String> query = Collections.singletonMap("rerank-factor", "2");
        Map<String, String> table =
                Collections.singletonMap("fields.embedding.ivf-pq.refine_factor", "7");

        assertThat(VectorSearchRefineOptions.resolve(query, table, "embedding", "ivf-pq"))
                .isEqualTo(2);
    }

    @Test
    void testPrefixAndAliasPrecedence() {
        Map<String, String> options = new HashMap<>();
        options.put("refine_factor", "1");
        options.put("ivf.refine_factor", "2");
        options.put("ivf_pq.refine_factor", "3");
        options.put("ivf-pq.refine_factor", "4");
        options.put("fields.embedding.refine_factor", "5");
        options.put("fields.embedding.ivf.refine_factor", "6");
        options.put("fields.embedding.ivf_pq.refine_factor", "7");
        options.put("fields.embedding.ivf-pq.rerank-factor", "8");

        assertThat(
                        VectorSearchRefineOptions.resolve(
                                Collections.emptyMap(), options, "embedding", "ivf-pq"))
                .isEqualTo(8);
    }

    @Test
    void testAliasPrecedenceWithinPrefix() {
        Map<String, String> options = new HashMap<>();
        options.put("refine_factor", "2");
        options.put("refine-factor", "3");
        options.put("rerank_factor", "4");
        options.put("rerank-factor", "5");

        assertThat(
                        VectorSearchRefineOptions.resolve(
                                options, Collections.emptyMap(), "embedding", "ivf-pq"))
                .isEqualTo(2);
    }

    @Test
    void testDisabledAndCheckedSearchLimit() {
        assertThat(
                        VectorSearchRefineOptions.resolve(
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                "embedding",
                                "ivf-pq"))
                .isZero();
        assertThat(VectorSearchRefineOptions.searchLimit(10, 0)).isEqualTo(10);
        assertThat(VectorSearchRefineOptions.searchLimit(10, 1)).isEqualTo(10);
        assertThat(VectorSearchRefineOptions.searchLimit(10, 3)).isEqualTo(30);
    }

    @Test
    void testRejectsInvalidFactorsAndOverflow() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> resolveQuery("0"))
                .withMessageContaining("refine factor must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> resolveQuery("-1"))
                .withMessageContaining("refine factor must be positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> resolveQuery("abc"))
                .withMessageContaining("Invalid vector refine factor");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> VectorSearchRefineOptions.searchLimit(Integer.MAX_VALUE, 2))
                .withMessageContaining("limit overflow");
    }

    private static int resolveQuery(String value) {
        return VectorSearchRefineOptions.resolve(
                Collections.singletonMap("refine_factor", value),
                Collections.emptyMap(),
                "embedding",
                "ivf-pq");
    }
}
