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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The request contract is shared by SDK and engine integrations. */
class EsSearchRequestTest {
    private static final String REQUEST =
            "{\"version\":1,\"operation\":\"SEARCH\",\"targets\":[{"
                    + "\"name\":\"docs\",\"database\":\"db\",\"table\":\"t\",\"indexes\":[[7]],\"bindings\":{\"text\":7}}],"
                    + "\"request\":{\"query\":{\"match\":{\"text\":\"lake\"}},\"from\":2,\"size\":3,\"fetch\":\"SOURCE\"}}";

    @Test
    void preservesFieldIdentityAndWindow() {
        EsSearchRequest request = EsSearchRequest.parse(REQUEST);
        assertThat(request.indexFields).containsExactly(7);
        assertThat(request.bindings).containsEntry("text", 7);
        assertThat(request.query.limit()).isEqualTo(5);
        assertThat(request.snapshotId).isNull();
        assertThat(request.fetchSource).isTrue();
        assertThatThrownBy(() -> request.indexFields.add(9))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> request.bindings.put("bad", 9))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsAmbiguousOrUnboundRequests() {
        assertThatThrownBy(
                        () ->
                                EsSearchRequest.parse(
                                        REQUEST.replace(
                                                "\"version\":1", "\"version\":1,\"version\":1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                EsSearchRequest.parse(
                                        REQUEST.replace(
                                                "\"bindings\":{\"text\":7}", "\"bindings\":{}")))
                .hasMessageContaining("Every query field needs a binding");
        assertThatThrownBy(
                        () -> EsSearchRequest.parse(REQUEST.replace("\"from\":2", "\"from\":9999")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                EsSearchRequest.parse(
                                        REQUEST.replace(
                                                "\"operation\":\"SEARCH\"",
                                                "\"operation\":\"UPDATE\"")))
                .hasMessageContaining("SEARCH only");
    }
}
