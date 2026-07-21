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

package org.apache.paimon.rest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SimpleHttpClient}. */
public class SimpleHttpClientTest {

    private static final String MALFORMED_URL =
            "https://alice:secret@host/bad path?sig=QUERY_SECRET";

    @Test
    public void testGetWithMalformedUrlDoesNotLeakCredentials() {
        assertMalformedUrlSanitized(() -> SimpleHttpClient.INSTANCE.get(MALFORMED_URL));
    }

    @Test
    public void testPostWithMalformedUrlDoesNotLeakCredentials() {
        assertMalformedUrlSanitized(
                () -> SimpleHttpClient.INSTANCE.post(MALFORMED_URL, null, null));
    }

    private static void assertMalformedUrlSanitized(
            org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOf(IllegalArgumentException.class)
                .hasNoCause()
                .matches(HttpClientUtils::isInvalidUriException)
                .satisfies(
                        e -> {
                            assertThat(String.valueOf(e)).doesNotContain("secret");
                            assertThat(String.valueOf(e)).doesNotContain("QUERY_SECRET");
                        });
    }
}
