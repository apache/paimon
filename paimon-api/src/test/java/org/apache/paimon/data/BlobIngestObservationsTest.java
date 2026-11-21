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

package org.apache.paimon.data;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobIngestObservationsTest {
    @Test
    void testDisabledNestedOperationSuspendsOuterTableAndRestoresIt() {
        List<String> outer = new ArrayList<>();
        List<String> inner = new ArrayList<>();
        try (BlobIngestObservations.Scope ignored =
                BlobIngestObservations.observe((stage, outcome, value) -> outer.add(outcome))) {
            BlobIngestObservations.record("blob_read", "before", 1);
            try (BlobIngestObservations.Scope disabled = BlobIngestObservations.observe(null)) {
                assertThat(BlobIngestObservations.enabled()).isFalse();
                BlobIngestObservations.record("blob_read", "disabled", 1);
                try (BlobIngestObservations.Scope enabled =
                        BlobIngestObservations.observe(
                                (stage, outcome, value) -> inner.add(outcome))) {
                    BlobIngestObservations.record("blob_read", "inner", 1);
                }
                assertThat(BlobIngestObservations.enabled()).isFalse();
            }
            BlobIngestObservations.record("blob_read", "after", 1);
        }
        assertThat(outer).containsExactly("before", "after");
        assertThat(inner).containsExactly("inner");
        assertThat(BlobIngestObservations.enabled()).isFalse();
    }

    @Test
    void testExceptionRestoresOuterScopeAndReporterFailureDoesNotEscape() {
        List<String> events = new ArrayList<>();
        try (BlobIngestObservations.Scope ignored =
                BlobIngestObservations.observe((stage, outcome, value) -> events.add(outcome))) {
            assertThatThrownBy(
                            () -> {
                                try (BlobIngestObservations.Scope disabled =
                                        BlobIngestObservations.observe(null)) {
                                    throw new IllegalStateException("business failure");
                                }
                            })
                    .hasMessage("business failure");
            assertThat(BlobIngestObservations.enabled()).isTrue();
            try (BlobIngestObservations.Scope failedReporter =
                    BlobIngestObservations.observe(
                            (stage, outcome, value) -> {
                                throw new IllegalStateException("reporter failure");
                            })) {
                BlobIngestObservations.record("blob_read", "ignored", 1);
            }
            BlobIngestObservations.record("blob_read", "restored", 1);
        }
        assertThat(events).containsExactly("restored");
        assertThat(BlobIngestObservations.enabled()).isFalse();
    }
}
