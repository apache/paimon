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

package org.apache.paimon.catalog;

import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests the inseparable lock scope and publication contract. */
class SnapshotCommitTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void directAndPreparedPublicationUseOneScope(boolean prepare) throws Exception {
        RecordingLock lock = new RecordingLock();
        Snapshot snapshot = mock(Snapshot.class);
        try (RecordingSnapshotCommit publisher = new RecordingSnapshotCommit(lock)) {
            if (prepare) {
                assertThat(
                                publisher.runWithLock(
                                        () -> {
                                            lock.events.add("read-head");
                                            lock.events.add("prepare");
                                            return publisher.commit(
                                                    null,
                                                    snapshot,
                                                    "main",
                                                    Collections.emptyList());
                                        }))
                        .isTrue();
            } else {
                assertThat(publisher.commit(null, snapshot, "main", Collections.emptyList()))
                        .isTrue();
            }
            assertThat(publisher.publications).isEqualTo(1);
        }
        if (prepare) {
            assertThat(lock.events)
                    .containsExactly(
                            "acquire",
                            "validate",
                            "read-head",
                            "prepare",
                            "validate",
                            "publish",
                            "release",
                            "close-lock",
                            "close-publisher");
        } else {
            assertThat(lock.events)
                    .containsExactly(
                            "acquire",
                            "validate",
                            "publish",
                            "release",
                            "close-lock",
                            "close-publisher");
        }
    }

    @Test
    void lostLeasePreventsPublicationAndNextAttemptAcquiresNewScope() throws Exception {
        RecordingLock lock = new RecordingLock();
        Snapshot snapshot = mock(Snapshot.class);
        try (RecordingSnapshotCommit publisher = new RecordingSnapshotCommit(lock)) {
            assertThatThrownBy(
                            () ->
                                    publisher.runWithLock(
                                            () -> {
                                                lock.expired = true;
                                                return publisher.commit(
                                                        null,
                                                        snapshot,
                                                        "main",
                                                        Collections.emptyList());
                                            }))
                    .hasMessage("Lost lease");
            assertThat(publisher.publications).isZero();
            assertThat(lock.held).isFalse();
            lock.expired = false;
            assertThat(publisher.commit(null, snapshot, "main", Collections.emptyList())).isTrue();
            assertThat(publisher.publications).isEqualTo(1);
        }
        assertThat(lock.events)
                .containsExactly(
                        "acquire",
                        "validate",
                        "validate",
                        "release",
                        "acquire",
                        "validate",
                        "publish",
                        "release",
                        "close-lock",
                        "close-publisher");
    }

    @Test
    void lockCloseFailureStillClosesPublisher() {
        RecordingLock lock = new RecordingLock();
        lock.failClose = true;
        RecordingSnapshotCommit publisher = new RecordingSnapshotCommit(lock);
        assertThatThrownBy(publisher::close)
                .isInstanceOf(IOException.class)
                .hasMessage("Lock close failed");
        assertThat(lock.events).containsExactly("close-lock", "close-publisher");
    }

    private static class RecordingSnapshotCommit extends SnapshotCommit {
        private final RecordingLock lock;
        private int publications;

        private RecordingSnapshotCommit(RecordingLock lock) {
            super(lock);
            this.lock = lock;
        }

        @Override
        protected boolean commitImpl(
                String baseSnapshotUuid,
                Snapshot snapshot,
                String branch,
                List<PartitionStatistics> statistics) {
            assertThat(lock.held).isTrue();
            lock.events.add("publish");
            publications++;
            return true;
        }

        @Override
        protected void closeImpl() {
            lock.events.add("close-publisher");
        }
    }

    private static class RecordingLock implements Lock {
        private final List<String> events = new ArrayList<>();
        private boolean held;
        private boolean expired;
        private boolean failClose;

        @Override
        public AutoCloseable acquire() throws Exception {
            assertThat(held).isFalse();
            events.add("acquire");
            held = true;
            return () -> {
                held = false;
                events.add("release");
            };
        }

        @Override
        public void ensureValid() {
            assertThat(held).isTrue();
            events.add("validate");
            if (expired) {
                throw new IllegalStateException("Lost lease");
            }
        }

        @Override
        public void close() throws IOException {
            events.add("close-lock");
            if (failClose) {
                throw new IOException("Lock close failed");
            }
        }
    }
}
