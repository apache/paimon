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

package org.apache.paimon.operation;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests validity checks and reentrancy of operation locks. */
class LockTest {
    @Test
    void emptyLockBypassesLockHandlingAndPropagatesTheOriginalFailure() throws Exception {
        Lock empty = Lock.empty();
        assertThat(Lock.reentrant(empty)).isSameAs(empty);
        Object result = new Object();
        assertThat(empty.runWithLock(() -> result)).isSameAs(result);
        InterruptedException failure = new InterruptedException("Action interrupted");
        assertThatThrownBy(
                        () ->
                                empty.runWithLock(
                                        () -> {
                                            throw failure;
                                        }))
                .isSameAs(failure)
                .hasNoSuppressedExceptions();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void emptyLockDoesNotAcquireOrValidateAScope() throws Exception {
        Lock empty =
                new Lock.EmptyLock() {
                    @Override
                    public AutoCloseable acquire() {
                        throw new AssertionError("No-lock path must not acquire a scope");
                    }

                    @Override
                    public void ensureValid() {
                        throw new AssertionError("No-lock path must not validate a lease");
                    }
                };
        Object result = new Object();
        assertThat(Lock.reentrant(empty).runWithLock(() -> result)).isSameAs(result);
    }

    @Test
    void releaseFailureKeepsSuccessfulResult() throws Exception {
        ScopeLock backend = new ScopeLock();
        backend.releaseFailure = new IOException("Release failed");
        Lock lock = Lock.reentrant(backend);
        Object result = new Object();
        assertThat(lock.runWithLock(() -> result)).isSameAs(result);
        assertThat(backend.releases).isEqualTo(1);
        assertThat(lock.<Object>runWithLock(() -> null)).isNull();
        assertThat(backend.releases).isEqualTo(2);
    }

    @Test
    void releaseFailureDoesNotMaskTheActionFailure() {
        ScopeLock backend = new ScopeLock();
        backend.releaseFailure = new IOException("Release failed");
        RuntimeException failure = new RuntimeException("Action failed");
        assertThatThrownBy(
                        () ->
                                Lock.reentrant(backend)
                                        .runWithLock(
                                                () -> {
                                                    throw failure;
                                                }))
                .isSameAs(failure)
                .hasSuppressedException(backend.releaseFailure);
        assertThat(backend.releases).isEqualTo(1);
    }

    @Test
    void acquisitionFailureNeverRunsTheOperation() {
        ScopeLock lock = new ScopeLock();
        IOException failure = new IOException("Acquisition failed");
        lock.acquisitionFailure = failure;
        AtomicInteger operations = new AtomicInteger();
        assertThatThrownBy(() -> lock.runWithLock(operations::incrementAndGet))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasCause(failure);
        assertThat(operations.get()).isZero();
        assertThat(lock.releases).isZero();
    }

    @Test
    void checkedOperationFailureIsPropagatedWithoutLockClassification() {
        ScopeLock lock = new ScopeLock();
        IOException failure = new IOException("Operation failed");
        assertThatThrownBy(
                        () ->
                                lock.runWithLock(
                                        () -> {
                                            throw failure;
                                        }))
                .isSameAs(failure)
                .hasNoSuppressedExceptions();
        assertThat(lock.releases).isEqualTo(1);
    }

    private static class ScopeLock implements Lock {
        private Exception acquisitionFailure;
        private Exception releaseFailure;
        private int releases;

        @Override
        public AutoCloseable acquire() throws Exception {
            if (acquisitionFailure != null) {
                throw acquisitionFailure;
            }
            return () -> {
                releases++;
                if (releaseFailure != null) {
                    throw releaseFailure;
                }
            };
        }

        @Override
        public void close() {}
    }

    @Test
    void nestedPublicationUsesOneScopeAndChecksValidity() throws Exception {
        List<String> events = new ArrayList<>();
        Lock backend =
                new Lock() {
                    @Override
                    public AutoCloseable acquire() throws Exception {
                        events.add("acquire");
                        return () -> {
                            events.add("release");
                        };
                    }

                    @Override
                    public void ensureValid() {
                        events.add("validate");
                    }

                    @Override
                    public void close() {
                        events.add("close");
                    }
                };
        try (Lock lock = Lock.reentrant(backend)) {
            lock.runWithLock(
                    () -> {
                        events.add("head");
                        return lock.runWithLock(
                                () -> {
                                    events.add("publish");
                                    return true;
                                });
                    });
            lock.runWithLock(
                    () -> {
                        events.add("next");
                        return true;
                    });
        }
        assertThat(events)
                .isEqualTo(
                        Arrays.asList(
                                "acquire",
                                "validate",
                                "head",
                                "validate",
                                "publish",
                                "release",
                                "acquire",
                                "validate",
                                "next",
                                "release",
                                "close"));
    }

    @Test
    void expiredLeasePreventsNestedPublicationAndScopeIsReleased() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicBoolean expired = new AtomicBoolean();
        Lock backend =
                new Lock() {
                    @Override
                    public AutoCloseable acquire() throws Exception {
                        events.add("acquire");
                        return () -> {
                            events.add("release");
                        };
                    }

                    @Override
                    public void ensureValid() {
                        if (expired.get()) {
                            throw new IllegalStateException("Expired");
                        }
                    }

                    @Override
                    public void close() {}
                };
        Lock lock = Lock.reentrant(backend);
        assertThatThrownBy(
                        () ->
                                lock.runWithLock(
                                        () -> {
                                            expired.set(true);
                                            return lock.runWithLock(
                                                    () -> {
                                                        events.add("publish");
                                                        return null;
                                                    });
                                        }))
                .hasMessage("Expired");
        assertThat(events).containsExactly("acquire", "release");
        expired.set(false);
        lock.runWithLock(() -> null);
        assertThat(events).containsExactly("acquire", "release", "acquire", "release");
    }

    @Test
    void failedInitialValidationReleasesScopeBeforeRetry() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicBoolean expired = new AtomicBoolean(true);
        Lock backend =
                new Lock() {
                    @Override
                    public AutoCloseable acquire() throws Exception {
                        events.add("acquire");
                        return () -> {
                            events.add("release");
                        };
                    }

                    @Override
                    public void ensureValid() {
                        if (expired.get()) {
                            throw new IllegalStateException("Expired");
                        }
                    }

                    @Override
                    public void close() {}
                };
        Lock lock = Lock.reentrant(backend);
        assertThatThrownBy(
                        () ->
                                lock.runWithLock(
                                        () -> {
                                            events.add("publish");
                                            return null;
                                        }))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasRootCauseMessage("Expired");
        assertThat(events).containsExactly("acquire", "release");
        expired.set(false);
        lock.runWithLock(
                () -> {
                    events.add("publish");
                    return null;
                });
        assertThat(events).containsExactly("acquire", "release", "acquire", "publish", "release");
    }
}
