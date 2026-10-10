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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests the owner lease across preparation, publication and renewal failure. */
class CatalogCommitLeaseTest {
    private final Catalog catalog = mock(Catalog.class);
    private final CatalogLock lock = mock(CatalogLock.class);
    private final CatalogCommitLock lease = mock(CatalogCommitLock.class);
    private final Identifier branch = new Identifier("db", "table", "main");
    private final CatalogSnapshotCommit commit =
            new CatalogSnapshotCommit(catalog, Identifier.create("db", "table"), "table-id", lock);

    @Test
    void firstAttemptDoesNotAcquire() {
        Snapshot head = mock(Snapshot.class);
        try (CommitAttempt attempt = begin(options(), 0)) {
            assertThat(attempt.latestSnapshot(() -> head)).isSameAs(head);
        }
        verifyNoInteractions(catalog, lock);
    }

    @Test
    void retryUsesGrantedHeadAndExistingCommitPayload() throws Exception {
        Snapshot head = mock(Snapshot.class);
        grant(head);
        Snapshot next = ownedSnapshot();
        when(catalog.commitSnapshot(branch, "table-id", "base", next, Collections.emptyList()))
                .thenReturn(true);
        CommitAttempt attempt = begin(options(), 1);
        assertThat(
                        attempt.latestSnapshot(
                                () -> {
                                    throw new AssertionError("Cached head used");
                                }))
                .isSameAs(head);
        assertThat(attempt.commit("base", next, "main", Collections.emptyList())).isTrue();
        attempt.close();
        attempt.close();
        verify(lease).close();
        verify(lock).acquireCommitLock(branch, "table-id", "morax-job");
        verify(catalog).commitSnapshot(branch, "table-id", "base", next, Collections.emptyList());
    }

    @Test
    void invalidLeasePreventsPublication() throws Exception {
        grant(null);
        doThrow(new IllegalStateException("Expired")).when(lease).ensureValid();
        try (CommitAttempt attempt = begin(options(), 1)) {
            assertThatThrownBy(
                            () ->
                                    attempt.commit(
                                            null, ownedSnapshot(), "main", Collections.emptyList()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Expired");
        }
        verify(lease).close();
        verify(catalog, never()).commitSnapshot(any(), any(), any(), any(), any());
    }

    @Test
    void leaseCannotPublishForAnotherOwnerOrBranch() throws Exception {
        grant(null);
        Snapshot other = mock(Snapshot.class);
        when(other.commitUser()).thenReturn("other-job");
        try (CommitAttempt attempt = begin(options(), 1)) {
            assertThatThrownBy(() -> attempt.commit(null, other, "main", Collections.emptyList()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(
                            () ->
                                    attempt.commit(
                                            null, ownedSnapshot(), "dev", Collections.emptyList()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        verify(catalog, never()).commitSnapshot(any(), any(), any(), any(), any());
    }

    @Test
    void busyWaitingDoesNotConsumePublicationRetryBudget() throws Exception {
        grant(null);
        when(lock.acquireCommitLock(branch, "table-id", "morax-job"))
                .thenReturn(Optional.empty(), Optional.empty(), Optional.of(lease));
        Options options = options();
        options.set(CoreOptions.COMMIT_MAX_RETRIES, 0);
        try (CommitAttempt attempt = begin(options, 1)) {
            assertThat(
                            attempt.latestSnapshot(
                                    () -> {
                                        throw new AssertionError();
                                    }))
                    .isNull();
        }
        verify(lock, times(3)).acquireCommitLock(branch, "table-id", "morax-job");
    }

    @Test
    void busyTimeoutFailsClearly() throws Exception {
        when(lock.acquireCommitLock(branch, "table-id", "morax-job")).thenReturn(Optional.empty());
        Options options = options();
        options.set(CoreOptions.COMMIT_TIMEOUT, Duration.ZERO);
        assertThatThrownBy(() -> begin(options, 1)).hasMessageContaining("Timed out");
    }

    @Test
    void retryPolicyWorksWithOtherCommittersWithoutRestOptions() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.COMMIT_LOCK_ON_RETRY, true);
        CoreOptions coreOptions = new CoreOptions(options);
        assertThat(coreOptions.restCommitLockEnabled()).isFalse();
        SnapshotCommit publisher = mock(SnapshotCommit.class);
        CommitAttempt guarded = mock(CommitAttempt.class);
        Snapshot head = mock(Snapshot.class);
        Snapshot next = ownedSnapshot();
        when(publisher.beginCommit("main", "morax-job", true)).thenReturn(Optional.of(guarded));
        when(guarded.latestSnapshot(any())).thenReturn(head);
        when(guarded.commit("base", next, "main", Collections.emptyList())).thenReturn(true);
        try (CommitAttempt attempt =
                CommitAttempt.begin(
                        publisher, coreOptions, "morax-job", 1, System.currentTimeMillis())) {
            assertThat(
                            attempt.latestSnapshot(
                                    () -> {
                                        throw new AssertionError("Cached head used");
                                    }))
                    .isSameAs(head);
            assertThat(attempt.commit("base", next, "main", Collections.emptyList())).isTrue();
        }
        verify(guarded).close();
        verifyNoInteractions(catalog, lock);
    }

    @Test
    void unsupportedCommitterRejectsLockedRetryAfterOptimisticFirstAttempt() {
        Options options = new Options();
        options.set(CoreOptions.COMMIT_LOCK_ON_RETRY, true);
        SnapshotCommit publisher =
                new RenamingSnapshotCommit(mock(SnapshotManager.class), Lock.empty());
        Snapshot head = mock(Snapshot.class);
        try (CommitAttempt attempt =
                CommitAttempt.begin(
                        publisher,
                        new CoreOptions(options),
                        "morax-job",
                        0,
                        System.currentTimeMillis())) {
            assertThat(attempt.latestSnapshot(() -> head)).isSameAs(head);
        }
        assertThatThrownBy(
                        () ->
                                CommitAttempt.begin(
                                        publisher,
                                        new CoreOptions(options),
                                        "morax-job",
                                        1,
                                        System.currentTimeMillis()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support locks");
    }

    private void grant(Snapshot head) throws Exception {
        when(lock.acquireCommitLock(branch, "table-id", "morax-job"))
                .thenReturn(Optional.of(lease));
        when(lease.snapshot()).thenReturn(head);
    }

    private Snapshot ownedSnapshot() {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.commitUser()).thenReturn("morax-job");
        return snapshot;
    }

    private CommitAttempt begin(Options options, int retry) {
        return CommitAttempt.begin(
                commit, new CoreOptions(options), "morax-job", retry, System.currentTimeMillis());
    }

    private static Options options() {
        Options options = new Options();
        options.set(CoreOptions.REST_COMMIT_LOCK_ENABLED, true);
        options.set(CoreOptions.COMMIT_LOCK_ON_RETRY, true);
        options.set(CoreOptions.COMMIT_MIN_RETRY_WAIT, Duration.ZERO);
        options.set(CoreOptions.COMMIT_MAX_RETRY_WAIT, Duration.ZERO);
        return options;
    }
}
