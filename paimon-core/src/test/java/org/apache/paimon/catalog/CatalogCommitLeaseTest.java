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
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests the owner lease across preparation, publication and renewal failure. */
class CatalogCommitLeaseTest {
    private final Catalog catalog = mock(Catalog.class);
    private final ScheduledExecutorService renewer = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final Identifier branch = new Identifier("db", "table", "main");
    private final CatalogSnapshotCommit commit =
            new CatalogSnapshotCommit(
                    catalog, Identifier.create("db", "table"), "table-id", renewer);

    @Test
    void firstAttemptDoesNotAcquire() {
        Snapshot head = mock(Snapshot.class);
        try (CommitAttempt attempt = begin(options(), 0)) {
            assertThat(attempt.latestSnapshot(() -> head)).isSameAs(head);
        }
        verifyNoInteractions(catalog, renewer);
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
        verify(future).cancel(false);
        verify(catalog).acquireCommitLock(branch, "table-id", "morax-job");
        verify(catalog).commitSnapshot(branch, "table-id", "base", next, Collections.emptyList());
    }

    @Test
    void renewalFailurePreventsPublicationAndCloseStopsRenewal() throws Exception {
        grant(null);
        when(catalog.renewCommitLock(branch, "table-id", "morax-job")).thenReturn(false);
        CommitAttempt attempt = begin(options(), 1);
        ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
        verify(renewer)
                .scheduleWithFixedDelay(
                        callback.capture(), eq(20000L), eq(20000L), eq(TimeUnit.MILLISECONDS));
        callback.getValue().run();
        assertThatThrownBy(
                        () ->
                                attempt.commit(
                                        null, ownedSnapshot(), "main", Collections.emptyList()))
                .isInstanceOf(IllegalStateException.class);
        attempt.close();
        callback.getValue().run();
        verify(catalog, times(1)).renewCommitLock(branch, "table-id", "morax-job");
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
        when(catalog.acquireCommitLock(branch, "table-id", "morax-job"))
                .thenReturn(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new CatalogCommitLock("morax-job", 60000, null)));
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
        verify(catalog, times(3)).acquireCommitLock(branch, "table-id", "morax-job");
    }

    @Test
    void busyTimeoutFailsClearly() throws Exception {
        when(catalog.acquireCommitLock(branch, "table-id", "morax-job"))
                .thenReturn(Optional.empty());
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
        verifyNoInteractions(catalog, renewer);
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
        when(catalog.acquireCommitLock(branch, "table-id", "morax-job"))
                .thenReturn(Optional.of(new CatalogCommitLock("morax-job", 60000, head)));
        when(renewer.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
                .thenAnswer(invocation -> future);
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
