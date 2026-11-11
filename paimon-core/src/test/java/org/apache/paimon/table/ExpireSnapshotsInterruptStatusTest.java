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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests interrupt handling in {@link ExpireSnapshotsImpl}. */
class ExpireSnapshotsInterruptStatusTest {

    @Test
    void expireUntilRestoresInterruptStatus() throws Exception {
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        SnapshotDeletion snapshotDeletion = mock(SnapshotDeletion.class);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);

        when(snapshotManager.branch()).thenReturn("main");
        when(snapshotDeletion.fileExecutor()).thenReturn(executor);
        when(snapshotManager.tryGetSnapshot(anyLong()))
                .thenAnswer(
                        ignored -> {
                            readStarted.countDown();
                            releaseRead.await();
                            return mock(Snapshot.class);
                        });

        ExpireSnapshotsImpl expire =
                new ExpireSnapshotsImpl(
                        snapshotManager,
                        mock(ChangelogManager.class),
                        snapshotDeletion,
                        mock(TagManager.class),
                        1);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread expirer =
                new Thread(
                        () -> {
                            try {
                                expire.expireUntil(1, 2);
                            } catch (Throwable t) {
                                failure.set(t);
                                interrupted.set(Thread.currentThread().isInterrupted());
                            }
                        });
        expirer.start();

        try {
            assertThat(readStarted.await(5, TimeUnit.SECONDS)).isTrue();
            expirer.interrupt();
            expirer.join(TimeUnit.SECONDS.toMillis(5));
        } finally {
            releaseRead.countDown();
            executor.shutdownNow();
        }

        assertThat(expirer.isAlive()).isFalse();
        assertThat(failure.get())
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(interrupted.get()).isTrue();
    }
}
