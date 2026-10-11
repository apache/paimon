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

package org.apache.paimon.hive;

import org.apache.paimon.client.ClientPool;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests heartbeat lifetime and expiry of Hive catalog lock scopes. */
class HiveCatalogLockTest {
    private final IMetaStoreClient client = mock(IMetaStoreClient.class);
    private final ScheduledExecutorService renewer = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final AtomicLong clock = new AtomicLong();
    private final ClientPool<IMetaStoreClient, TException> pool =
            new ClientPool<IMetaStoreClient, TException>() {
                @Override
                public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException {
                    return action.run(client);
                }

                @Override
                public void execute(ExecuteAction<IMetaStoreClient, TException> action)
                        throws TException {
                    action.run(client);
                }
            };

    @Test
    void heartbeatExtendsLeaseAndStopsAfterScope() throws Exception {
        HiveCatalogLock lock = lock();
        Runnable renewal =
                lock.runWithLock(
                        "db",
                        "table",
                        () -> {
                            clock.set(TimeUnit.MILLISECONDS.toNanos(2000));
                            Runnable task = renewal();
                            task.run();
                            clock.set(TimeUnit.MILLISECONDS.toNanos(4000));
                            lock.ensureValid();
                            return task;
                        });
        renewal.run();
        verify(client, times(3)).heartbeat(0L, 7L);
        verify(client).unlock(7L);
        verify(future).cancel(false);
    }

    @Test
    void heartbeatFailureInvalidatesScopeAndPreservesCause() throws Exception {
        HiveCatalogLock lock = lock();
        TException failure = new TException("Lost lock");
        lock.runWithLock(
                "db",
                "table",
                () -> {
                    doThrow(failure).when(client).heartbeat(0L, 7L);
                    renewal().run();
                    assertThatThrownBy(lock::ensureValid).hasCause(failure);
                    return null;
                });
        verify(client).unlock(7L);
    }

    @Test
    void expiryWithoutHeartbeatPreventsFurtherUse() throws Exception {
        HiveCatalogLock lock = lock();
        lock.runWithLock(
                "db",
                "table",
                () -> {
                    clock.set(TimeUnit.MILLISECONDS.toNanos(3000));
                    assertThatThrownBy(lock::ensureValid).hasMessageContaining("no longer usable");
                    renewal().run();
                    return null;
                });
        verify(client).heartbeat(0L, 7L);
        verify(client).unlock(7L);
        assertThatThrownBy(lock::ensureValid).hasMessageContaining("No Hive lock");
    }

    @Test
    void preparationFailureReleasesScope() throws Exception {
        HiveCatalogLock lock = lock();
        RuntimeException failure = new RuntimeException("Preparation failed");
        assertThatThrownBy(
                        () ->
                                lock.runWithLock(
                                        "db",
                                        "table",
                                        () -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
        verify(client).unlock(7L);
        verify(future).cancel(false);
    }

    @Test
    void publicationChecksTheServerEvenWhenClientTimeoutIsLonger() throws Exception {
        HiveCatalogLock lock = lock();
        lock.runWithLock(
                "db",
                "table",
                () -> {
                    NoSuchLockException missing = new NoSuchLockException("Expired on server");
                    doThrow(missing).when(client).heartbeat(0L, 7L);
                    // No scheduled renewal or local expiry has occurred yet.
                    assertThatThrownBy(lock::ensureValid).hasCause(missing);
                    return null;
                });
        verify(client).unlock(7L);
    }

    @Test
    void olderHeartbeatResponseCannotShortenANewerDeadline() throws Exception {
        HiveCatalogLock lock = lock();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch oldStarted = new CountDownLatch(1);
        CountDownLatch oldResponse = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        try {
            lock.runWithLock(
                    "db",
                    "table",
                    () -> {
                        doAnswer(
                                        invocation -> {
                                            if (requests.incrementAndGet() == 1) {
                                                oldStarted.countDown();
                                                assertThat(oldResponse.await(10, TimeUnit.SECONDS))
                                                        .isTrue();
                                            }
                                            return null;
                                        })
                                .when(client)
                                .heartbeat(0L, 7L);
                        clock.set(TimeUnit.MILLISECONDS.toNanos(1000));
                        Future<?> old = executor.submit(renewal());
                        assertThat(oldStarted.await(10, TimeUnit.SECONDS)).isTrue();
                        clock.set(TimeUnit.MILLISECONDS.toNanos(2000));
                        lock.ensureValid();
                        oldResponse.countDown();
                        old.get(10, TimeUnit.SECONDS);
                        clock.set(TimeUnit.MILLISECONDS.toNanos(4000));
                        lock.ensureValid();
                        return null;
                    });
        } finally {
            oldResponse.countDown();
            executor.shutdownNow();
        }
    }

    private HiveCatalogLock lock() throws Exception {
        when(client.lock(any())).thenReturn(new LockResponse(7L, LockState.ACQUIRED));
        when(renewer.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
                .thenAnswer(invocation -> future);
        return new HiveCatalogLock(pool, 10, 3000, 3000, renewer, clock::get);
    }

    private Runnable renewal() {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(renewer).scheduleWithFixedDelay(task.capture(), anyLong(), anyLong(), any());
        return task.getValue();
    }
}
