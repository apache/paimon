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

package org.apache.paimon.utils;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ForwardingExecutorService;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.Futures;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * This ExecutorService blocks the submission of new tasks when its queue is already full by using a
 * semaphore.
 */
public class BlockedDelegatingExecutor extends ForwardingExecutorService {

    private final Semaphore queueingPermits;
    private final ExecutorService executorDelegate;

    public BlockedDelegatingExecutor(ExecutorService executorDelegate, int permitCount) {
        this.queueingPermits = new Semaphore(permitCount, true);
        this.executorDelegate = requireNonNull(executorDelegate);
    }

    @Override
    protected ExecutorService delegate() {
        return executorDelegate;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new CallableWithPermitRelease<>(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }
        return super.submit(new RunnableWithPermitRelease(task));
    }

    @Override
    public void execute(Runnable command) {
        try {
            queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.execute(new RunnableWithPermitRelease(command));
    }

    /** Releases a permit after the task is executed. */
    private class RunnableWithPermitRelease implements Runnable {

        private final Runnable delegate;

        RunnableWithPermitRelease(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            try {
                delegate.run();
            } finally {
                queueingPermits.release();
            }
        }
    }

    /** Releases a permit after the task is completed. */
    private class CallableWithPermitRelease<T> implements Callable<T> {

        private final Callable<T> delegate;

        CallableWithPermitRelease(Callable<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T call() throws Exception {
            try {
                return delegate.call();
            } finally {
                queueingPermits.release();
            }
        }
    }
}
