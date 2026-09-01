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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ForwardingExecutorService} to delegate tasks to limit the number of tasks executed
 * concurrently.
 */
public class SemaphoredDelegatingExecutor extends ForwardingExecutorService {

    private final Semaphore queueingPermits;
    private final ExecutorService executorDelegated;
    private final int permitCount;

    public SemaphoredDelegatingExecutor(
            ExecutorService executorDelegated, int permitCount, boolean fair) {
        this.permitCount = permitCount;
        this.queueingPermits = new Semaphore(permitCount, fair);
        this.executorDelegated = executorDelegated;
    }

    @Override
    protected ExecutorService delegate() {
        return this.executorDelegated;
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
            this.queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }

        CallableWithPermitRelease<T> wrapped = new CallableWithPermitRelease<>(task);
        try {
            return super.submit(wrapped);
        } catch (RejectedExecutionException e) {
            wrapped.releasePermit();
            throw e;
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        try {
            this.queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }

        RunnableWithPermitRelease wrapped = new RunnableWithPermitRelease(task);
        try {
            return super.submit(wrapped, result);
        } catch (RejectedExecutionException e) {
            wrapped.releasePermit();
            throw e;
        }
    }

    @Override
    public Future<?> submit(Runnable task) {
        try {
            this.queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Futures.immediateFailedFuture(e);
        }

        RunnableWithPermitRelease wrapped = new RunnableWithPermitRelease(task);
        try {
            return super.submit(wrapped);
        } catch (RejectedExecutionException e) {
            wrapped.releasePermit();
            throw e;
        }
    }

    @Override
    public void execute(Runnable command) {
        try {
            this.queueingPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // A permit was never acquired, so the task must not run: its wrapper would
            // release a permit on completion and inflate the semaphore beyond permitCount.
            // Reject rather than drop silently: execute() has no failed-future channel,
            // and callers waiting on CompletableFuture.join() would hang forever.
            throw new RejectedExecutionException(
                    "Interrupted while waiting for a permit to submit the task", e);
        }

        RunnableWithPermitRelease wrapped = new RunnableWithPermitRelease(command);
        try {
            super.execute(wrapped);
        } catch (RejectedExecutionException e) {
            wrapped.releasePermit();
            throw e;
        }
    }

    public int getAvailablePermits() {
        return this.queueingPermits.availablePermits();
    }

    public int getWaitingCount() {
        return this.queueingPermits.getQueueLength();
    }

    public int getPermitCount() {
        return this.permitCount;
    }

    @Override
    public String toString() {
        return "SemaphoredDelegatingExecutor{"
                + "permitCount="
                + getPermitCount()
                + ", available="
                + getAvailablePermits()
                + ", waiting="
                + getWaitingCount()
                + '}';
    }

    private class RunnableWithPermitRelease implements Runnable {

        private final Runnable delegated;
        private final AtomicBoolean released = new AtomicBoolean(false);

        RunnableWithPermitRelease(Runnable delegated) {
            this.delegated = delegated;
        }

        @Override
        public void run() {
            try {
                this.delegated.run();
            } finally {
                releasePermit();
            }
        }

        /**
         * Returns the acquired permit, at most once. A delegate that runs the task inline (for
         * example {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}) may both run
         * the wrapper and let a {@link RejectedExecutionException} out of the same call, so the
         * submitting method and {@link #run()} can each reach this.
         */
        void releasePermit() {
            if (this.released.compareAndSet(false, true)) {
                SemaphoredDelegatingExecutor.this.queueingPermits.release();
            }
        }
    }

    private class CallableWithPermitRelease<T> implements Callable<T> {

        private final Callable<T> delegated;
        private final AtomicBoolean released = new AtomicBoolean(false);

        CallableWithPermitRelease(Callable<T> delegated) {
            this.delegated = delegated;
        }

        @Override
        public T call() throws Exception {
            T result;
            try {
                result = this.delegated.call();
            } finally {
                releasePermit();
            }

            return result;
        }

        /** Returns the acquired permit, at most once. See {@link RunnableWithPermitRelease}. */
        void releasePermit() {
            if (this.released.compareAndSet(false, true)) {
                SemaphoredDelegatingExecutor.this.queueingPermits.release();
            }
        }
    }
}
