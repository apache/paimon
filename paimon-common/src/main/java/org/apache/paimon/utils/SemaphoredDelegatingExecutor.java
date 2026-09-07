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
        boolean acquired = true;
        try {
            this.queueingPermits.acquire();
        } catch (InterruptedException e) {
            // Semaphore.acquire() throws as soon as the caller carries an interrupt flag, even
            // when permits are free, and execute() has no channel for reporting that the task
            // was dropped. Run it anyway, as this class always has, but remember that no permit
            // backs this task so its wrapper does not hand back one that was never taken.
            Thread.currentThread().interrupt();
            acquired = false;
        }

        RunnableWithPermitRelease wrapped = new RunnableWithPermitRelease(command, acquired);
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
        private final AtomicBoolean permitHeld;

        RunnableWithPermitRelease(Runnable delegated) {
            this(delegated, true);
        }

        RunnableWithPermitRelease(Runnable delegated, boolean permitHeld) {
            this.delegated = delegated;
            this.permitHeld = new AtomicBoolean(permitHeld);
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
         * Hands the permit back, at most once, and only if one was acquired for this task. A
         * delegate that runs the task inline (for example {@link
         * java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}) may both run the wrapper and
         * let a {@link RejectedExecutionException} out of the same call, so the submitting method
         * and {@link #run()} can each reach this.
         */
        void releasePermit() {
            if (this.permitHeld.compareAndSet(true, false)) {
                SemaphoredDelegatingExecutor.this.queueingPermits.release();
            }
        }
    }

    private class CallableWithPermitRelease<T> implements Callable<T> {

        private final Callable<T> delegated;
        private final AtomicBoolean permitHeld = new AtomicBoolean(true);

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

        /** Hands the permit back, at most once. See {@link RunnableWithPermitRelease}. */
        void releasePermit() {
            if (this.permitHeld.compareAndSet(true, false)) {
                SemaphoredDelegatingExecutor.this.queueingPermits.release();
            }
        }
    }
}
